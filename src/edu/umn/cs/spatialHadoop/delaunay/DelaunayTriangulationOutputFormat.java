package edu.umn.cs.spatialHadoop.delaunay;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

/**
 * Writes the results of the {@link DelaunayTriangulation} operation and merges
 * non-final results upon the completion of all reducers.
 * @author Ahmed Eldawy
 *
 */
public class DelaunayTriangulationOutputFormat extends
  FileOutputFormat<Boolean, SimpleGraph> {
  static final Log LOG = LogFactory.getLog(DelaunayTriangulationOutputFormat.class);
  
  public static class TriangulationRecordWriter extends
    RecordWriter<Boolean, SimpleGraph> {
    
    /**An output stream to write non-final triangulations*/
    private FSDataOutputStream nonFinalOut;
    /**An output stream to write final triangulations*/
    private PrintStream finalOut;
    private TaskAttemptContext context;

    public TriangulationRecordWriter(FileSystem fs, Path nonFinalFile,
        Path finalFile, TaskAttemptContext context) throws IOException {
      this.context = context;
      if (nonFinalFile != null)
        this.nonFinalOut = fs.create(nonFinalFile);
      if (finalFile != null && context.getConfiguration().getBoolean("output", true))
        this.finalOut = new PrintStream(fs.create(finalFile));
    }

    @Override
    public void write(Boolean key, SimpleGraph value)
        throws IOException, InterruptedException {
      if (key.booleanValue()) {
        // Write a final triangulation in a user-friendly text format
        if (finalOut != null)
          writeFinalTriangulation(finalOut, value, context);
      } else {
        value.write(nonFinalOut);
      }
    }

    /**
     * Writes a final triangulation in a user-friendly format
     * @param t
     */
    public static void writeFinalTriangulation(PrintStream ps, SimpleGraph t,
        Progressable progress) {
      Text text = new Text2();
      for (int i = 0; i < t.edgeStarts.length; i++) {
        Point startNode = t.sites[t.edgeStarts[i]];
        text.clear();
        startNode.toText(text);
        ps.print(text); // Write start node

        ps.print('\t'); // Field separator
        
        Point endNode = t.sites[t.edgeEnds[i]];
        text.clear();
        endNode.toText(text);
        ps.println(text); // Write end node and new line
        
        if (progress != null && (i & 0xff) == 0)
          progress.progress();
      }
    }

    @Override
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      if (finalOut != null)
        finalOut.close();
      if (nonFinalOut != null)
        nonFinalOut.close();
    }
  }

  @Override
  public RecordWriter<Boolean, SimpleGraph> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    Path nonFinalFile = getDefaultWorkFile(context, ".nonfinal");
    Path finalFile = getDefaultWorkFile(context, ".final");
    FileSystem fs = nonFinalFile.getFileSystem(context.getConfiguration());
    return new TriangulationRecordWriter(fs, nonFinalFile, finalFile, context);
  }
  
  
  public static class TriangulationMerger extends FileOutputCommitter {
    
    private Path outPath;
    private TaskAttemptContext task;

    TriangulationMerger(Path outputPath, TaskAttemptContext context) throws IOException {
      super(outputPath, context);
      this.outPath = outputPath;
      this.task = context;
    }
    
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      // Read back intermediate triangulation and merge them
      final FileSystem fs = outPath.getFileSystem(context.getConfiguration());
      final FileStatus[] nonFinalFiles = fs.listStatus(outPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().toLowerCase().endsWith(".nonfinal");
        }
      });
      
      try {
        List<List<SimpleGraph>> allLists = Parallel.forEach(nonFinalFiles.length, new RunnableRange<List<SimpleGraph>>() {
          @Override
          public List<SimpleGraph> run(int i1, int i2) {
            try {
              List<SimpleGraph> triangulations = new ArrayList<SimpleGraph>();
              for (int i = i1; i < i2; i++) {
                FSDataInputStream in = fs.open(nonFinalFiles[i].getPath());
                while (in.available() > 0) {
                  SimpleGraph t = new SimpleGraph();
                  t.readFields(in);
                  triangulations.add(t);
                }
                in.close();
              }
              return triangulations;
            } catch (IOException e) {
              throw new RuntimeException("Error reading non-final triangulations", e);
            }
          }
        });
        
        List<SimpleGraph> allTriangulations = new ArrayList<SimpleGraph>();
        for (List<SimpleGraph> list : allLists)
          allTriangulations.addAll(list);
        SimpleGraph finalAnswer;
        if (allTriangulations.size() == 1) {
          finalAnswer = allTriangulations.get(0);
        } else {
          System.out.println("Merging "+allTriangulations.size()+" triangulations");
          finalAnswer = GSDTAlgorithm.mergeTriangulations(
              allTriangulations, task).getFinalAnswerAsGraph();
        }
        // Write the final answer to the output and delete intermediate files
        System.out.println("Writing final output");
        Path finalAnswerPath = new Path(outPath, "lastPart.final");
        PrintStream ps = new PrintStream(fs.create(finalAnswerPath));
        TriangulationRecordWriter.writeFinalTriangulation(ps, finalAnswer, task);
        ps.close();
        
        System.out.println("Cleaning up");
        // Delete intermediate files
        for (FileStatus nonFinalFile : nonFinalFiles)
          fs.delete(nonFinalFile.getPath(), false);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException {
    Path jobOutputPath = getOutputPath(context);
    return new TriangulationMerger(jobOutputPath, context);
  }
  
}