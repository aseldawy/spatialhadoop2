/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;
import edu.umn.cs.spatialHadoop.util.Progressable;

/**
 * Computes the union of a set of shapes using a distributed MapReduce program.
 * The file is split into n partitions, the union of each partition is computed
 * separately, and finally the results are merged into one reducer. 
 * @author Ahmed Eldawy
 *
 */
public class Union {
  public static final GeometryFactory FACTORY = new GeometryFactory();
  
  /**Logger for this class*/
  public static final Log LOG = LogFactory.getLog(Union.class);
  
  /**
   * The map function for the BasicUnion algorithm which works on a set of
   * shapes. It computes the union of all these shapes and writes the result
   * to the output.
   * @author Ahmed Eldawy
   *
   * @param <S>
   */
  static class UnionMap<S extends OGCJTSShape> extends 
      Mapper<Rectangle, Iterable<S>, IntWritable, OGCJTSShape> {
    Random rand = new Random();
    private double[] columnBoundaries;
    IntWritable key = new IntWritable();
    
    @Override
    protected void setup(
        Mapper<Rectangle, Iterable<S>, IntWritable, OGCJTSShape>.Context context)
            throws IOException, InterruptedException {
      super.setup(context);
      columnBoundaries = SpatialSite.getReduceSpace(context.getConfiguration());
      if (columnBoundaries == null)
        key.set(new Random().nextInt(context.getNumReduceTasks()));
    }
    
    @Override
    protected void map(Rectangle mbr, Iterable<S> shapes, final Context context)
        throws IOException, InterruptedException {
      if (mbr.isValid()) {
        int col = Arrays.binarySearch(this.columnBoundaries, mbr.getCenterPoint().x);
        if (col < 0)
          col = -col - 1;
        key.set(col);
      }
      
      List<Geometry> vgeoms = new ArrayList<Geometry>();
      for (S s : shapes)
        vgeoms.add(s.geom);

      LOG.info("Computing the union of "+vgeoms.size()+" geoms");
      ResultCollector<Geometry> resultCollector = new ResultCollector<Geometry>() {
        OGCJTSShape value = new OGCJTSShape();
        @Override
        public void collect(Geometry r) {
          try {
            value.geom = r;
            context.write(key, value);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      SpatialAlgorithms.multiUnion(vgeoms.toArray(new Geometry[vgeoms.size()]),
          new Progressable.TaskProgressable(context),resultCollector);
      LOG.info("Union computed");
    }
  }
  
  static class UnionReduce extends
    Reducer<IntWritable, OGCJTSShape, NullWritable, OGCJTSShape> {
    
    @Override
    protected void reduce(final IntWritable dummy, Iterable<OGCJTSShape> shapes,
        final Context context) throws IOException, InterruptedException {
      List<Geometry> vgeoms = new ArrayList<Geometry>();
      for (OGCJTSShape s : shapes)
        vgeoms.add(s.geom);

      LOG.info("Computing the union of "+vgeoms.size()+" geoms");
      ResultCollector<Geometry> resultCollector = new ResultCollector<Geometry>() {
        NullWritable key = NullWritable.get();
        OGCJTSShape value = new OGCJTSShape();
        @Override
        public void collect(Geometry r) {
          try {
            value.geom = r;
            context.write(key, value);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      SpatialAlgorithms.multiUnion(vgeoms.toArray(new Geometry[vgeoms.size()]),
          new Progressable.TaskProgressable(context), resultCollector);
      LOG.info("Union computed");
    }
  }
  
  /**
   * The UnionOutputCommitter performs an additional post-processing step that
   * combines the output of all reducers
   * @author Ahmed Eldawy
   *
   */
  public static class UnionOutputCommitter extends FileOutputCommitter {

    private Path outPath;
    private TaskAttemptContext task;

    public UnionOutputCommitter(Path outputPath, TaskAttemptContext task)
        throws IOException {
      super(outputPath, task);
      outPath = outputPath;
      this.task = task;
    }
    
    @Override
    public void commitJob(final JobContext context) throws IOException {
      super.commitJob(context);
      // Read all resulting files and combine them together
      final FileSystem fs = outPath.getFileSystem(context.getConfiguration());
      final FileStatus[] outFiles = fs.listStatus(outPath, SpatialSite.NonHiddenFileFilter);
      
      try {
        List<List<Geometry>> allLists = Parallel.forEach(outFiles.length, new RunnableRange<List<Geometry>>() {
          @Override
          public List<Geometry> run(int i1, int i2) {
            try {
              List<Geometry> geoms = new ArrayList<Geometry>();
              for (int i = i1; i < i2; i++) {
                LineReader reader = new LineReader(fs.open(outFiles[i].getPath()));
                Text line = new Text2();
                while (reader.readLine(line) > 0) {
                  geoms.add(TextSerializerHelper.consumeGeometryJTS(line, '\0'));
                }
                reader.close();
              }
              return geoms;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
        List<Geometry> allGeoms = new ArrayList<Geometry>();
        for (List<Geometry> list : allLists)
          allGeoms.addAll(list);
        
        final PrintStream ps = new PrintStream(fs.create(new Path(outPath, "finalResult.wkt")));
        
        ResultCollector<Geometry> resultCollector = new ResultCollector<Geometry>() {
          @Override
          public synchronized void collect(Geometry r) {
            ps.println(r.toText());
          }
        };
        SpatialAlgorithms.multiUnion(allGeoms.toArray(new Geometry[allGeoms.size()]),
            new Progressable.TaskProgressable(task), resultCollector);
        ps.close();

        // Delete all intermediate files
        for (FileStatus outFile : outFiles)
          fs.delete(outFile.getPath(), false);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  public static class UnionOutputFormat extends TextOutputFormat3<NullWritable, OGCJTSShape> {
    
    @Override
    public synchronized OutputCommitter getOutputCommitter(
        TaskAttemptContext context) throws IOException {
      Path jobOutputPath = getOutputPath(context);
      return new UnionOutputCommitter(jobOutputPath, context);
    }
  }
  
  private static Job unionMapReduce(Path input, Path output,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = new Job(params, "BasicUnion");
    job.setJarByClass(Union.class);

    // Set map and reduce
    job.setMapperClass(UnionMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(OGCJTSShape.class);
    job.setReducerClass(UnionReduce.class);
    SpatialSite.splitReduceSpace(job, new Path[] {input}, params);

    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.addInputPath(job, input);

    job.setOutputFormatClass(UnionOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // Submit the job
    if (!params.getBoolean("background", false)) {
      job.waitForCompletion(false);
      if (!job.isSuccessful())
        throw new RuntimeException("Job failed!");
    } else {
      job.submit();
    }
    return job;
  }
  
  private static <S extends OGCJTSShape> void unionLocal(Path inPath, Path outPath,
      final OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    // 1- Split the input path/file to get splits that can be processed independently
    final SpatialInputFormat3<Rectangle, S> inputFormat =
        new SpatialInputFormat3<Rectangle, S>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inPath);
    final List<InputSplit> splits = inputFormat.getSplits(job);
    int parallelism = params.getInt("parallel", Runtime.getRuntime().availableProcessors());
    
    // 2- Process splits in parallel
    final List<Float> progresses = new Vector<Float>();
    final IntWritable overallProgress = new IntWritable(0);
    List<List<Geometry>> results = Parallel.forEach(splits.size(), new RunnableRange<List<Geometry>>() {
      @Override
      public List<Geometry> run(final int i1, final int i2) {
        final int pi;
        final IntWritable splitsProgress = new IntWritable();
        synchronized(progresses) {
          pi = progresses.size();
          progresses.add(0f);
        }
        final float progressRatio = (i2 - i1) / (float) splits.size();
        Progressable progress = new Progressable.NullProgressable() {
          @Override
          public void progress(float p) {
            progresses.set(pi, p * ((splitsProgress.get() - i1) / (float)(i2 - i1)) * progressRatio);
            float sum = 0;
            for (float f : progresses)
              sum += f;
            int newProgress = (int) (sum * 100);
            if (newProgress > overallProgress.get()) {
              overallProgress.set(newProgress);
              LOG.info("Local union progress "+newProgress+"%");
            }
          }
        };
        
        
        final List<Geometry> localUnion = new ArrayList<Geometry>();
        ResultCollector<Geometry> output = new ResultCollector<Geometry>() {
          @Override
          public void collect(Geometry r) {
            localUnion.add(r);
          }
        };
        
        final int MaxBatchSize = 100000;
        Geometry[] batch = new Geometry[MaxBatchSize];
        int batchSize = 0;
        for (int i = i1; i < i2; i++) {
          splitsProgress.set(i);
          try {
            FileSplit fsplit = (FileSplit) splits.get(i);
            final RecordReader<Rectangle, Iterable<S>> reader =
                inputFormat.createRecordReader(fsplit, null);
            if (reader instanceof SpatialRecordReader3) {
              ((SpatialRecordReader3)reader).initialize(fsplit, params);
            } else if (reader instanceof RTreeRecordReader3) {
              ((RTreeRecordReader3)reader).initialize(fsplit, params);
            } else if (reader instanceof HDFRecordReader) {
              ((HDFRecordReader)reader).initialize(fsplit, params);
            } else {
              throw new RuntimeException("Unknown record reader");
            }
            while (reader.nextKeyValue()) {
              Iterable<S> shapes = reader.getCurrentValue();
              for (S s : shapes) {
                if (s.geom == null)
                  continue;
                batch[batchSize++] = s.geom;
                if (batchSize >= MaxBatchSize) {
                  SpatialAlgorithms.multiUnion(batch, progress, output);
                  batchSize = 0;
                }
              }
            }
            reader.close();
          } catch (IOException e) {
            LOG.error("Error processing split "+splits.get(i), e);
          } catch (InterruptedException e) {
            LOG.error("Error processing split "+splits.get(i), e);
          }
        }
        // Union all remaining geometries
        try {
          Geometry[] finalBatch = new Geometry[batchSize];
          System.arraycopy(batch, 0, finalBatch, 0, batchSize);
          SpatialAlgorithms.multiUnion(finalBatch, progress, output);
          return localUnion;
        } catch (IOException e) {
          // Should never happen as the context is passed as null
          throw new RuntimeException("Error in local union", e);
        }
      }
    }, parallelism);
    
    // Write result to output
    LOG.info("Merge the results of all splits");
    int totalNumGeometries = 0;
    for (List<Geometry> result : results)
      totalNumGeometries += result.size();
    List<Geometry> allInOne = new ArrayList<Geometry>(totalNumGeometries);
    for (List<Geometry> result : results)
      allInOne.addAll(result);
    
    final S outShape = (S) params.getShape("shape");
    final PrintStream out;
    if (outPath == null || !params.getBoolean("output", true)) {
      // Skip writing the output
      out = new PrintStream(new NullOutputStream());
    } else {
      FileSystem outFS = outPath.getFileSystem(params);
      out = new PrintStream(outFS.create(outPath));
    }
    
    SpatialAlgorithms.multiUnion(allInOne.toArray(new Geometry[allInOne.size()]),
        new Progressable.NullProgressable() {
      int lastProgress = 0;
      public void progress(float p) {
        int newProgresss = (int) (p * 100);
        if (newProgresss > lastProgress) {
          LOG.info("Global union progress "+(lastProgress = newProgresss)+"%");
        }
      }
    }, new ResultCollector<Geometry>() {
      Text line = new Text2();
      
      @Override
      public void collect(Geometry r) {
        outShape.geom = r;
        outShape.toText(line);
        out.println(line);
      }
    });
    out.close();
  }

  public static Job union(Path inPath, Path outPath,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    if (OperationsParams.isLocal(params, inPath)) {
      unionLocal(inPath, outPath, params);
      return null;
    } else {
      return unionMapReduce(inPath, outPath, params);
    }
  }

  private static void printUsage() {
    System.out.println("Union");
    System.out.println("Finds the union of all shapes in the input file.");
    System.out.println("The output is one shape that represents the union of all shapes in input file.");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to file that contains all shapes");
    System.out.println("<output file>: (*) Path to output file.");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    
    if (!params.checkInputOutput()) {
      printUsage();
      return;
    }
    
    Path input = params.getInputPath();
    Path output = params.getOutputPath();
    Shape shape = params.getShape("shape");
    
    if (shape == null || !(shape instanceof OGCJTSShape)) {
      LOG.error("Given shape must be a subclass of "+OGCJTSShape.class);
      return;
    }

    long t1 = System.currentTimeMillis();
    union(input, output, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
}
