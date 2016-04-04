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
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.Estimator;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;


/**
 * Calculates number of records in a file depending on its type. If the file
 * is a text file, it counts number of lines. If it's a grid file with no local
 * index, it counts number of non-empty lines. If it's a grid file with RTree
 * index, it counts total number of records stored in all RTrees.
 * @author Ahmed Eldawy
 *
 */
public class RecordCount {

  public static class Map extends MapReduceBase implements
      Mapper<CellInfo, Text, NullWritable, LongWritable> {
    private static final NullWritable Dummy = NullWritable.get();
    private static final LongWritable ONEL = new LongWritable(1);

    public void map(CellInfo lineId, Text line,
        OutputCollector<NullWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      output.collect(Dummy, ONEL);
    }
  }
  
  public static class Reduce extends MapReduceBase implements
  Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
    @Override
    public void reduce(NullWritable dummy, Iterator<LongWritable> values,
        OutputCollector<NullWritable, LongWritable> output, Reporter reporter)
            throws IOException {
      long total_lines = 0;
      while (values.hasNext()) {
        LongWritable next = values.next();
        total_lines += next.get();
      }
      output.collect(dummy, new LongWritable(total_lines));
    }
  }
  
  /**
   * Counts the exact number of lines in a file by issuing a MapReduce job
   * that does the thing
   * @param fs
   * @param inFile
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public static long recordCountMapReduce(FileSystem fs, Path inFile) throws IOException, InterruptedException {
    JobConf job = new JobConf(RecordCount.class);
    
    Path outputPath = new Path(inFile.toUri().getPath()+".linecount");
    FileSystem outFs = outputPath.getFileSystem(job);
    outFs.delete(outputPath, true);
    
    job.setJobName("LineCount");
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);
    
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(1);
    
    job.setInputFormat(ShapeLineInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeLineInputFormat.setInputPaths(job, inFile);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    JobClient.runJob(job);
    
    // Read job result
    if (OperationsParams.isLocal(job, inFile)) {
      // Enforce local execution if explicitly set by user or for small files
      job.set("mapred.job.tracker", "local");
      // Use multithreading too
      job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    }
    
    long lineCount = 0;
    FileStatus[] results = outFs.listStatus(outputPath);
    for (FileStatus fileStatus : results) {
      if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
        LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
        Text text = new Text();
        if (lineReader.readLine(text) > 0) {
          lineCount = Long.parseLong(text.toString());
        }
        lineReader.close();
      }
    }
    
    outFs.delete(outputPath, true);
    
    return lineCount;
  }
  
  /**
   * Counts the approximate number of lines in a file by getting an approximate
   * average line length
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static<T> long recordCountApprox(FileSystem fs, Path file) throws IOException {
    final long fileSize = fs.getFileStatus(file).getLen();
    final FSDataInputStream in = fs.open(file);
    
    Estimator<Long> lineEstimator = new Estimator<Long>(0.05);
    lineEstimator.setRandomSample(new Estimator.RandomSample() {
      
      @Override
      public double next() {
        int lineLength = 0;
        try {
          long randomFilePosition = (long)(Math.random() * fileSize);
          in.seek(randomFilePosition);
          
          // Skip the rest of this line
          byte lastReadByte;
          do {
            lastReadByte = in.readByte();
          } while (lastReadByte != '\n' && lastReadByte != '\r');

          while (in.getPos() < fileSize - 1) {
            lastReadByte = in.readByte();
            if (lastReadByte == '\n' || lastReadByte == '\r') {
              break;
            }
            lineLength++;
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        return lineLength+1;
      }
    });
    
    lineEstimator.setUserFunction(new Estimator.UserFunction<Long>() {
      @Override
      public Long calculate(double x) {
        return (long)(fileSize / x);
      }
    });
    
    lineEstimator.setQualityControl(new Estimator.QualityControl<Long>() {
      
      @Override
      public boolean isAcceptable(Long y1, Long y2) {
        return (double)Math.abs(y2 - y1) / Math.min(y1, y2) < 0.01;
      }
    });
    
    Estimator.Range<Long> lineCount = lineEstimator.getEstimate();
    in.close();
    
    return (lineCount.limit1 + lineCount.limit2) / 2;
  }
  
  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    JobConf conf = new JobConf(RecordCount.class);
    Path inputFile = params.getPath();
    FileSystem fs = inputFile.getFileSystem(conf);
    if (!fs.exists(inputFile)) {
      throw new RuntimeException("Input file does not exist");
    }
    boolean random = params.getBoolean("random", false);
    long lineCount;
    if (random) {
      lineCount = recordCountApprox(fs, inputFile);
    } else {
      lineCount = recordCountMapReduce(fs, inputFile);
    }
    System.out.println("Count of records in "+inputFile+" is "+lineCount);
  }

}
