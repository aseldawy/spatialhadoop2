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
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;
import edu.umn.cs.spatialHadoop.util.ResultCollectorSynchronizer;

/**
 * Performs a range query over a spatial file.
 * @author Ahmed Eldawy
 *
 */
public class RangeQuery {
  /**Logger for RangeQuery*/
  static final Log LOG = LogFactory.getLog(RangeQuery.class);
  
  /**
   * The map function used for range query
   * 
   * @author Ahmed Eldawy
   */
  public static class RangeQueryMap extends
      Mapper<Rectangle, Iterable<Shape>, NullWritable, Shape> {
    @Override
    protected void map(final Rectangle cellMBR, Iterable<Shape> value,
        final Context context) throws IOException, InterruptedException {
      NullWritable dummyKey = NullWritable.get();
      for (Shape s : value) {
        context.write(dummyKey, s);
      }
    }
  }
  
  public static Job rangeQueryMapReduce(Path inFile, Path outFile,
      OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
    // Use the built-in range filter of the input format
    params.set(SpatialInputFormat3.InputQueryRange, params.get("rect"));
    // Use multithreading in case it is running locally
    params.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    
    Job job = new Job(params, "RangeQuery");
    job.setJarByClass(RangeQuery.class);
    job.setNumReduceTasks(0);
    
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inFile);
    
    job.setMapperClass(RangeQueryMap.class);

    if (params.getBoolean("output", true) && outFile != null) {
      job.setOutputFormatClass(TextOutputFormat3.class);
      TextOutputFormat3.setOutputPath(job, outFile);
    } else {
      // Skip writing the output for the sake of debugging
      job.setOutputFormatClass(NullOutputFormat.class);
    }
    // Submit the job
    if (!params.getBoolean("background", false)) {
      job.waitForCompletion(false);
    } else {
      job.submit();
    }
    return job;
  }
  
  /**
   * Runs a range query on the local machine (no MapReduce) and the output is
   * streamed to the provided result collector. The query might run in parallel
   * which makes it necessary to design the result collector to accept parallel
   * calls to the method {@link ResultCollector#collect(Object)}.
   * You can use {@link ResultCollectorSynchronizer} to synchronize calls to
   * your ResultCollector if you cannot design yours to be thread safe.
   * @param inPath
   * @param queryRange
   * @param shape
   * @param params
   * @param output
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public static <S extends Shape> long rangeQueryLocal(Path inPath,
      final Shape queryRange, final S shape,
      final OperationsParams params, final ResultCollector<S> output) throws IOException, InterruptedException {
    // Set MBR of query shape in job configuration to work with the spatial filter
    OperationsParams.setShape(params, SpatialInputFormat3.InputQueryRange, queryRange.getMBR());
    // 1- Split the input path/file to get splits that can be processed independently
    final SpatialInputFormat3<Rectangle, S> inputFormat =
        new SpatialInputFormat3<Rectangle, S>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inPath);
    final List<InputSplit> splits = inputFormat.getSplits(job);
    
    // 2- Process splits in parallel
    List<Long> results = Parallel.forEach(splits.size(), new RunnableRange<Long>() {
      @Override
      public Long run(int i1, int i2) {
        long results = 0;
        for (int i = i1; i < i2; i++) {
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
              for (Shape s : shapes) {
                results++;
                if (output != null)
                  output.collect((S) s);
              }
            }
            reader.close();
          } catch (IOException e) {
            LOG.error("Error processing split "+splits.get(i), e);
          } catch (InterruptedException e) {
            LOG.error("Error processing split "+splits.get(i), e);
          }
        }
        return results;
      }
    });
    long totalResultSize = 0;
    for (long result : results)
      totalResultSize += result;
    return totalResultSize;
  }
  
  private static void printUsage() {
    System.out.println("Performs a range query on an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - Path to output file");
    System.out.println("shape:<point|rectangle|polygon> - (*) Type of shapes stored in input file");
    System.out.println("rect:<x1,y1,x2,y2> - (*) Query rectangle");
    System.out.println("-overwrite - Overwrite output file without notice");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    
    final Path[] paths = params.getPaths();
    if (paths.length <= 1 && !params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    if (paths.length >= 2 && !params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    if (params.get("rect") == null) {
      System.err.println("You must provide a query range");
      printUsage();
      System.exit(1);
    }
    final Path inPath = params.getInputPath();
    final Path outPath = params.getOutputPath();
    final Rectangle[] queryRanges = params.getShapes("rect", new Rectangle());

    // All running jobs
    final Vector<Long> resultsCounts = new Vector<Long>();
    Vector<Job> jobs = new Vector<Job>();
    Vector<Thread> threads = new Vector<Thread>();

    long t1 = System.currentTimeMillis();
    for (int i = 0; i < queryRanges.length; i++) {
      final OperationsParams queryParams = new OperationsParams(params);
      OperationsParams.setShape(queryParams, "rect", queryRanges[i]);
      if (OperationsParams.isLocal(new JobConf(queryParams), inPath)) {
        // Run in local mode
        final Rectangle queryRange = queryRanges[i];
        final Shape shape = queryParams.getShape("shape");
        final Path output = outPath == null ? null :
          (queryRanges.length == 1 ? outPath : new Path(outPath, String.format("%05d", i)));
        Thread thread = new Thread() {
          @Override
          public void run() {
            FSDataOutputStream outFile = null;
            final byte[] newLine = System.getProperty("line.separator", "\n").getBytes();
            try {
              ResultCollector<Shape> collector = null;
              if (output != null) {
                FileSystem outFS = output.getFileSystem(queryParams);
                final FSDataOutputStream foutFile = outFile = outFS.create(output);
                collector = new ResultCollector<Shape>() {
                  final Text tempText = new Text2();
                  @Override
                  public synchronized void collect(Shape r) {
                    try {
                      tempText.clear();
                      r.toText(tempText);
                      foutFile.write(tempText.getBytes(), 0, tempText.getLength());
                      foutFile.write(newLine);
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
                  }
                };
              } else {
                outFile = null;
              }
              long resultCount = rangeQueryLocal(inPath, queryRange, shape, queryParams, collector);
              resultsCounts.add(resultCount);
            } catch (IOException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } finally {
              try {
                if (outFile != null)
                  outFile.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        };
        thread.start();
        threads.add(thread);
      } else {
        // Run in MapReduce mode
        queryParams.setBoolean("background", true);
        Job job = rangeQueryMapReduce(inPath, outPath, queryParams);
        jobs.add(job);
      }
    }

    while (!jobs.isEmpty()) {
      Job firstJob = jobs.firstElement();
      firstJob.waitForCompletion(false);
      if (!firstJob.isSuccessful()) {
        System.err.println("Error running job "+firstJob);
        System.err.println("Killing all remaining jobs");
        for (int j = 1; j < jobs.size(); j++)
          jobs.get(j).killJob();
        System.exit(1);
      }
      Counters counters = firstJob.getCounters();
      Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
      resultsCounts.add(outputRecordCounter.getValue());
      jobs.remove(0);
    }
    while (!threads.isEmpty()) {
      try {
        Thread thread = threads.firstElement();
        thread.join();
        threads.remove(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    long t2 = System.currentTimeMillis();
    
    System.out.println("Time for "+queryRanges.length+" jobs is "+(t2-t1)+" millis");
    System.out.println("Results counts: "+resultsCounts);
  }
}
