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
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.RTree;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialInputFormat2;
import edu.umn.cs.spatialHadoop.mapred.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
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
   * @author eldawy
   *
   * @param <T>
   */
  public static class RangeQueryMap extends
      Mapper<Rectangle, Iterable<Shape>, NullWritable, Shape> {
    /**A shape that is used to filter input*/
    private Shape queryShape;
    private Rectangle queryMbr;
    private final NullWritable dummy = NullWritable.get();
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      queryShape = OperationsParams.getShape(context.getConfiguration(), "rect");
      queryMbr = queryShape.getMBR();
    }

    @Override
    protected void map(final Rectangle cellMBR, Iterable<Shape> value,
        final Context context) throws IOException, InterruptedException {
      if (value instanceof RTree) {
        RTree<Shape> shapes = (RTree<Shape>) value;
        shapes.search(queryMbr, new ResultCollector<Shape>() {
          @Override
          public void collect(Shape shape) {
            try {
              boolean report_result = false;
              if (cellMBR.isValid()) {
                // Check for duplicate avoidance using reference point technique
                Rectangle intersection = queryMbr.getIntersection(shape.getMBR());
                report_result = cellMBR.contains(intersection.x1, intersection.y1);
              } else {
                // A heap block, report right away
                report_result = true;
              }
              if (report_result)
                context.write(dummy, shape);
            } catch (IOException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
      } else {
        for (Shape shape : value) {
          Rectangle shapeMBR = shape.getMBR();
          if (shapeMBR != null && shapeMBR.isIntersected(queryMbr)
              && shape.isIntersected(queryShape)) {
            boolean report_result = false;
            if (cellMBR.isValid()) {
              // Check for duplicate avoidance using reference point technique
              double reference_x = Math.max(queryMbr.x1, shapeMBR.x1);
              double reference_y = Math.max(queryMbr.y1, shapeMBR.y1);
              report_result = cellMBR.contains(reference_x, reference_y);
            } else {
              // A heap block, report right away
              report_result = true;
            }
            
            if (report_result)
              context.write(dummy, shape);
          }
        }
      }
    
    }
  }
  
  
  /**
   * The map function used for range query
   * @author eldawy
   *
   * @param <T>
   */
  public static class RangeQueryMapNoDupAvoidance extends
      Mapper<Rectangle, Iterable<Shape>, NullWritable, Shape> {
    /**A shape that is used to filter input*/
    private Shape queryShape;
    private Rectangle queryMbr;
    private final NullWritable dummy = NullWritable.get();

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      queryShape = OperationsParams.getShape(context.getConfiguration(), "rect");
      queryMbr = queryShape.getMBR();
    }
    

    @Override
    protected void map(Rectangle key, Iterable<Shape> value,
        final Context context) throws IOException,
        InterruptedException {
      if (value instanceof RTree) {
        RTree<Shape> shapes = (RTree<Shape>) value;
        shapes.search(queryMbr, new ResultCollector<Shape>() {
          @Override
          public void collect(Shape shape) {
            try {
              context.write(dummy, shape);
            } catch (IOException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
      } else {
        for (Shape shape : value) {
          if (shape.isIntersected(queryShape)) {
            context.write(dummy, shape);
          }
        }
      }
    }
    /**
     * Map function for non-indexed blocks
     */
    public void map(final Rectangle cellMbr, final Iterable<Shape> value,
        final OutputCollector<NullWritable, Shape> output, Reporter reporter)
            throws IOException {}
  }
  
  public static Job rangeQueryMapReduce(Path inFile, Path outFile,
      OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
    boolean overwrite = params.getBoolean("overwrite", false);
    Shape shape = OperationsParams.getShape(params, "shape");
    FileSystem outFs = inFile.getFileSystem(params);
    Path outputPath = outFile;
    if (outputPath == null) {
      do {
        outputPath = new Path(inFile.getName()+
            ".rangequery_"+(int)(Math.random() * 1000000));
      } while (outFs.exists(outputPath));
      params.setBoolean("output", false); // Avoid writing the output
    } else {
      if (outFs.exists(outputPath)) {
        if (overwrite) {
          outFs.delete(outputPath, true);
        } else {
          throw new RuntimeException("Output path already exists and -overwrite flag is not set");
        }
      }
    }
    // Use the built-in range filter of the input format
    params.set(SpatialInputFormat2.InputQueryRange, params.get("rect"));
    // Use multithreading in case it is running locally
    params.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    
    Job job = new Job(params, "RangeQuery");
    job.setJarByClass(RangeQuery.class);
    job.setNumReduceTasks(0);
    
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(shape.getClass());
    
    FileSystem inFs = inFile.getFileSystem(params);
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inFile);
    
    GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(inFs, inFile);
    if (gIndex != null && gIndex.isReplicated())
      job.setMapperClass(RangeQueryMap.class);
    else
      job.setMapperClass(RangeQueryMapNoDupAvoidance.class);

    if (params.getBoolean("output", true)) {
      job.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job, outputPath);
    } else {
      // Skip writing the output for the sake of debugging
      job.setOutputFormatClass(NullOutputFormat.class);
    }
    // Submit the job
    if (!params.is("background")) {
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
   * @param inFile
   * @param params
   * @param output
   * @return
   * @throws IOException
   */
  public static <S extends Shape> long rangeQueryLocal(Path inPath,
      final Shape queryRange, final S shape,
      final OperationsParams params, final ResultCollector<S> output) throws IOException {
    // Set MBR of query shape in job configuration to work with the spatial filter
    OperationsParams.setShape(params, "rect", queryRange.getMBR());
    params.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    final FileSystem inFS = inPath.getFileSystem(params);
    // 1- Split the input path/file to get splits that can be processed independently
    ShapeIterInputFormat inputFormat = new ShapeIterInputFormat();
    JobConf job = new JobConf(params);
    ShapeIterInputFormat.setInputPaths(job, inPath);
    final InputSplit[] splits = inputFormat.getSplits(job, Runtime.getRuntime().availableProcessors());
    
    // 2- Process splits in parallel
    Vector<Long> results = Parallel.forEach(splits.length, new RunnableRange<Long>() {
      @Override
      public Long run(int i1, int i2) {
        S privateShape = (S) shape.clone();
        long results = 0;
        for (int i = i1; i < i2; i++) {
          try {
            FileSplit fsplit = (FileSplit) splits[i];
            if (fsplit.getStart() == 0 && SpatialSite.isRTree(inFS, fsplit.getPath())) {
              // Handle an RTree
              RTree<S> rtree = SpatialSite.loadRTree(inFS, fsplit.getPath(), privateShape);
              results += rtree.search(queryRange, output);
              rtree.close();
            } else {
              // Handle a heap file
              ShapeIterRecordReader reader = new ShapeIterRecordReader(params, fsplit);
              reader.setShape(privateShape);
              Rectangle key = reader.createKey();
              ShapeIterator shapes = reader.createValue();
              while (reader.next(key, shapes)) {
                for (Shape s : shapes) {
                  if (queryRange.isIntersected(s)) {
                    results++;
                    if (output != null)
                      output.collect((S) s);
                  }
                }
              }
              reader.close();
            }
          } catch (IOException e) {
            LOG.error("Error processing split "+splits[i], e);
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
    Vector<Long> resultsCounts = new Vector<Long>();
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
            try {
              ResultCollector<Shape> collector = null;
              if (output != null) {
                FileSystem outFS = output.getFileSystem(queryParams);
                final FSDataOutputStream outFile = outFS.create(output);
                final Text tempText = new Text2();
                collector = new ResultCollector<Shape>() {
                  @Override
                  public void collect(Shape r) {
                    try {
                      tempText.clear();
                      r.toText(tempText);
                      outFile.write(tempText.getBytes(), 0, tempText.getLength());
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
                  }
                };
              }
              rangeQueryLocal(inPath, queryRange, shape, queryParams, collector);
            } catch (IOException e) {
              e.printStackTrace();
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
