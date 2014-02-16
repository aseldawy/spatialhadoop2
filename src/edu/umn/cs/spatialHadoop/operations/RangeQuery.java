/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.RTree;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.RTreeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * Performs a range query over a spatial file.
 * @author Ahmed Eldawy
 *
 */
public class RangeQuery {
  /**Logger for RangeQuery*/
  private static final Log LOG = LogFactory.getLog(RangeQuery.class);
  
  /**Name of the config line that stores the class name of the query shape*/
  public static final String QUERY_SHAPE_CLASS =
      "edu.umn.cs.spatialHadoop.operations.RangeQuery.QueryShapeClass";

  /**Name of the config line that stores the query shape*/
  public static final String QUERY_SHAPE =
      "edu.umn.cs.spatialHadoop.operations.RangeQuery.QueryShape";

  /**Reference to the last range query job submitted*/
  public static RunningJob lastRunningJob;
  
  /**
   * A filter function that selects partitions overlapping with a query range.
   * @author Ahmed Eldawy
   *
   */
  public static class RangeFilter extends DefaultBlockFilter {
    
    /**Name of the config line that stores the query shape*/
    private static final String QUERY_SHAPE = "RangeFilter.QueryShape";

    /**A shape that is used to filter input*/
    private Shape queryRange;
    
    /**
     * Sets the query range in the given job.
     * @param job
     * @param shape
     */
    public static void setQueryRange(JobConf job, Shape shape) {
      SpatialSite.setShape(job, QUERY_SHAPE, shape);
    }
    
    public static Shape getQueryRange(JobConf job) {
      return SpatialSite.getShape(job, QUERY_SHAPE);
    }
    
    @Override
    public void configure(JobConf job) {
      this.queryRange = SpatialSite.getShape(job, QUERY_SHAPE);
    }
    
    @Override
    public void selectCells(GlobalIndex<Partition> gIndex,
        ResultCollector<Partition> output) {
      int numPartitions;
      if (gIndex.isReplicated()) {
        // Need to process all partitions to perform duplicate avoidance
        numPartitions = gIndex.rangeQuery(queryRange, output);
        LOG.info("Selected "+numPartitions+" partitions overlapping "+queryRange);
      } else {
        Rectangle queryRange = this.queryRange.getMBR();
        // Need to process only partitions on the perimeter of the query range
        // Partitions that are totally contained in query range should not be
        // processed and should be copied to output directly
        numPartitions = 0;
        for (Partition p : gIndex) {
          if (queryRange.contains(p)) {
            // TODO partitions totally contained in query range should be copied
            // to output directly

            // XXX Until hard links are supported, R-tree blocks are processed
            // similar to R+-tree
            output.collect(p);
            numPartitions++;
          } else if (p.isIntersected(queryRange)) {
            output.collect(p);
            numPartitions++;
          }
        }
        LOG.info("Selected "+numPartitions+" partitions on the perimeter of "+queryRange);
      }
    }
  }
  
  
  /**
   * The map function used for range query
   * @author eldawy
   *
   * @param <T>
   */
  public static class RangeQueryMap extends MapReduceBase implements
      Mapper<Rectangle, Writable, NullWritable, Shape> {
    /**A shape that is used to filter input*/
    private Shape queryShape;
    private Rectangle queryMbr;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      try {
        String queryShapeClassName = job.get(QUERY_SHAPE_CLASS);
        Class<? extends Shape> queryShapeClass =
            Class.forName(queryShapeClassName).asSubclass(Shape.class);
        queryShape = queryShapeClass.newInstance();
        queryShape.fromText(new Text(job.get(QUERY_SHAPE)));
        queryMbr = queryShape.getMBR();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    
    private final NullWritable dummy = NullWritable.get();
    
    /**
     * Map function for non-indexed blocks
     */
    public void map(final Rectangle cellMbr, final Writable value,
        final OutputCollector<NullWritable, Shape> output, Reporter reporter)
            throws IOException {
      if (value instanceof Shape) {
        Shape shape = (Shape) value;
        if (shape.isIntersected(queryShape)) {
          boolean report_result = false;
          if (cellMbr.isValid()) {
            // Check for duplicate avoidance using reference point technique
            double reference_x = Math.max(queryMbr.x1, shape.getMBR().x1);
            double reference_y = Math.max(queryMbr.y1, shape.getMBR().y1);
            report_result = cellMbr.contains(reference_x, reference_y);
          } else {
            // A heap block, report right away
            report_result = true;
          }
          
          if (report_result)
            output.collect(dummy, shape);
        }
      } else if (value instanceof RTree) {
        RTree<Shape> shapes = (RTree<Shape>) value;
        shapes.search(queryMbr, new ResultCollector<Shape>() {
          @Override
          public void collect(Shape shape) {
            try {
              boolean report_result = false;
              if (cellMbr.isValid()) {
                // Check for duplicate avoidance using reference point technique
                Rectangle intersection = queryMbr.getIntersection(shape.getMBR());
                report_result = cellMbr.contains(intersection.x1, intersection.y1);
              } else {
                // A heap block, report right away
                report_result = true;
              }
              if (report_result)
                output.collect(dummy, shape);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
      }
    }
  }
  
  
  /**
   * The map function used for range query
   * @author eldawy
   *
   * @param <T>
   */
  public static class RangeQueryMapNoDupAvoidance extends MapReduceBase implements
      Mapper<Rectangle, Writable, NullWritable, Shape> {
    /**A shape that is used to filter input*/
    private Shape queryShape;
    private Rectangle queryMbr;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      try {
        String queryShapeClassName = job.get(QUERY_SHAPE_CLASS);
        Class<? extends Shape> queryShapeClass =
            Class.forName(queryShapeClassName).asSubclass(Shape.class);
        queryShape = queryShapeClass.newInstance();
        queryShape.fromText(new Text(job.get(QUERY_SHAPE)));
        queryMbr = queryShape.getMBR();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    
    private final NullWritable dummy = NullWritable.get();
    
    /**
     * Map function for non-indexed blocks
     */
    public void map(final Rectangle cellMbr, final Writable value,
        final OutputCollector<NullWritable, Shape> output, Reporter reporter)
            throws IOException {
      if (value instanceof Shape) {
        Shape shape = (Shape) value;
        if (shape.isIntersected(queryShape)) {
          output.collect(dummy, shape);
        }
      } else if (value instanceof RTree) {
        RTree<Shape> shapes = (RTree<Shape>) value;
        shapes.search(queryMbr, new ResultCollector<Shape>() {
          @Override
          public void collect(Shape shape) {
            try {
              output.collect(dummy, shape);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
      }
    }
  }
  
  /**
   * Performs a range query using MapReduce
   * @param fs
   * @param inputFile
   * @param queryRange
   * @param shape
   * @param output
   * @return
   * @throws IOException
   */
  public static long rangeQueryMapReduce(FileSystem fs, Path inputFile,
      Path userOutputPath, Shape queryShape, Shape shape, boolean overwrite,
      boolean background)
      throws IOException {
    JobConf job = new JobConf(shape.getClass());
    
    FileSystem outFs = inputFile.getFileSystem(job);
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path(inputFile.toUri().getPath()+
            ".rangequery_"+(int)(Math.random() * 1000000));
      } while (outFs.exists(outputPath));
    } else {
      if (outFs.exists(outputPath)) {
        if (overwrite) {
          outFs.delete(outputPath, true);
        } else {
          throw new RuntimeException("Output path already exists and -overwrite flag is not set");
        }
      }
    }
    
    job.setJobName("RangeQuery");
    job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    RangeFilter.setQueryRange(job, queryShape); // Set query range for filter

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    // Decide which map function to use depending on how blocks are indexed
    // And also which input format to use
    if (SpatialSite.isRTree(fs, inputFile)) {
      // RTree indexed file
      LOG.info("Searching an RTree indexed file");
      job.setInputFormat(RTreeInputFormat.class);
    } else {
      // A file with no local index
      LOG.info("Searching a non local-indexed file");
      job.setInputFormat(ShapeInputFormat.class);
    }
    
    GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(fs, inputFile);
    if (gIndex != null && gIndex.isReplicated())
      job.setMapperClass(RangeQueryMap.class);
    else
      job.setMapperClass(RangeQueryMapNoDupAvoidance.class);

    // Set query range for the map function
    job.set(QUERY_SHAPE_CLASS, queryShape.getClass().getName());
    job.set(QUERY_SHAPE, queryShape.toText(new Text()).toString());
    
    // Set shape class for the SpatialInputFormat
    SpatialSite.setShapeClass(job, shape.getClass());
    
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, inputFile);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    if (!background) {
      RunningJob runningJob = JobClient.runJob(job);
      Counters counters = runningJob.getCounters();
      Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
      final long resultCount = outputRecordCounter.getValue();
      
      // If outputPath not set by user, automatically delete it
      if (userOutputPath == null)
        outFs.delete(outputPath, true);
      
      return resultCount;
    } else {
      JobClient jc = new JobClient(job);
      lastRunningJob = jc.submitJob(job);
      return -1;
    }
  }
  
  /**
   * Runs a range query on the local machine by iterating over the whole file.
   * @param fs - FileSystem that contains input file
   * @param file - path to the input file
   * @param queryRange - The range to look in
   * @param shape - An instance of the shape stored in file
   * @param output - Output is sent to this collector. If <code>null</code>,
   *  output is not collected and only the number of results is returned. 
   * @return number of results found
   * @throws IOException
   */
  public static<S extends Shape> long rangeQueryLocal(FileSystem fs, Path file,
      Shape queryRange, S shape, ResultCollector<S> output)
      throws IOException {
    long file_size = fs.getFileStatus(file).getLen();
    ShapeRecordReader<S> shapeReader =
        new ShapeRecordReader<S>(fs.open(file), 0, file_size);

    long resultCount = 0;
    Rectangle cell = shapeReader.createKey();

    while (shapeReader.next(cell, shape)) {
      if (shape.isIntersected(queryRange)) {
        boolean report_result;
        if (cell.isValid()) {
          // Check for duplicate avoidance
          Rectangle intersection_mbr =
              queryRange.getMBR().getIntersection(shape.getMBR());
          report_result = cell.contains(intersection_mbr.x1, intersection_mbr.y1);
        } else {
          report_result = true;
        }
        if (report_result) {
          resultCount++;
          if (output != null) {
            output.collect(shape);
          }
        }
      }
    }
    shapeReader.close();
    return resultCount;
  }
  
  private static void printUsage() {
    System.out.println("Performs a range query on an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - Path to output file");
    System.out.println("rect:<x,y,w,h> - (*) Query rectangle");
    System.out.println("-overwrite - Overwrite output file without notice");
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    final Path[] paths = cla.getPaths();
    if (paths.length == 0 || (cla.getShape("rect") == null &&
        cla.getFloat("ratio", -1.0f) < 0.0f)) {
      printUsage();
      throw new RuntimeException("Illegal parameters");
    }
    JobConf conf = new JobConf(FileMBR.class);
    final Path inputFile = paths[0];
    final FileSystem fs = inputFile.getFileSystem(conf);
    if (!fs.exists(inputFile)) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }
    final Path outputPath = paths.length > 1 ? paths[1] : null;
    final Rectangle[] queryRanges = cla.getShapes("rect", new Rectangle());
    int concurrency = cla.getInt("concurrency", 1);
    final Shape stockShape = cla.getShape("shape");
    final boolean overwrite = cla.is("overwrite");

    final long[] results = new long[queryRanges.length];
    final Vector<Thread> threads = new Vector<Thread>();

    final BooleanWritable exceptionHappened = new BooleanWritable();
    
    Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        ex.printStackTrace();
        exceptionHappened.set(true);
      }
    };

    for (int i = 0; i < queryRanges.length; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
            try {
              int thread_i = threads.indexOf(this);
              long result_count = rangeQueryMapReduce(fs, inputFile, outputPath,
                  queryRanges[thread_i], stockShape, overwrite, false);
              results[thread_i] = result_count;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
        }
      };
      t.setUncaughtExceptionHandler(h);
      threads.add(t);
    }

    long t1 = System.currentTimeMillis();
    do {
      // Ensure that there is at least MaxConcurrentThreads running
      int i = 0;
      while (i < concurrency && i < threads.size()) {
        Thread.State state = threads.elementAt(i).getState(); 
        if (state == Thread.State.TERMINATED) {
          // Thread already terminated, remove from the queue
          threads.remove(i);
        } else if (state == Thread.State.NEW) {
          // Start the thread and move to next one
          threads.elementAt(i++).start();
        } else {
          // Thread is still running, skip over it
          i++;
        }
      }
      if (!threads.isEmpty()) {
        try {
          // Sleep for 10 seconds or until the first thread terminates
          threads.firstElement().join(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } while (!threads.isEmpty());
    long t2 = System.currentTimeMillis();
    
    if (exceptionHappened.get())
      throw new RuntimeException("Not all jobs finished correctly");
    System.out.println("Time for "+queryRanges.length+" jobs is "+(t2-t1)+" millis");
    
    System.out.print("Result size: [");
    for (long result : results) {
      System.out.print(result+", ");
    }
    System.out.println("]");
  }
}
