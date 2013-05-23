package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.spatial.BlockFilter;
import org.apache.hadoop.mapred.spatial.RTreeInputFormat;
import org.apache.hadoop.mapred.spatial.RangeFilter;
import org.apache.hadoop.mapred.spatial.ShapeInputFormat;
import org.apache.hadoop.mapred.spatial.ShapeRecordReader;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.ResultCollector;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

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
  
  /**
   * The map function used for range query
   * @author eldawy
   *
   * @param <T>
   */
  public static class RangeQueryMap extends MapReduceBase implements
      Mapper<CellInfo, Writable, NullWritable, Shape> {
    /**A shape that is used to filter input*/
    private Shape queryShape;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      try {
        String queryShapeClassName = job.get(QUERY_SHAPE_CLASS);
        Class<? extends Shape> queryShapeClass =
            Class.forName(queryShapeClassName).asSubclass(Shape.class);
        queryShape = queryShapeClass.newInstance();
        queryShape.fromText(new Text(job.get(QUERY_SHAPE)));
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
    public void map(final CellInfo cellInfo, final Writable value,
        final OutputCollector<NullWritable, Shape> output, Reporter reporter)
            throws IOException {
      if (value instanceof Shape) {
        Shape shape = (Shape) value;
        if (shape.isIntersected(queryShape)) {
          boolean report_result = false;
          if (cellInfo.cellId == -1) {
            // A heap block, report right away
            report_result = true;
          } else {
            // Check for duplicate avoidance using reference point technique
            Rectangle intersection =
                queryShape.getMBR().getIntersection(shape.getMBR());
            report_result = cellInfo.contains(intersection.x, intersection.y);
          }
          if (report_result)
            output.collect(dummy, shape);
        }
      } else if (value instanceof RTree) {
        RTree<Shape> shapes = (RTree<Shape>) value;
        shapes.search(queryShape.getMBR(), new ResultCollector<Shape>() {
          @Override
          public void collect(Shape shape) {
            try {
              boolean report_result = false;
              if (cellInfo.cellId == -1) {
                // A heap block, report right away
                report_result = true;
              } else {
                // Check for duplicate avoidance using reference point technique
                Rectangle intersection =
                    queryShape.getMBR().getIntersection(shape.getMBR());
                report_result = cellInfo.contains(intersection.x, intersection.y);
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
      Path userOutputPath, Shape queryShape, Shape shape, boolean overwrite)
      throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
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
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(0);
    job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    RangeFilter.setQueryRange(job, queryShape); // Set query range for filter

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
    job.setMapperClass(RangeQueryMap.class);

    // Set query range for the map function
    job.set(QUERY_SHAPE_CLASS, queryShape.getClass().getName());
    job.set(QUERY_SHAPE, queryShape.toText(new Text()).toString());
    
    // Set shape class for the SpatialInputFormat
    job.set(SpatialSite.SHAPE_CLASS, shape.getClass().getName());
    
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, inputFile);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();
    
    // If outputPath not set by user, automatically delete it
    if (userOutputPath == null)
      outFs.delete(outputPath, true);

    return resultCount;
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
    CellInfo cell = shapeReader.createKey();

    while (shapeReader.next(cell, shape)) {
      if (shape.isIntersected(queryRange)) {
        boolean report_result;
        if (cell.cellId == -1) {
          report_result = true;
        } else {
          // Check for duplicate avoidance
          Rectangle intersection_mbr =
              queryRange.getMBR().getIntersection(shape.getMBR());
          report_result = cell.contains(intersection_mbr.x, intersection_mbr.y);
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
    if (paths.length == 0 || (cla.getRectangle() == null &&
        cla.getSelectionRatio() < 0.0f)) {
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
    final Rectangle[] queryRanges = cla.getRectangles();
    int concurrency = cla.getConcurrency();
    final Shape stockShape = cla.getShape(true);
    final boolean overwrite = cla.isOverwrite();

    final long[] results = new long[queryRanges.length];
    final Vector<Thread> threads = new Vector<Thread>();
    
    Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        ex.printStackTrace();
        throw new RuntimeException("Error running a thread");
      }
    };

    for (int i = 0; i < queryRanges.length; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
            try {
              int thread_i = threads.indexOf(this);
              long result_count = rangeQueryMapReduce(fs, inputFile, outputPath,
                  queryRanges[thread_i], stockShape, overwrite);
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
    System.out.println("Time for "+queryRanges.length+" jobs is "+(t2-t1)+" millis");
    
    System.out.print("Result size: [");
    for (long result : results) {
      System.out.print(result+", ");
    }
    System.out.println("]");
  }
}
