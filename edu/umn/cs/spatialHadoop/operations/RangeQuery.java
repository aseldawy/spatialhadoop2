package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

/**
 * Performs a range query over a spatial file.
 * @author eldawy
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
  public static class RangeQueryMap<T extends Shape> extends MapReduceBase {
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
    public void map(CellInfo cellInfo, T shape,
        OutputCollector<NullWritable, T> output, Reporter reporter)
            throws IOException {
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
    }
    
    /**
     * Map function for RTree indexed blocks
     * @param shapeId
     * @param shapes
     * @param output
     * @param reporter
     */
    public void map(final CellInfo cellInfo, RTree<T> shapes,
        final OutputCollector<NullWritable, T> output, Reporter reporter) {
      shapes.search(queryShape.getMBR(), new ResultCollector<T>() {
        @Override
        public void collect(T shape) {
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
  
  /** Mapper for non-indexed blocks */
  public static class Map1<T extends Shape> extends RangeQueryMap<T> implements
      Mapper<CellInfo, T, NullWritable, T> { }

  /** Mapper for RTree indexed blocks */
  public static class Map2<T extends Shape> extends RangeQueryMap<T> implements
      Mapper<CellInfo, RTree<T>, NullWritable, T> { }
  
  /**
   * Performs a range query using MapReduce
   * @param fs
   * @param file
   * @param queryRange
   * @param shape
   * @param output
   * @return
   * @throws IOException
   */
  public static long rangeQueryMapReduce(FileSystem fs, Path file,
      Shape queryShape, Shape shape, ResultCollector<Shape> output,
      Text outputPathText)
      throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath;
    FileSystem outFs = file.getFileSystem(job);
    do {
      outputPath = new Path("/"+file.getName()+
          ".rangequery_"+(int)(Math.random() * 1000000));
    } while (outFs.exists(outputPath));
    if (outputPathText != null)
      outputPathText.set(outputPath.toString());
    
    job.setJobName("RangeQuery");
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setBoolean(SpatialSite.AutoCombineSplits, false);
    job.setNumReduceTasks(0);
    job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    RangeFilter.setQueryRange(job, queryShape); // Set query range for filter

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    // Decide which map function to use depending on how blocks are indexed
    // And also which input format to use
    FSDataInputStream in = fs.open(file);
    if (in.readLong() == SpatialSite.RTreeFileMarker) {
      // RTree indexed file
      LOG.info("Searching an RTree indexed file");
      job.setMapperClass(Map2.class);
      job.setInputFormat(RTreeInputFormat.class);
    } else {
      // A file with no local index
      LOG.info("Searching a non local-indexed file");
      job.setMapperClass(Map1.class);
      job.setInputFormat(ShapeInputFormat.class);
    }
    in.close();

    // Set query range for the map function
    job.set(QUERY_SHAPE_CLASS, queryShape.getClass().getName());
    job.set(QUERY_SHAPE, queryShape.toText(new Text()).toString());
    
    // Set shape class for the SpatialInputFormat
    job.set(SpatialSite.SHAPE_CLASS, shape.getClass().getName());
    
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();
    
    // Read job result
    if (output != null) {
      Text line = new Text();
      FileStatus[] results = outFs.listStatus(outputPath);
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0
            && fileStatus.getPath().getName().startsWith("part-")) {
          // Report every single result
          LineReader lineReader = new LineReader(outFs.open(fileStatus
              .getPath()));
          line.clear();
          while (lineReader.readLine(line) > 0) {
            shape.fromText(line);
            output.collect(shape);
          }
          lineReader.close();
        }
      }
    }
    
    if (outputPathText == null)
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
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(FileMBR.class);
    final Path inputFile = cla.getPath();
    Rectangle queryRange = cla.getRectangle();
    final FileSystem fs = inputFile.getFileSystem(conf);
    int count = cla.getCount();
    final float ratio = cla.getSelectionRatio();
    int concurrency = cla.getConcurrency();
    final Shape stockShape = cla.getShape(true);
    boolean local = cla.isLocal();
    long seed = cla.getSeed();

    final Vector<Long> results = new Vector<Long>();
    
    if (ratio >= 0.0 && ratio <= 1.0f) {
      final Rectangle queryMBR = queryRange != null?
          queryRange :
            (local ? FileMBR.fileMBRLocal(fs, inputFile, stockShape) :
              FileMBR.fileMBRMapReduce(fs, inputFile, stockShape));
      final Vector<Thread> threads = new Vector<Thread>();
      final Vector<Rectangle> query_rectangles = new Vector<Rectangle>();
      Sampler.sampleLocal(fs, inputFile, count, seed, new ResultCollector<Shape>(){
        @Override
        public void collect(final Shape value) {
          Rectangle query_rectangle = new Rectangle();
          query_rectangle.width = (long) (queryMBR.width * ratio);
          query_rectangle.height = (long) (queryMBR.height * ratio);
          query_rectangle.x = value.getMBR().x - query_rectangle.width / 2;
          query_rectangle.y = value.getMBR().y - query_rectangle.height / 2;
          query_rectangles.add(query_rectangle);
          threads.add(new Thread() {
            @Override
            public void run() {
              try {
                int thread_i = threads.indexOf(this);
                long result_count = rangeQueryMapReduce(fs, inputFile,
                    query_rectangles.elementAt(thread_i), stockShape,
                    null, null);
                results.add(result_count);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          });
        }
      }, stockShape);

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
      System.out.println("Time for "+count+" jobs is "+(t2-t1)+" millis");
      System.out.println("Result size: "+results);
    } else {
      long resultCount = 
          rangeQueryMapReduce(fs, inputFile, queryRange, stockShape, null, null);
      System.out.println("Result size: "+resultCount);
    }
    
  }
}
