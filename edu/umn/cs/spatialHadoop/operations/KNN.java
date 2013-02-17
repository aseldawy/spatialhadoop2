package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
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
import org.apache.hadoop.spatial.Circle;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.ResultCollector;
import org.apache.hadoop.spatial.ResultCollector2;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.PriorityQueue;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

/**
 * Performs k Nearest Neighbor (kNN) query over a spatial file.
 * @author eldawy
 *
 */
public class KNN {
  /**Logger for KNN*/
  private static final Log LOG = LogFactory.getLog(KNN.class);

  /**Statistics for debugging. Total number of iterations by all KNN queries*/
  private static AtomicInteger TotalIterations = new AtomicInteger();
  
  /**Configuration line name for query point*/
  public static final String QUERY_POINT =
      "edu.umn.cs.spatialHadoop.operations.KNN.QueryPoint";

  public static final String K = "edu.umn.cs.spatialHadoop.operations.KNN.K";

  public static class TextWithDistance implements Writable, Cloneable, TextSerializable {
    public double distance; 
    public Text text = new Text();
    
    public TextWithDistance() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeDouble(distance);
      text.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      distance = in.readDouble();
      text.readFields(in);
    }
    
    @Override
    public Text toText(Text t) {
      TextSerializerHelper.serializeDouble(distance, t, ',');
      t.append(text.getBytes(), 0, text.getLength());
      return t;
    }
    
    @Override
    public int hashCode() {
      return this.text.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
      return this.text.equals(((TextWithDistance)obj).text);
    }
    
    @Override
    public void fromText(Text t) {
      distance = TextSerializerHelper.consumeDouble(t, ',');
      text.set(t);
    }
    
    @Override
    public String toString() {
      return distance+","+text;
    }
    
    @Override
    protected TextWithDistance clone() {
      TextWithDistance c = new TextWithDistance();
      c.distance = this.distance;
      c.text.set(this.text);
      return c;
    }
  }
  
  /**
   * Mapper for KNN MapReduce. Calculates the distance between a shape and 
   * the query point.
   * @author eldawy
   *
   */
  public static class KNNMap<S extends Shape> extends MapReduceBase {
    private static final NullWritable Dummy = NullWritable.get();
    /**A temporary object to be used for output*/
    private final TextWithDistance outputValue = new TextWithDistance();
    
    /**User query*/
    private Point queryPoint;
    private int k;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      queryPoint = new Point();
      queryPoint.fromText(new Text(job.get(QUERY_POINT)));
      k = job.getInt(K, 1);
    }

    /**
     * Map for non-indexed (heap) blocks
     * @param id
     * @param shape
     * @param output
     * @param reporter
     * @throws IOException
     */
    public void map(CellInfo cell, S shape,
        OutputCollector<NullWritable, TextWithDistance> output,
        Reporter reporter) throws IOException {
      outputValue.distance = (long)shape.distanceTo(queryPoint.x, queryPoint.y);
      outputValue.text.clear();
      shape.toText(outputValue.text);
      output.collect(Dummy, outputValue);
    }

    /**
     * Map for RTree indexed blocks
     * @param id
     * @param shapes
     * @param output
     * @param reporter
     * @throws IOException
     */
    public void map(CellInfo cellInfo, RTree<S> shapes,
        final OutputCollector<NullWritable, TextWithDistance> output,
        Reporter reporter) throws IOException {
      shapes.knn(queryPoint.x, queryPoint.y, k, new ResultCollector2<S, Long>() {
        @Override
        public void collect(S shape, Long distance) {
          try {
            outputValue.distance = distance;
            outputValue.text.clear();
            shape.toText(outputValue.text);
            output.collect(Dummy, outputValue);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }
  
  public static class Map1<S extends Shape> extends KNNMap<S>
    implements Mapper<CellInfo, S, NullWritable, TextWithDistance> {}

  public static class Map2<S extends Shape> extends KNNMap<S>
    implements Mapper<CellInfo, RTree<S>, NullWritable, TextWithDistance> {}

  /**
   * Keeps KNN objects ordered by their distance descending
   * @author eldawy
   *
   */
  public static class KNNObjects extends PriorityQueue<TextWithDistance> {
    /**
     * A hashset of all elements currently in the heap. Used to avoid inserting
     * the same object twice.
     */
    Set<TextWithDistance> allElements = new HashSet<TextWithDistance>();
    /**Capacity of the queue*/
    private int capacity;
    
    public KNNObjects(int k) {
      this.capacity = k;
      super.initialize(k);
    }
    
    /**
     * Keep elements sorted by distance in descending order (Max heap)
     */
    @Override
    protected boolean lessThan(Object a, Object b) {
      return ((TextWithDistance)a).distance >= ((TextWithDistance)b).distance;
    }
    
    @Override
    public boolean insert(TextWithDistance newElement) {
      // Skip element if already there
      if (allElements.contains(newElement))
        return false;
      boolean overflow = this.size() == capacity;
      Object overflowItem = this.top();
      boolean inserted = super.insert(newElement);
      if (inserted) {
        if (overflow)
          allElements.remove(overflowItem);
        allElements.add(newElement);
      }
      return inserted;
    }
  }
  
  /**
   * Reduce (and combine) class for KNN MapReduce. Given a list of shapes,
   * choose the k with least distances.
   * @author eldawy
   *
   */
  public static class KNNReduce<S extends Shape> extends MapReduceBase implements
  Reducer<NullWritable, TextWithDistance, NullWritable, TextWithDistance> {
    /**User query*/
    private Point queryPoint;
    private int k;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      queryPoint = new Point();
      queryPoint.fromText(new Text(job.get(QUERY_POINT)));
      k = job.getInt(K, 1);
    }

    @Override
    public void reduce(NullWritable dummy, Iterator<TextWithDistance> values,
        OutputCollector<NullWritable, TextWithDistance> output, Reporter reporter)
            throws IOException {
      if (k == 0)
        return;
      PriorityQueue<TextWithDistance> knn = new KNNObjects(k);
      while (values.hasNext()) {
        TextWithDistance t = values.next();
        knn.insert(t.clone());
      }
      
      while (knn.size() > 0) {
        TextWithDistance t = knn.pop();
        output.collect(dummy, t);
      }
    }
  }
  
  /**
   * A MapReduce version of KNN query.
   * @param fs
   * @param outputFile
   * @param queryPoint
   * @param shape
   * @param output
   * @return
   * @throws IOException
   */
  public static <S extends Shape> long knnMapReduce(FileSystem fs,
      Path outputFile, Path userOutputPath, Point queryPoint, int k, S shape,
      OutputCollector<Double, S> output, boolean overwrite)
          throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
    FileSystem outFs = outputFile.getFileSystem(job);
    
    job.setJobName("KNN");
    
    FSDataInputStream in = fs.open(outputFile);
    if (in.readLong() == SpatialSite.RTreeFileMarker) {
      LOG.info("Performing KNN on RTree blocks");
      job.setMapperClass(Map2.class);
      job.setInputFormat(RTreeInputFormat.class);
    } else {
      LOG.info("Performing KNN on heap blocks");
      job.setMapperClass(Map1.class);
      // Combiner is needed for heap blocks
      job.setCombinerClass(KNNReduce.class);
      job.setInputFormat(ShapeInputFormat.class);
    }
    in.close();
    
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(TextWithDistance.class);
    job.set(QUERY_POINT, queryPoint.toText(new Text()).toString());
    job.setInt(K, k);
    
    job.setReducerClass(KNNReduce.class);
    job.setNumReduceTasks(1);
    
    job.set(SpatialSite.SHAPE_CLASS, shape.getClass().getName());
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, outputFile);

    RunningJob runningJob;
    FileStatus fileStatus = fs.getFileStatus(outputFile);
    BlockLocation[] fileBlockLocations =
        fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

    job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    
    // TextWithDistance used to read output results
    final TextWithDistance t = new TextWithDistance();
    
    Shape range_for_this_iteration = new Point(queryPoint.x, queryPoint.y);
    int additional_blocks_2b_processed;
    long resultCount;
    int iterations = 0;
    
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path("/"+outputFile.getName()+
            ".knn_"+(int)(Math.random() * 1000000));
      } while (outFs.exists(outputPath));
    } else {
      if (outFs.exists(outputPath)) {
        if (overwrite)
          outFs.delete(outputPath, true);
        else
          throw new RuntimeException("Output path already exists and -overwrite flag is not set");
      }
    }

    do {
      // Delete results of last iteration if not first iteration
      if (outputPath != null)
        outFs.delete(outputPath, true);
        
      TextOutputFormat.setOutputPath(job, outputPath);
      
      LOG.info("Running iteration: "+(++iterations));
      RangeFilter.setQueryRange(job, range_for_this_iteration);

      // Submit the job
      runningJob = JobClient.runJob(job);

      // Retrieve answers for this iteration
      Counters counters = runningJob.getCounters();
      Counter outputRecordCounter = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
      resultCount = outputRecordCounter.getValue();
      
      Circle range_for_next_iteration;
      if (resultCount < k) {
        LOG.info("Found only "+resultCount+" results");
        // Did not find enough results in the query space
        // Increase the distance by doubling the maximum distance
        double maximum_distance = 0;
        for (BlockLocation l : fileBlockLocations) {
          if (l.getCellInfo().isIntersected(range_for_this_iteration)) {
            double distance =
                l.getCellInfo().getMaxDistanceTo(queryPoint.x, queryPoint.y);
            if (distance > maximum_distance)
              maximum_distance = distance;
          }
        }
        range_for_next_iteration =
            new Circle(queryPoint.x, queryPoint.y, maximum_distance*2);
        LOG.info("Expanding to "+maximum_distance*2);
      } else {
        // Calculate the new test range which is a circle centered at the
        // query point and distance to the k^{th} neighbor

        // Get distance to the kth neighbor
        final DoubleWritable distance_to_kth_neighbor = new DoubleWritable();
        FileStatus[] results = outFs.listStatus(outputPath);
        for (FileStatus result_file : results) {
          if (result_file.getLen() > 0 && result_file.getPath().getName().startsWith("part-")) {
            in = outFs.open(result_file.getPath());
            LineReader reader = new LineReader(in);
            Text first_line = new Text();
            reader.readLine(first_line);
            t.fromText(first_line);
            distance_to_kth_neighbor.set(t.distance);
            in.close();
          }
        }
        range_for_next_iteration = new Circle(queryPoint.x, queryPoint.y,
            distance_to_kth_neighbor.get());
        LOG.info("Expanding to kth neighbor: "+distance_to_kth_neighbor);
      }
      
      // Calculate the number of blocks to be processed to check the
      // terminating condition;
      additional_blocks_2b_processed = 0;
      for (BlockLocation l : fileBlockLocations) {
        if (l.getCellInfo() != null &&
            l.getCellInfo().isIntersected(range_for_next_iteration) &&
            !(l.getCellInfo().isIntersected(range_for_this_iteration))) {
          additional_blocks_2b_processed++;
        }
      }
      range_for_this_iteration = range_for_next_iteration;
    } while (additional_blocks_2b_processed > 0);
    
    // Read job result
    if (output != null) {
      FileStatus[] results = outFs.listStatus(outputPath);
      for (FileStatus result_file : results) {
        if (result_file.getLen() > 0 && result_file.getPath().getName().startsWith("part-")) {
          // Report every single result as a pair of shape with distance
          LineReader lineReader = new LineReader(outFs.open(result_file.getPath()));
          Text line = new Text();
          line.clear();
          while (lineReader.readLine(line) > 0) {
            t.fromText(line);
            shape.fromText(t.text);
            output.collect(t.distance, shape);
          }
          lineReader.close();
        }
      }
    }

    // If output file is not set by user, delete it
    if (userOutputPath == null)
      outFs.delete(outputPath, true);
    TotalIterations.addAndGet(iterations);
    
    return resultCount;
  }
  
  public static<S extends Shape> long knnLocal(FileSystem fs, Path file,
      Point queryPoint, int k, S shape,
      OutputCollector<Double, Shape> output)
      throws IOException {
    long file_size = fs.getFileStatus(file).getLen();
    ShapeRecordReader<S> shapeReader =
        new ShapeRecordReader<S>(fs.open(file), 0, file_size);

    CellInfo key = shapeReader.createKey();
    
    TextWithDistance[] knn = new TextWithDistance[k];

    while (shapeReader.next(key, shape)) {
      double distance = shape.distanceTo(queryPoint.x, queryPoint.y);
      int i = k - 1;
      while (i >= 0 && (knn[i] == null || knn[i].distance > distance)) {
        i--;
      }
      i++;
      if (i < k) {
        if (knn[i] != null) {
          for (int j = k - 1; j > i; j--)
            knn[j] = knn[j-1];
        }
        
        knn[i] = new TextWithDistance();
        shape.toText(knn[i].text);
        knn[i].distance = distance;
      }
    }
    shapeReader.close();
    
    long resultCount = 0;
    for (int i = 0; i < knn.length; i++) {
      if (knn[i] != null) {
        if (output != null) {
          shape.fromText(knn[i].text);
          output.collect(knn[i].distance, shape);
        }
        resultCount++;
      }
    }
    return resultCount;
  }
  
  private static void printUsage() {
    System.out.println("Performs a KNN query on an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - Path to output file");
    System.out.println("k:<k> - (*) Number of neighbors to file");
    System.out.println("point:<x,y> - (*) Coordinates of the query point");
    System.out.println("-overwrite - Overwrite output file without notice");
  }

  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] paths = cla.getPaths();
    Configuration conf = new Configuration();
    final Path inputFile = paths[0];
    Point queryPoint = cla.getPoint();
    final FileSystem fs = inputFile.getFileSystem(conf);
    final int k = cla.getK();
    int count = cla.getCount();
    int concurrency = cla.getConcurrency();
    final Shape shape = cla.getShape(true);
    final double closenessFactor = cla.getClosenessFactor();
    long seed = cla.getSeed();
    if (k == 0) {
      LOG.warn("k = 0");
    }
    final boolean overwrite = cla.isOverwrite();
    
    if (paths.length == 1 || (queryPoint == null && count == 0)) {
      printUsage();
      throw new RuntimeException("Illegal arguments");
    }
    final Path outputPath = paths[1];
    
    final Vector<Long> results = new Vector<Long>();
    
    if (queryPoint != null) {
      // User provided a query, use it
      long resultCount = 
          knnMapReduce(fs, inputFile, outputPath, queryPoint, k, shape, null, overwrite);
      System.out.println("Result size: "+resultCount);
    } else {
      // Generate query at random points
      final Vector<Thread> threads = new Vector<Thread>();
      final Vector<Point> query_points = new Vector<Point>();
      if (closenessFactor == -1.0) {
        // Get query points from file
        Sampler.sampleLocal(fs, inputFile, count, seed, new ResultCollector<Shape>(){
          @Override
          public void collect(final Shape value) {
            query_points.add(new Point(value.getMBR().x, value.getMBR().y));
          }
        }, shape);
      } else {
        // Get query points according to its closeness to grid intersections
        FileStatus fileStatus = fs.getFileStatus(inputFile);
        BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        Random random = new Random(seed);
        for (int i = 0; i < count; i++) {
          int i_block = random.nextInt(blockLocations.length);
          int direction = random.nextInt(4);
          // Get center point (x, y)
          long cx = blockLocations[i_block].getCellInfo().getXMid();
          long cy = blockLocations[i_block].getCellInfo().getYMid();
          long cw = blockLocations[i_block].getCellInfo().width;
          long ch = blockLocations[i_block].getCellInfo().height;
          int signx = ((direction & 1) == 0)? 1 : -1;
          int signy = ((direction & 2) == 1)? 1 : -1;
          long x = (long) (cx + cw * closenessFactor / 2 * signx);
          long y = (long) (cy + ch * closenessFactor / 2 * signy);
          query_points.add(new Point(x, y));
        }
      }

      for (int i = 0; i < query_points.size(); i++) {
        threads.add(new Thread() {
          @Override
          public void run() {
            try {
              Point query_point =
                  query_points.elementAt(threads.indexOf(this));
              long result_count = knnMapReduce(fs, inputFile, outputPath,
                  query_point, k, shape, null, overwrite);
              results.add(result_count);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
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
      System.out.println("Time for "+count+" jobs is "+(t2-t1)+" millis");
      System.out.println("Result size: "+results);
    }
    System.out.println("Total iterations: "+TotalIterations);
  }
}