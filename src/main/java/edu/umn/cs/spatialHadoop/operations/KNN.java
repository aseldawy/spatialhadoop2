/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.PriorityQueue;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Circle;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.indexing.RTree;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;

/**
 * Performs k Nearest Neighbor (kNN) query over a spatial file.
 * @author Ahmed Eldawy
 *
 */
public class KNN {
  /**Logger for KNN*/
  private static final Log LOG = LogFactory.getLog(KNN.class);

  /**Statistics for debugging. Total number of iterations by all KNN queries*/
  private static AtomicInteger TotalIterations = new AtomicInteger();

  /**
   * Stores a shape text along with its distance to the query point. Notice that
   * it cannot be a ShapeWithDistance because we cannot easily deserialize it
   * unless we know the right class of the shape.
   * @author Ahmed Eldawy
   *
   */
  public static class TextWithDistance implements Writable, Cloneable, TextSerializable, Comparable<TextWithDistance> {
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
      if (obj == null)
        return false;
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

    @Override
    public int compareTo(TextWithDistance o) {
      return this.distance < o.distance ? -1 : (this.distance > o.distance ? +1 : 0);
    }
  }
  
  /**Stores a shape along with its distance from the query point*/
  static class ShapeWithDistance <S extends Shape> implements Comparable<ShapeWithDistance<S>> {
    public S shape;
    public double distance;
    
    public ShapeWithDistance() {
    }
    
    public ShapeWithDistance(S s, double d) {
      this.shape = s;
      this.distance = d;
    }
  
    @Override
    public int compareTo(ShapeWithDistance<S> o) {
      return this.distance < o.distance? -1 : (this.distance > o.distance? +1 : 0);
    }
    
    @Override
    public String toString() {
      return shape.toString() + " @"+distance;
    }
  }

  /**
   * Keeps KNN objects ordered by their distance descending
   * @author Ahmed Eldawy
   *
   */
  public static class KNNObjects<S extends Comparable<S>> extends PriorityQueue<S> {
    /**
     * A hashset of all elements currently in the heap. Used to avoid inserting
     * the same object twice.
     */
    Set<S> allElements = new HashSet<S>();
    /**Capacity of the queue*/
    private int capacity;
    
    public KNNObjects(int k) {
      this.capacity = k;
      super.initialize(k);
    }
    
    /**
     * Keep elements sorted in descending order (Max heap)
     */
    @Override
    protected boolean lessThan(Object a, Object b) {
      return ((S)a).compareTo((S)b) > 0;
    }
    
    @Override
    public boolean insert(S newElement) {
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
   * Mapper for KNN MapReduce. Calculates the distance between a shape and 
   * the query point.
   * @author eldawy
   *
   */
  public static class KNNMap<S extends Shape> extends
    Mapper<Rectangle, Iterable<Shape>, NullWritable, TextWithDistance> {
    /**A temporary object to be used for output*/
    private final TextWithDistance outputValue = new TextWithDistance();
    
    /**User query*/
    private Point queryPoint;
    private int k;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      Configuration conf = context.getConfiguration();
      queryPoint = (Point) OperationsParams.getShape(conf, "point");
      k = conf.getInt("k", 1);
    }
    
    @Override
    protected void map(Rectangle key, Iterable<Shape> shapes, final Context context)
        throws IOException, InterruptedException {
      final NullWritable dummy = NullWritable.get();
      if (shapes instanceof RTree) {
        ((RTree<S>)shapes).knn(queryPoint.x, queryPoint.y, k, new ResultCollector2<S, Double>() {
          @Override
          public void collect(S shape, Double distance) {
            try {
              outputValue.distance = distance;
              outputValue.text.clear();
              shape.toText(outputValue.text);
              context.write(dummy, outputValue);
            } catch (IOException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
      } else {
        for (Shape shape : shapes) {
          outputValue.distance = shape.distanceTo(queryPoint.x, queryPoint.y);
          outputValue.text.clear();
          shape.toText(outputValue.text);
          context.write(dummy, outputValue);
        }
      }
    }
  }
  
  /**
   * Reduce (and combine) class for KNN MapReduce. Given a list of shapes,
   * choose the k with least distances.
   * @author eldawy
   *
   */
  public static class KNNReduce<S extends Shape> extends
      Reducer<NullWritable, TextWithDistance, NullWritable, TextWithDistance> {
    /**User query*/
    private Point queryPoint;
    private int k;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      Configuration conf = context.getConfiguration();
      queryPoint = (Point) OperationsParams.getShape(conf, "point");
      k = conf.getInt("k", 1);
    }
    
    @Override
    protected void reduce(NullWritable dummy,
        Iterable<TextWithDistance> values, Context context) throws IOException,
        InterruptedException {
      if (k == 0)
        return;
      PriorityQueue<TextWithDistance> knn = new KNNObjects<TextWithDistance>(k);
      for (TextWithDistance t : values) {
        knn.insert(t.clone());
      }
      
      TextWithDistance[] knnAscendingOrder = new TextWithDistance[knn.size()];
      int i = knnAscendingOrder.length;
      while (knn.size() > 0) {
        TextWithDistance t = knn.pop();
        knnAscendingOrder[--i] = t;
      }
      // Write results in the ascending order
      for (TextWithDistance t : knnAscendingOrder)
        context.write(dummy, t);
    }
  }
  
  /**
   * A MapReduce version of KNN query.
   * @param fs
   * @param inputPath
   * @param queryPoint
   * @param shape
   * @param output
   * @return
   * @throws IOException
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  private static <S extends Shape> Job knnMapReduce(Path inputPath,
      Path userOutputPath, OperationsParams params) throws IOException,
      ClassNotFoundException, InterruptedException {
    Job job = new Job(params, "KNN");
    job.setJarByClass(KNN.class);
    
    FileSystem inFs = inputPath.getFileSystem(params);
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inputPath);
    
    job.setMapperClass(KNNMap.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(TextWithDistance.class);

    job.setReducerClass(KNNReduce.class);
    job.setNumReduceTasks(1);
    
    job.getConfiguration().setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    final Point queryPoint = (Point) params.getShape("point");
    final int k = params.getInt("k", 1);
    
    final IntWritable additional_blocks_2b_processed = new IntWritable(0);
    long resultCount;
    int iterations = 0;
    
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path(inputPath.getName()+
            ".knn_"+(int)(Math.random() * 1000000));
      } while (inFs.exists(outputPath));
    }
    job.setOutputFormatClass(TextOutputFormat3.class);
    TextOutputFormat3.setOutputPath(job, outputPath);
    
    GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, inputPath);
    Configuration templateConf = job.getConfiguration();

    FileSystem outFs = outputPath.getFileSystem(params);
    // Start with the query point to select all partitions overlapping with it
    Shape range_for_this_iteration = new Point(queryPoint.x, queryPoint.y);
    
    do {
      job = new Job(templateConf);
      // Delete results of last iteration if not first iteration
      if (outputPath != null)
        outFs.delete(outputPath, true);
        
      LOG.info("Running iteration: "+(++iterations));
      // Set query range for the SpatialInputFormat
      OperationsParams.setShape(job.getConfiguration(), RangeFilter.QueryRange,
          range_for_this_iteration);

      // Submit the job
      if (params.getBoolean("background", false)) {
        // XXX this is incorrect because if the job needs multiple iterations,
        // it will run only the first one
        job.waitForCompletion(false);
        return job;
      }
      job.waitForCompletion(false);

      // Retrieve answers for this iteration
      Counters counters = job.getCounters();
      Counter resultSizeCounter = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
      resultCount = resultSizeCounter.getValue();
      
      if (globalIndex != null) {
        Circle range_for_next_iteration;
        if (resultCount < k) {
          LOG.info("Found only "+resultCount+" results");
          // Did not find enough results in the query space
          // Increase the distance by doubling the maximum distance among all
          // partitions that were processed
          final DoubleWritable maximum_distance = new DoubleWritable(0);
          int matched_partitions = globalIndex.rangeQuery(range_for_this_iteration, new ResultCollector<Partition>() {
            @Override
            public void collect(Partition p) {
              double distance =
                  p.getMaxDistanceTo(queryPoint.x, queryPoint.y);
              if (distance > maximum_distance.get())
                maximum_distance.set(distance);
            }
          });
          if (matched_partitions == 0) {
            // The query point is outside the search space
            // Set the range to include the closest partition
            globalIndex.knn(queryPoint.x, queryPoint.y, 1, new ResultCollector2<Partition, Double>() {
              @Override
              public void collect(Partition r, Double s) {
                maximum_distance.set(s);
              }
            });
          }
          range_for_next_iteration =
              new Circle(queryPoint.x, queryPoint.y, maximum_distance.get()*2);
          LOG.info("Expanding to "+maximum_distance.get()*2);
        } else {
          // Calculate the new test range which is a circle centered at the
          // query point and distance to the k^{th} neighbor
          
          // Get distance to the kth neighbor
          final DoubleWritable distance_to_kth_neighbor = new DoubleWritable();
          FileStatus[] results = outFs.listStatus(outputPath);
          for (FileStatus result_file : results) {
            if (result_file.getLen() > 0 && result_file.getPath().getName().startsWith("part-")) {
              // Read the last line (kth neighbor)
              Tail.tail(outFs, result_file.getPath(), 1, new TextWithDistance(), new ResultCollector<TextWithDistance>() {

                @Override
                public void collect(TextWithDistance r) {
                  distance_to_kth_neighbor.set(r.distance);
                }
              });
            }
          }
          range_for_next_iteration = new Circle(queryPoint.x, queryPoint.y,
              distance_to_kth_neighbor.get());
          LOG.info("Expanding to kth neighbor: "+distance_to_kth_neighbor);
        }
        
        // Calculate the number of blocks to be processed to check the
        // terminating condition;
        additional_blocks_2b_processed.set(0);
        final Shape temp = range_for_this_iteration;
        globalIndex.rangeQuery(range_for_next_iteration, new ResultCollector<Partition>() {
          @Override
          public void collect(Partition p) {
            if (!(p.isIntersected(temp))) {
              additional_blocks_2b_processed.set(additional_blocks_2b_processed.get() + 1);
            }
          }
        });
        range_for_this_iteration = range_for_next_iteration;
      }
    } while (additional_blocks_2b_processed.get() > 0);
    
    // If output file is not set by user, delete it
    if (userOutputPath == null)
      outFs.delete(outputPath, true);
    TotalIterations.addAndGet(iterations);
    
    return job;
  }
  
  private static<S extends Shape> long knnLocal(Path inFile, Path outPath,
      OperationsParams params) throws IOException, InterruptedException {
    int iterations = 0;
    FileSystem fs = inFile.getFileSystem(params);
    Point queryPoint = (Point) OperationsParams.getShape(params, "point");
    int k = params.getInt("k", 1);
    // Top-k objects are retained in this object
    PriorityQueue<ShapeWithDistance<S>> knn = new KNNObjects<ShapeWithDistance<S>>(k);

    SpatialInputFormat3<Rectangle, Shape> inputFormat =
        new SpatialInputFormat3<Rectangle, Shape>();
    
    final GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(fs, inFile);
    double kthDistance = Double.MAX_VALUE;
    if (gIndex != null) {
      // There is a global index, use it
      PriorityQueue<ShapeWithDistance<Partition>> partitionsToProcess =
          new PriorityQueue<KNN.ShapeWithDistance<Partition>>() {
        {
          initialize(gIndex.size());
        }
        
        @Override
        protected boolean lessThan(Object a, Object b) {
          return ((ShapeWithDistance<Partition>)a).distance <
              ((ShapeWithDistance<Partition>)b).distance;
        }
      };
      for (Partition p : gIndex) {
        double distance = p.getMinDistanceTo(queryPoint.x, queryPoint.y);
        partitionsToProcess.insert(new ShapeWithDistance<Partition>(p.clone(), distance));
      }
      
      while (partitionsToProcess.size() > 0 &&
          partitionsToProcess.top().distance <= kthDistance) {
        
        ShapeWithDistance<Partition> partitionToProcess = partitionsToProcess.pop();
        // Process this partition
        Path partitionPath = new Path(inFile, partitionToProcess.shape.filename);
        long length = fs.getFileStatus(partitionPath).getLen();
        FileSplit fsplit = new FileSplit(partitionPath, 0, length, new String[0]);
        RecordReader<Rectangle, Iterable<Shape>> reader =
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
        iterations++;
        
        while (reader.nextKeyValue()) {
          Iterable<Shape> shapes = reader.getCurrentValue();
          for (Shape shape : shapes) {
            double distance = shape.distanceTo(queryPoint.x, queryPoint.y);
            if (distance <= kthDistance)
              knn.insert(new ShapeWithDistance<S>((S)shape.clone(), distance));
          }
        }
        reader.close();
        
        if (knn.size() >= k)
          kthDistance = knn.top().distance;
      }
    } else {
      // No global index, have to scan the whole file
      Job job = new Job(params);
      SpatialInputFormat3.addInputPath(job, inFile);
      List<InputSplit> splits = inputFormat.getSplits(job);
      
      for (InputSplit split : splits) {
        RecordReader<Rectangle, Iterable<Shape>> reader =
            inputFormat.createRecordReader(split, null);
        if (reader instanceof SpatialRecordReader3) {
          ((SpatialRecordReader3)reader).initialize(split, params);
        } else if (reader instanceof RTreeRecordReader3) {
          ((RTreeRecordReader3)reader).initialize(split, params);
        } else if (reader instanceof HDFRecordReader) {
          ((HDFRecordReader)reader).initialize(split, params);
        } else {
          throw new RuntimeException("Unknown record reader");
        }
        iterations++;
        
        while (reader.nextKeyValue()) {
          Iterable<Shape> shapes = reader.getCurrentValue();
          for (Shape shape : shapes) {
            double distance = shape.distanceTo(queryPoint.x, queryPoint.y);
            knn.insert(new ShapeWithDistance<S>((S)shape.clone(), distance));
          }
        }
        
        reader.close();
      }
      if (knn.size() >= k)
        kthDistance = knn.top().distance;
    }
    long resultCount = knn.size();
    if (outPath != null && params.getBoolean("output", true)) {
      FileSystem outFS = outPath.getFileSystem(params);
      PrintStream ps = new PrintStream(outFS.create(outPath));
      Vector<ShapeWithDistance<S>> resultsOrdered =
          new Vector<ShapeWithDistance<S>>((int) resultCount);
      resultsOrdered.setSize((int) resultCount);
      while (knn.size() > 0) {
        ShapeWithDistance<S> nextAnswer = knn.pop();
        resultsOrdered.set(knn.size(), nextAnswer);
      }
      
      Text text = new Text();
      for (ShapeWithDistance<S> answer : resultsOrdered) {
        text.clear();
        TextSerializerHelper.serializeDouble(answer.distance, text, ',');
        answer.shape.toText(text);
        ps.println(text);
      }
      ps.close();
    }
    TotalIterations.addAndGet(iterations);
    return resultCount;
    
  }
  
  public static Job knn(Path inFile, Path outFile, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    if (params.getBoolean("local", true)) {
      // Process without MapReduce. The default for KNN regardless of input size
      knnLocal(inFile, outFile, params);
      return null;
    } else {
      // Process with MapReduce
      return knnMapReduce(inFile, outFile, params);
    }
  }
  
  private static void printUsage() {
    System.out.println("Performs a KNN query on an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - Path to output file");
    System.out.println("k:<k> - (*) Number of neighbors to file");
    System.out.println("point:<x,y> - (*) Coordinates of the query point");
    System.out.println("-overwrite - Overwrite output file without notice");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  public static void main(String[] args) throws IOException {
    final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] paths = params.getPaths();
    if (paths.length <= 1 && !params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    if (paths.length > 1 && !params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    final Path inputFile = params.getInputPath();
    int count = params.getInt("count", 1);
    double closeness = params.getFloat("closeness", -1.0f);
    final Point[] queryPoints = closeness < 0 ? params.getShapes("point", new Point()) : new Point[count];
    final FileSystem fs = inputFile.getFileSystem(params);
    final int k = params.getInt("k", 1);
    int concurrency = params.getInt("concurrency", 100);
    if (k == 0) {
      LOG.warn("k = 0");
    }

    if (queryPoints.length == 0) {
      printUsage();
      throw new RuntimeException("Illegal arguments");
    }
    final Path outputPath = paths.length > 1 ? paths[1] : null;

    if (closeness >= 0) {
      // Get query points according to its closeness to grid intersections
      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, inputFile);
      long seed = params.getLong("seed", System.currentTimeMillis());
      Random random = new Random(seed);
      for (int i = 0; i < count; i++) {
        int i_block = random.nextInt(gindex.size());
        int direction = random.nextInt(4);
        // Generate a point in the given direction
        // Get center point (x, y)
        Iterator<Partition> iterator = gindex.iterator();
        while (i_block-- >= 0)
          iterator.next();
        Partition partition = iterator.next();
        double cx = (partition.x1 + partition.x2) / 2;
        double cy = (partition.y1 + partition.y2) / 2;
        double cw = partition.x2 - partition.x1;
        double ch = partition.y2 - partition.y1;
        int signx = ((direction & 1) == 0)? 1 : -1;
        int signy = ((direction & 2) == 1)? 1 : -1;
        double x = cx + cw * closeness / 2 * signx;
        double y = cy + ch * closeness / 2 * signy;
        queryPoints[i] = new Point(x, y);
      }
    }

    final BooleanWritable exceptionHappened = new BooleanWritable();
    
    Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        ex.printStackTrace();
        exceptionHappened.set(true);
      }
    };

    // Run each query in a separate thread
    final Vector<Thread> threads = new Vector<Thread>();
    for (int i = 0; i < queryPoints.length; i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            Point query_point = queryPoints[threads.indexOf(this)];
            OperationsParams newParams = new OperationsParams(params);
            OperationsParams.setShape(newParams, "point", query_point);
            Job job = knn(inputFile, outputPath, params);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (ClassNotFoundException e) {
            e.printStackTrace();
          }
        }
      };
      thread.setUncaughtExceptionHandler(h);
      threads.add(thread);
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

    System.out.println("Time for " + queryPoints.length + " jobs is "
        + (t2 - t1) + " millis");
    System.out.println("Total iterations: " + TotalIterations);
  }
}