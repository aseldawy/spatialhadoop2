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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.MemoryReporter;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

/**
 * Computes the farthest pair for a set of points
 * @author Ahmed Eldawy
 *
 */
public class FarthestPair {
  
  private static final Log LOG = LogFactory.getLog(FarthestPair.class);
  
  private static final String FarthestPairLowerBound = "FarthestPair.FarthestPairLowerBound";
  
  public static enum FarthestPairCounters {FP_ProcessedPairs};
  
  public static double cross(Point o, Point a, Point b) {
    return (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);   
  }
  
  public static class PairDistance extends PairWritable<Point> {
    public double distance;
    
    public PairDistance() {
      this.first = new Point();
      this.second = new Point();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeDouble(distance);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      distance = in.readDouble();
    }
    
    @Override
    public String toString() {
      return this.first+","+this.second+","+distance;
    }
  }
  
  public static PairDistance rotatingCallipers(Point[] a) {
    PairDistance farthest_pair = new PairDistance();
    int i, j = 1, j_plus_one = 2 % a.length;
    for (i = 0; i < a.length; i++) {
      int i_plus_one = (i + 1) % a.length;
      while(cross(a[i], a[i_plus_one], a[j_plus_one]) > cross(a[i], a[i_plus_one], a[j])) {
        j = j_plus_one;
        j_plus_one = (j + 1) % a.length;
      }
      double dist = a[i].distanceTo(a[j]);
      if (dist > farthest_pair.distance) {
        farthest_pair.distance = dist;
        farthest_pair.first.set(a[i].x, a[i].y);
        farthest_pair.second.set(a[j].x, a[j].y);
      }

      dist = a[i_plus_one].distanceTo(a[j]);
      if (dist > farthest_pair.distance) {
        farthest_pair.distance = dist;
        farthest_pair.first.set(a[i_plus_one].x, a[i_plus_one].y);
        farthest_pair.second.set(a[j].x, a[j].y);
      }
    }
    return farthest_pair;
  }
  
  /**
   * Computes an upper bound of the farthest pair of all possible points
   * that could be in two partitions.
   * @param mbr1
   * @param mbr2
   * @return
   */
  private static double computeUB(Rectangle mbr1, Rectangle mbr2) {
    // Since the farthest distance has to be on the convex hull, we compute
    // all pair-wise distances between the corner points on the two rectangles
    // and take the maximum out of them
    double[] xs = {mbr1.x1, mbr1.x1, mbr1.x2, mbr1.x2,
        mbr2.x1, mbr2.x1, mbr2.x2, mbr2.x2};
    double[] ys = {mbr1.y1, mbr1.y2, mbr1.y1, mbr1.y2,
        mbr2.y1, mbr2.y2, mbr2.y1, mbr2.y2};
    double maxd2 = 0;
    for (int i1 = 0; i1 < 4; i1++) {
      for (int i2 = 4; i2 < 8; i2++) {
        double dx = xs[i1] - xs[i2];
        double dy = ys[i1] - ys[i2];
        double d2 = dx * dx + dy * dy;
        if (d2 > maxd2)
          maxd2 = d2;
      }
    }
    return Math.sqrt(maxd2);
  }

  /**
   * Computes a lower bound of the maximum distance between two points in
   * the two given rectangles. It is assumed that both rectangles
   * are minimal bounding rectangles (MBRs), that is, there has to be at least
   * one point on each of the four edges of each rectangle.
   * @param mbr1
   * @param mbr2
   * @return
   */
  private static double computeLB(Rectangle mbr1, Rectangle mbr2) {
    double distance;
    double lb = 0;
    // Closest possible points on the left and right edges of mbr1
    distance = mbr1.x2 - mbr1.x1;
    if (distance > lb) lb = distance;
    // Same for mbr2
    distance = mbr2.x2 - mbr2.x1;
    if (distance > lb) lb = distance;
    // Closest possible points on the top and bottom edges of mbr1
    distance = mbr1.y2 - mbr1.y1;
    if (distance > lb) lb = distance;
    // Same for mbr2
    distance = mbr2.y2 - mbr2.y1;
    if (distance > lb) lb = distance;

    // Calculate the minimum possible distance between a point on the left
    // edge of one MBR and the right edge of the other MBR
    double dx, dy;
    dx = Math.max(Math.abs(mbr2.x2 - mbr1.x1), Math.abs(mbr1.x2 - mbr2.x1));
    if (mbr1.y2 > mbr2.y1 && mbr2.y2 > mbr1.y1) {
      // The MBRs overlap in the y-coordinate
      dy = 0;
    } else {
      // Non-overlapping, compute the minimum possible vertical distance
      // as the worse case scenario
      dy = Math.min(Math.abs(mbr2.y1 - mbr1.y2), Math.abs(mbr1.y1 - mbr2.y2));
    }
    distance = Math.sqrt(dx * dx + dy * dy);
    if (distance > lb) lb = distance;
    
    // Calculate the minimum possible distance between a point on the top
    // edge of one MBR and the bottom edge of the other MBR
    dy = Math.max(Math.abs(mbr2.y2 - mbr1.y1), Math.abs(mbr1.y2 - mbr2.y1));
    if (mbr1.x2 > mbr2.x1 && mbr2.x2 > mbr1.x1) {
      // The MBRs overlap in the x-coordinate
      dx = 0;
    } else {
      // Non-overlapping, compute the minimum possible vertical distance
      // as the worse case scenario
      dx = Math.min(Math.abs(mbr2.x1 - mbr1.x2), Math.abs(mbr1.x1 - mbr2.x2));
    }
    distance = Math.sqrt(dx * dx + dy * dy);
    if (distance > lb) lb = distance;
    
    return distance;
  }

  
  public static class FarthestPairMap extends
    Mapper<Rectangle, Iterable<Shape>, NullWritable, PairDistance> {
    
    /**
     * An initial lower bound for the distance of the farthest pair. If the
     * mapper cannot produce a farthest pair that is bigger than that, it does
     * not have to continue processing. 
     */
    private float fplb;
    /**File system of the input*/
    private FileSystem fs;
    /**The input path*/
    private Path inPath;
    /**The main partition assigned to this mapper*/
    private Partition mainPartition;
    /**Candidate partitions that can produce an answer with the main partition*/
    private Partition[] candidatePartitions;
    /**Upper bounds of candidate partitions*/
    private double[] candidateUpperBounds;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      inPath = SpatialInputFormat3.getInputPaths(context)[0];
      fs = inPath.getFileSystem(context.getConfiguration());
      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, inPath);
      if (gindex == null) // If not global index
        throw new RuntimeException("Farthest pair operation can only work with indexed files");
      fplb = context.getConfiguration().getFloat(FarthestPairLowerBound, 0);

      FileSplit fsplit = (FileSplit) context.getInputSplit();
      mainPartition = null;
      final List<Partition> otherPartitions = new ArrayList<Partition>();
      for (Partition p : gindex) {
        if (p.filename.equals(fsplit.getPath().getName()))
          mainPartition = p;
        else
          otherPartitions.add(p);
      }
      int i = 0;
      final List<Double> upperbounds = new ArrayList<Double>();
      while (i < otherPartitions.size()) {
        double upperBound = computeUB(mainPartition, otherPartitions.get(i));
        if (upperBound < fplb) {
          // It cannot produce any possible answer
          otherPartitions.remove(i);
        } else {
          // Can produce a possible answer, consider it
          upperbounds.add(upperBound);
          i++;
        }
      }
      
      // Sort by upper bounds
      IndexedSortable sortable = new IndexedSortable() {
        @Override
        public void swap(int i, int j) {
          double temp = upperbounds.get(i);
          upperbounds.set(i, upperbounds.get(j));
          upperbounds.set(j, temp);
          
          Partition temp2 = otherPartitions.get(i);
          otherPartitions.set(i, otherPartitions.get(j));
          otherPartitions.set(j, temp2);
        }
        
        @Override
        public int compare(int i, int j) {
          double d = upperbounds.get(i) - upperbounds.get(j);
          // Sort in descending order
          if (d < 0) return +1;
          if (d > 0) return -1;
          return 0;
        }
      };
      new QuickSort().sort(sortable, 0, i);
      candidatePartitions = otherPartitions.toArray(new Partition[i]);
      candidateUpperBounds = new double[i];
      while (i-- > 0)
        candidateUpperBounds[i] = upperbounds.get(i);
    }
    
    @Override
    protected void map(Rectangle key, Iterable<Shape> value, Context context)
            throws IOException, InterruptedException {
      Counter processedPairs = context.getCounter(FarthestPairCounters.FP_ProcessedPairs);
      List<Point> tempPoints = new ArrayList<Point>();
      for (Shape s : value)
        tempPoints.add((Point) s.clone());
      Point[] mainPoints = tempPoints.toArray(new Point[tempPoints.size()]);
      mainPoints = ConvexHull.convexHullInMemory(mainPoints);
      
      // Process other partitions, one-by-one in their order
      PairDistance answer = new PairDistance();
      answer.distance = fplb;
      int i = 0;
      final SpatialInputFormat3<Rectangle, Point> inputFormat =
          new SpatialInputFormat3<Rectangle, Point>();
      while (i < candidatePartitions.length && candidateUpperBounds[i] > answer.distance) {
        processedPairs.increment(1);
        tempPoints = new ArrayList<Point>();
        Path partitionPath = new Path(inPath, candidatePartitions[i].filename);
        FileStatus partitionStatus = fs.getFileStatus(partitionPath);
        FileSplit fsplit = new FileSplit(partitionPath, 0, partitionStatus.getLen(), new String[0]);
        final RecordReader<Rectangle, Iterable<Point>> reader =
            inputFormat.createRecordReader(fsplit, null);
        if (reader instanceof SpatialRecordReader3) {
          ((SpatialRecordReader3)reader).initialize(fsplit, context);
        } else if (reader instanceof RTreeRecordReader3) {
          ((RTreeRecordReader3)reader).initialize(fsplit, context);
        } else if (reader instanceof HDFRecordReader) {
          ((HDFRecordReader)reader).initialize(fsplit, context);
        } else {
          throw new RuntimeException("Unknown record reader");
        }
        while (reader.nextKeyValue()) {
          Iterable<Point> pts = reader.getCurrentValue();
          for (Point p : pts) {
            tempPoints.add(p.clone());
          }
        }
        reader.close();
        
        // Compute the farthest pair between mainPoints and otherPoints
        Point[] otherPoints = tempPoints.toArray(new Point[tempPoints.size()]);
        Point[] allPoints = new Point[mainPoints.length + otherPoints.length];
        System.arraycopy(mainPoints, 0, allPoints, 0, mainPoints.length);
        System.arraycopy(otherPoints, 0, allPoints, mainPoints.length, otherPoints.length);
        Point[] chull = ConvexHull.convexHullInMemory(allPoints);
        PairDistance fp = rotatingCallipers(chull);
        if (fp.distance > answer.distance)
          answer = fp;
        i++;
      }
      
      if (answer.first != null)
        context.write(NullWritable.get(), answer);
    }
  }
  
  public static class FarthestPairReducer extends
      Reducer<NullWritable, PairDistance, NullWritable, PairDistance> {

    @Override
    protected void reduce(NullWritable dummy, Iterable<PairDistance> values,
        Context context) throws IOException, InterruptedException {
      PairDistance globalFarthestPair = new PairDistance();
      Iterator<PairDistance> ivalues = values.iterator();
      if (ivalues.hasNext()) {
        PairDistance temp = ivalues.next();
        globalFarthestPair.first = temp.first.clone();
        globalFarthestPair.second = temp.second.clone();
        globalFarthestPair.distance = temp.distance;
      }
      
      while (ivalues.hasNext()) {
        PairDistance temp = ivalues.next();
        if (temp.distance > globalFarthestPair.distance) {
          globalFarthestPair.first = temp.first.clone();
          globalFarthestPair.second = temp.second.clone();
          globalFarthestPair.distance = temp.distance;
        }
      }
      
      context.write(dummy, globalFarthestPair);
    }
  }
  
  public static Job farthestPairMapReduce(Path inFile, Path outPath,
      OperationsParams params)
          throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(params, "FarthestPair");
    job.setJarByClass(FarthestPair.class);
    job.setMapperClass(FarthestPairMap.class);
    job.setReducerClass(FarthestPairReducer.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(PairDistance.class);
    job.setInputFormatClass(SpatialInputFormat3.class);
    // Add input file twice to treat it as a binary function
    SpatialInputFormat3.addInputPath(job, inFile);
    job.setOutputFormatClass(TextOutputFormat3.class);
    TextOutputFormat3.setOutputPath(job, outPath);
    
    // Calculate an initial lower bound on the farthest pair from the global index
    FileSystem fs = inFile.getFileSystem(params);
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, inFile);
    if (gindex != null) {
      double tightLowerBound = 0;
      for (Partition p1 : gindex) {
        for (Partition p2 : gindex) {
          double lb = computeLB(p1, p2);
          if (lb > tightLowerBound)
            tightLowerBound = lb;
        }
      }
      job.getConfiguration().setFloat(FarthestPairLowerBound, (float) tightLowerBound);
    }

    // Start the job
    if (params.getBoolean("background", false)) {
      // Run in background
      job.submit();
    } else {
      job.waitForCompletion(params.getBoolean("verbose", false));
    }
    return job;
  }

  public static PairDistance farthestPairLocal(Path[] inPaths, final OperationsParams params)
      throws IOException, InterruptedException {
    if (params.getBoolean("mem", false))
      MemoryReporter.startReporting();
    // 1- Split the input path/file to get splits that can be processed
    // independently
    final SpatialInputFormat3<Rectangle, Point> inputFormat =
        new SpatialInputFormat3<Rectangle, Point>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inPaths);
    final List<org.apache.hadoop.mapreduce.InputSplit> splits = inputFormat.getSplits(job);
    final Point[][] allLists = new Point[splits.size()][];
    
    // 2- Read all input points in memory
    LOG.info("Reading points from "+splits.size()+" splits");
    List<Integer> numsPoints = Parallel.forEach(splits.size(), new RunnableRange<Integer>() {
      @Override
      public Integer run(int i1, int i2) {
        try {
          int numPoints = 0;
          for (int i = i1; i < i2; i++) {
            List<Point> points = new ArrayList<Point>();
            org.apache.hadoop.mapreduce.lib.input.FileSplit fsplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit) splits.get(i);
            final org.apache.hadoop.mapreduce.RecordReader<Rectangle, Iterable<Point>> reader =
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
              Iterable<Point> pts = reader.getCurrentValue();
              for (Point p : pts) {
                points.add(p.clone());
              }
            }
            reader.close();
            numPoints += points.size();
            allLists[i] = points.toArray(new Point[points.size()]);
          }
          return numPoints;
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }, params.getInt("parallel", Runtime.getRuntime().availableProcessors()));
    
    int totalNumPoints = 0;
    for (int numPoints : numsPoints)
      totalNumPoints += numPoints;
    
    LOG.info("Read "+totalNumPoints+" points and merging into one list");
    Point[] allPoints = new Point[totalNumPoints];
    int pointer = 0;
    
    for (int iList = 0; iList < allLists.length; iList++) {
      System.arraycopy(allLists[iList], 0, allPoints, pointer, allLists[iList].length);
      pointer += allLists[iList].length;
      allLists[iList] = null; // To let the GC collect it
    }
    
    LOG.info("Computing closest-pair for "+allPoints.length+" points");
    long t1 = System.currentTimeMillis();
    Point[] convexHull = ConvexHull.convexHullInMemory(allPoints);
    long t2 = System.currentTimeMillis();
    PairDistance farthestPair = rotatingCallipers(convexHull);
    long t3 = System.currentTimeMillis();
    System.out.println("Convex hull in "+(t2-t1)/1000.0+" seconds "
        + "and rotating calipers in "+(t3-t2)/1000.0+" seconds");
    return farthestPair;
  }
  
  public static Job farthestPair(Path[] inFiles, Path outPath, OperationsParams params)
      throws IOException, InterruptedException,      ClassNotFoundException {
    if (OperationsParams.isLocal(params, inFiles)) {
      farthestPairLocal(inFiles, params);
      return null;
    } else {
      return farthestPairMapReduce(inFiles[0], outPath, params);
    }
  }
  
  private static void printUsage() {
    System.err.println("Computes the farthest pair of points in an input file of points");
    System.err.println("Parameters: (* marks required parameters)");
    System.err.println("<input file>: (*) Path to input file");
    System.err.println("<output file>: Path to output file");
    System.err.println("-overwrite: Overwrite output file without notice");
    
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    OperationsParams params = new OperationsParams(parser);
    
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path[] inFiles = params.getInputPaths();
    Path outPath = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    Job job = farthestPair(inFiles, outPath, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: " + (t2 - t1) + " millis");
    if (job != null) {
      Counter processedPairs = job.getCounters().findCounter(FarthestPairCounters.FP_ProcessedPairs);
      System.out.println("Processed "+processedPairs.getValue()+" pairs");
    }
  }
  
}
