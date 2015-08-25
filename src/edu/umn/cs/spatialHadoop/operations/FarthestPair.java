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
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapred.BinaryRecordReader;
import edu.umn.cs.spatialHadoop.mapred.BinarySpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat2;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
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
    int i = 0, j = 1, j_plus_one = 2 % a.length;
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
  
  
  private static PairDistance rotatingCallipersLocal(double[] xs, double[] ys, int[] hull_points) {
    PairDistance farthest_pair = new PairDistance();
    Point pi = new Point();
    Point pi_plus_one = new Point();
    Point pj = new Point();
    Point pj_plus_one = new Point();
    int i = 0, j = 1, j_plus_one = 2 % hull_points.length;
    pj.x = xs[hull_points[j]]; pj.y = ys[hull_points[j]];
    pj_plus_one.x = xs[hull_points[j_plus_one]]; pj_plus_one.y = ys[hull_points[j_plus_one]];
    for (i = 0; i < hull_points.length; i++) {
      pi.x = xs[hull_points[i]]; pi.y = ys[hull_points[i]];
      int i_plus_one = (i + 1) % hull_points.length;
      pi_plus_one.x = xs[hull_points[i_plus_one]]; pi_plus_one.y = ys[hull_points[i_plus_one]];
      while(cross(pi, pi_plus_one, pj_plus_one) > cross(pi, pi_plus_one, pj)) {
        j = j_plus_one;
        j_plus_one = (j + 1) % hull_points.length;
        pj.x = xs[hull_points[j]]; pj.y = ys[hull_points[j]];
        pj_plus_one.x = xs[hull_points[j_plus_one]]; pj_plus_one.y = ys[hull_points[j_plus_one]];
      }
      double dist = pi.distanceTo(pj);
      if (dist > farthest_pair.distance) {
        farthest_pair.distance = dist;
        farthest_pair.first.set(pi.x, pi.y);
        farthest_pair.second.set(pj.x, pj.y);
      }

      dist = pi_plus_one.distanceTo(pj);
      if (dist > farthest_pair.distance) {
        farthest_pair.distance = dist;
        farthest_pair.first.set(pi_plus_one.x, pi_plus_one.y);
        farthest_pair.second.set(pj.x, pj.y);
      }
    }
    return farthest_pair;
  }

  
  /**
   * In place version of convex hull. Given points are sorted on x direction
   * and the indexes of points on the convex hull are returned.
   * @param xs
   * @param ys
   * @param size
   * @return
   */
  public static int[] convexHullInPlace(final double[] xs, final double[] ys, final int size) {
    Stack<Integer> s1 = new Stack<Integer>();
    Stack<Integer> s2 = new Stack<Integer>();
    
    
    IndexedSortable sortableX = new IndexedSortable() {
      
      @Override
      public void swap(int a, int b) {
        double temp = xs[a];
        xs[a] = xs[b];
        xs[b] = temp;
        temp = ys[a];
        ys[a] = ys[b];
        ys[b] = temp;
      }
      
      @Override
      public int compare(int a, int b) {
    	if (xs[a] == xs[b])
    	  return 0;
        if (xs[a] < xs[b])
          return -1;
        return 1;
      }
    };
    
    final IndexedSorter sorter = new QuickSort();
    sorter.sort(sortableX, 0, size);    
    
    Point p1 = new Point();
    Point p2 = new Point();
    Point p3 = new Point();
    
    // Lower chain
    for (int i=0; i<size; i++) {
      while(s1.size() > 1) {
        p1.x = xs[s1.get(s1.size() - 2)];
        p1.y = ys[s1.get(s1.size() - 2)];
        p2.x = xs[s1.get(s1.size() - 1)];
        p2.y = ys[s1.get(s1.size() - 1)];
        p3.x = xs[i];
        p3.y = ys[i];
        double crossProduct = (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x);
        if (crossProduct <= 0) s1.pop();
        else break;
      }
      s1.push(i);
    }
    
    // Upper chain
    for (int i = size - 1; i >= 0; i--) {
      while(s2.size() > 1) {
        p1.x = xs[s2.get(s2.size() - 2)];
        p1.y = ys[s2.get(s2.size() - 2)];
        p2.x = xs[s2.get(s2.size() - 1)];
        p2.y = ys[s2.get(s2.size() - 1)];
        p3.x = xs[i];
        p3.y = ys[i];
        double crossProduct = (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x);
        if (crossProduct <= 0) s2.pop();
        else break;
      }
      s2.push(i);
    }
    
    s1.pop();
    s2.pop();
    s1.addAll(s2);
    int[] hull_points = new int[s1.size()];
    for (int i = 0; i < s1.size(); i++)
      hull_points[i] = s1.get(i);
    return hull_points;    
  }

  public static class FarthestPairFilter extends DefaultBlockFilter {
    
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

    @Override
    public void selectCellPairs(GlobalIndex<Partition> gIndex1,
        GlobalIndex<Partition> gIndex2,
        ResultCollector2<Partition, Partition> output) {
      double fp_lb = 0;
      for (Partition p1 : gIndex1) {
        for (Partition p2 : gIndex2) {
          double min_distance = computeLB(p1, p2);
          if (min_distance > fp_lb)
            fp_lb = min_distance;
        }
      }
      LOG.info("LB on distance is "+fp_lb);
      int selectedPairs = 0;
      for (Partition p1 : gIndex1) {
        boolean selected = false;
        for (Partition p2 : gIndex2) {
          if (p1.equals(p2))
            continue;
          double max_distance = computeUB(p1, p2);
          if (max_distance >= fp_lb) {
            output.collect(p1, p2);
            selectedPairs++;
            selected = true;
          }
        }
        if (!selected) {
          // Test if this partition should be processed by itself and has not
          // been selected for process with any other partition
          if (computeUB(p1, p1) >= fp_lb) {
            output.collect(p1, p1);
            selectedPairs++;
          }
        }
      }
      LOG.info("Selected " + selectedPairs + " out of "
          + (gIndex1.size() * (gIndex2.size()-1) / 2) + " possible pairs");
      System.out.println("Selected " + selectedPairs + " out of "
          + (gIndex1.size() * (gIndex2.size()-1) / 2) + " possible pairs");
    }
  }

  public static class FarthestPairMap extends MapReduceBase implements
      Mapper<PairWritable<Rectangle>, PairWritable<ArrayWritable>, NullWritable, PairDistance> {
    @Override
    public void map(PairWritable<Rectangle> key,
        PairWritable<ArrayWritable> value,
        OutputCollector<NullWritable, PairDistance> out, Reporter reporter)
        throws IOException {
      Shape[] shapes1 = (Shape[]) value.first.get();
      Shape[] shapes2 = (Shape[]) value.second.get();
      // Concatenate shapes1 and shapes2 into one array
      Point[] points = new Point[shapes1.length + shapes2.length];
      System.arraycopy(shapes1, 0, points, 0, shapes1.length);
      System.arraycopy(shapes2, 0, points, shapes1.length, shapes2.length);
      
      // Calculate the farthest pair of all points
      Point[] convexHull = ConvexHull.convexHullInMemory(points);
      PairDistance farthestPair = rotatingCallipers(convexHull);
      out.collect(NullWritable.get(), farthestPair);
    }
  }
  
  
  public static class FarthestPairReducer extends MapReduceBase implements
  Reducer<NullWritable, PairDistance, NullWritable, PairDistance> {

    @Override
    public void reduce(NullWritable key, Iterator<PairDistance> values,
        OutputCollector<NullWritable, PairDistance> output, Reporter reporter)
        throws IOException {
      PairDistance globalFarthestPair = new PairDistance();
      if (values.hasNext()) {
        PairDistance temp = values.next();
        globalFarthestPair.first = temp.first.clone();
        globalFarthestPair.second = temp.second.clone();
        globalFarthestPair.distance = temp.distance;
      }
      
      while (values.hasNext()) {
        PairDistance temp = values.next();
        if (temp.distance > globalFarthestPair.distance) {
          globalFarthestPair.first = temp.first.clone();
          globalFarthestPair.second = temp.second.clone();
          globalFarthestPair.distance = temp.distance;
        }
      }
      
      output.collect(key, globalFarthestPair);
    }
  }
  
  
  /**
   * Input format that returns a record reader that reads a pair of arrays of
   * shapes
   * @author Ahmed Eldawy
   *
   */
  public static class FPInputFormatArray extends BinarySpatialInputFormat<Rectangle, ArrayWritable> {

    /**
     * Reads a pair of arrays of shapes
     * @author Ahmed Eldawy
     *
     */
    public static class FPRecordReader extends BinaryRecordReader<Rectangle, ArrayWritable> {
      public FPRecordReader(Configuration conf, CombineFileSplit fileSplits) throws IOException {
        super(conf, fileSplits);
      }
      
      @Override
      protected RecordReader<Rectangle, ArrayWritable> createRecordReader(
          Configuration conf, CombineFileSplit split, int i) throws IOException {
        FileSplit fsplit = new FileSplit(split.getPath(i),
            split.getStartOffsets()[i],
            split.getLength(i), split.getLocations());
        return new ShapeArrayRecordReader(conf, fsplit);
      }
    }

    @Override
    public RecordReader<PairWritable<Rectangle>, PairWritable<ArrayWritable>> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new FPRecordReader(job, (CombineFileSplit)split);
    }
  }
  
  public static void farthestPairMapReduce(Path inFile, Path userOutPath,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, FarthestPair.class);
    Path outPath = userOutPath;
    FileSystem outFs = (userOutPath == null ? inFile : userOutPath).getFileSystem(job);
    
    if (outPath == null) {
      do {
        outPath = new Path(inFile.toUri().getPath()+
            ".farthest_pair_"+(int)(Math.random() * 1000000));
      } while (outFs.exists(outPath));
    }
    
    job.setJobName("FarthestPair");
    job.setClass(SpatialSite.FilterClass, FarthestPairFilter.class, BlockFilter.class);
    job.setMapperClass(FarthestPairMap.class);
    job.setReducerClass(FarthestPairReducer.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(PairDistance.class);
    job.setInputFormat(FPInputFormatArray.class);
    // Add input file twice to treat it as a binary function
    FPInputFormatArray.addInputPath(job, inFile);
    FPInputFormatArray.addInputPath(job, inFile);
    job.setOutputFormat(TextOutputFormat.class);
    GridOutputFormat2.setOutputPath(job, outPath);
    
    JobClient.runJob(job);
    
    // If outputPath not set by user, automatically delete it
    if (userOutPath == null)
      outFs.delete(outPath, true);
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
    Vector<Integer> numsPoints = Parallel.forEach(splits.size(), new RunnableRange<Integer>() {
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
  
  public static void farthestPair(Path[] inFiles, Path outPath, OperationsParams params)
      throws IOException, InterruptedException,      ClassNotFoundException {
    if (OperationsParams.isLocal(params, inFiles)) {
      farthestPairLocal(inFiles, params);
    } else {
      farthestPairMapReduce(inFiles[0], outPath, params);
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
    farthestPair(inFiles, outPath, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: " + (t2 - t1) + " millis");
  }
  
}
