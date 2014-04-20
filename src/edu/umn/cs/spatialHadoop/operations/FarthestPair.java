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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BinaryRecordReader;
import edu.umn.cs.spatialHadoop.mapred.BinarySpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat2;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayRecordReader;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

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
        farthest_pair.first = a[i];
        farthest_pair.second = a[j];
      }

      dist = a[i_plus_one].distanceTo(a[j]);
      if (dist > farthest_pair.distance) {
        farthest_pair.distance = dist;
        farthest_pair.first = a[i_plus_one];
        farthest_pair.second = a[j];
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
        farthest_pair.first = pi;
        farthest_pair.second = pj;
      }

      dist = pi_plus_one.distanceTo(pj);
      if (dist > farthest_pair.distance) {
        farthest_pair.distance = dist;
        farthest_pair.first = pi_plus_one;
        farthest_pair.second = pj;
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

  
  /**
   * Computes the closest pair by reading points from stream
   * @param p 
   * @return 
   */
  public static <S extends Point> PairDistance farthestPairStream(S p) throws IOException {
    ShapeRecordReader<S> reader =
        new ShapeRecordReader<S>(System.in, 0, Long.MAX_VALUE);
    double[] xs = new double[1000000];
    double[] ys = new double[1000000];
    int size = 0;
    
    Rectangle key = new Rectangle();
    while (reader.next(key, p)) {
      xs[size] = p.x;
      ys[size] = p.y;
      size++;
      if (size == xs.length) {
        // Need to expand array
        double[] old_xs = xs;
        xs = new double[old_xs.length * 2];
        System.arraycopy(old_xs, 0, xs, 0, old_xs.length);
        double[] old_ys = ys;
        ys = new double[old_ys.length * 2];
        System.arraycopy(old_ys, 0, ys, 0, old_ys.length);
      }
      if ((size % 10000000) == 0) {
        LOG.info("Loaded "+size+" points");
      }
    }
    LOG.info("Loaded a total of "+size+ " points");
    int[] hull_points = convexHullInPlace(xs, ys, size);
    LOG.info("Convex hull computed with "+hull_points.length+" points");
    return rotatingCallipersLocal(xs, ys, hull_points);
  }


  public static class FarthestPairFilter extends DefaultBlockFilter {

    static class PartitionPair {
      Partition p1, p2;
      double mindist, maxdist;
      public PartitionPair(Partition p1, Partition p2) {
        this.p1 = p1;
        this.p2 = p2;
        mindist = this.p1.getMinDistance(this.p2);
        maxdist = this.p1.getMaxDistance(this.p2);
      }

      boolean dominates(PartitionPair pp2) {
        return this.mindist > pp2.maxdist;
      }
    }
    
    @Override
    public void selectCellPairs(GlobalIndex<Partition> gIndex1,
        GlobalIndex<Partition> gIndex2,
        ResultCollector2<Partition, Partition> output) {
      ArrayList<PartitionPair> selectedPairs = new ArrayList<PartitionPair>();
      for (Partition p1 : gIndex1) {
        for (Partition p2 : gIndex2) {
          if (p1.equals(p2))
            continue;

          // Compare this partition pair to all other pairs
          PartitionPair pp = new PartitionPair(p1, p2);
          int i = 0;
          boolean dominated = false;
          while (!dominated && i < selectedPairs.size()) {
            PartitionPair pp2 = selectedPairs.get(i);
            // Check if pp dominates an already selected partition pair
            if (pp.dominates(pp2)) {
              selectedPairs.remove(i);
            } else {
              // Check if pp is dominated
              dominated = pp2.dominates(pp);
              i++;
            }
          }
          
          // The new pair is not dominated by any other pair, add it
          if (!dominated)
            selectedPairs.add(pp);
        }
      }
      LOG.info("Selected " + selectedPairs.size() + " out of "
          + (gIndex1.size() * gIndex2.size()) + " possible pairs");
      
      for (PartitionPair pp : selectedPairs) {
        output.collect(pp.p1, pp.p2);
      }
    }
  }

  public static class FarthestPairMap extends MapReduceBase
      implements
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
      Point[] convexHull = ConvexHull.convexHull(points);
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
    JobConf job = new JobConf(FarthestPair.class);
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
    job.setClass("shape", Point.class, Shape.class);
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
  
  private static void printUsage() {
    System.err.println("Computes the farthest pair of points in an input file of points");
    System.err.println("Parameters: (* marks required parameters)");
    System.err.println("<input file>: (*) Path to input file");
    System.err.println("<output file>: Path to output file");
    System.err.println("-overwrite: Overwrite output file without notice");
    
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] paths = params.getPaths();
    if (paths.length == 0) {
      if (params.is("local")) {
        long t1 = System.currentTimeMillis();
        farthestPairStream((Point)params.getShape("shape"));
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: "+(t2-t1)+" millis");
      } else {
        printUsage();
        System.exit(1);
      }
      return;
    }
    if (paths.length == 1 && !params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    if (paths.length > 1 && !params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    Path inFile = params.getInputPath();
    Path outFile = params.getOutputPath();
    
    long t1 = System.currentTimeMillis();
    farthestPairMapReduce(inFile, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
  
}
