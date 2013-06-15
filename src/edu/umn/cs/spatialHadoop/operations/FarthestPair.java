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
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat2;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

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
  
  public static class PairDistance {
    public Point p1, p2;
    public double distance;
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
        farthest_pair.p1 = a[i];
        farthest_pair.p2 = a[j];
      }

      dist = a[i_plus_one].distanceTo(a[j]);
      if (dist > farthest_pair.distance) {
        farthest_pair.distance = dist;
        farthest_pair.p1 = a[i_plus_one];
        farthest_pair.p2 = a[j];
      }
    }
    return farthest_pair;
  }
  
  /**
   * Computes the farthest pair of points in an input file of points.
   * @param inFile
   * @param outFile
   * @param overwrite
   * @throws IOException
   */
  public static PairDistance farthestPairLocal(Path inFile, boolean overwrite)
      throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    long file_size = inFs.getFileStatus(inFile).getLen();
    
    ShapeRecordReader<Point> shapeReader = new ShapeRecordReader<Point>(
        new Configuration(), new FileSplit(inFile, 0, file_size, new String[] {}));

    Rectangle key = shapeReader.createKey();
    Point point = new Point();

    Vector<Point> points = new Vector<Point>();
    
    while (shapeReader.next(key, point)) {
      points.add(point.clone());
    }
    
    Point[] arPoints = points.toArray(new Point[points.size()]);
    
    Point[] convex_hull = ConvexHull.convexHull(arPoints);
    return rotatingCallipers(convex_hull);
  }
  
  public static class FarthestPairReducer extends MapReduceBase implements
  Reducer<NullWritable,Point,NullWritable,Point> {
    
    @Override
    public void reduce(NullWritable dummy, Iterator<Point> points,
        OutputCollector<NullWritable, Point> output, Reporter reporter)
        throws IOException {
      Vector<Point> vpoints = new Vector<Point>();
      while (points.hasNext()) {
        vpoints.add(points.next().clone());
      }
      Point[] convex_hull = ConvexHull.convexHull(vpoints.toArray(new Point[vpoints.size()]));
      PairDistance farthest_pair = rotatingCallipers(convex_hull);
      
      // Output the two points
      output.collect(dummy, farthest_pair.p1);
      output.collect(dummy, farthest_pair.p2);
    }
  }
  
  public static void farthestPairMapReduce(Path inFile, Path userOutPath,
      boolean overwrite) throws IOException {
    JobConf job = new JobConf(FarthestPair.class);
    Path outPath = userOutPath;
    FileSystem outFs = (userOutPath == null ? inFile : userOutPath).getFileSystem(job);
    
    if (outPath == null) {
      do {
        outPath = new Path(inFile.toUri().getPath()+
            ".convex_hull_"+(int)(Math.random() * 1000000));
      } while (outFs.exists(outPath));
    } else {
      if (outFs.exists(outPath)) {
        if (overwrite) {
          outFs.delete(outPath, true);
        } else {
          throw new RuntimeException("Output path already exists and -overwrite flag is not set");
        }
      }
    }
    
    job.setJobName("ConvexHull");
    job.setClass(SpatialSite.FilterClass, ConvexHull.ConvexHullFilter.class, BlockFilter.class);
    job.setMapperClass(ConvexHull.IdentityMapper.class);
    job.setCombinerClass(ConvexHull.ConvexHullReducer.class);
    job.setReducerClass(FarthestPairReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Point.class);
    job.setInputFormat(ShapeInputFormat.class);
    job.set(SpatialSite.SHAPE_CLASS, Point.class.getName());
    ShapeInputFormat.addInputPath(job, inFile);
    job.setOutputFormat(GridOutputFormat2.class);
    GridOutputFormat2.setOutputPath(job, outPath);
    
    JobClient.runJob(job);
    
  }
  
  private static void printUsage() {
    System.err.println("Computes the farthest pair of points in an input file of points");
    System.err.println("Parameters: (* marks required parameters)");
    System.err.println("<input file>: (*) Path to input file");
    System.err.println("<output file>: Path to output file");
    System.err.println("-overwrite: Overwrite output file without notice");
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] paths = cla.getPaths();
    if (paths.length == 0) {
      printUsage();
      return;
    }
    Path inFile = paths[0];
    Path outFile = paths.length > 1? paths[1] : null;
    boolean overwrite = cla.isOverwrite();
    if (!overwrite && outFile != null && outFile.getFileSystem(new Configuration()).exists(outFile)) {
      System.err.println("Output path already exists and overwrite flag is not set");
      return;
    }
    
    farthestPairMapReduce(inFile, outFile, cla.isOverwrite());
  }
  
}
