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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
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
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat2;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

/**
 * Computes the convex hull for a set of shapes
 * @author Ahmed Eldawy
 *
 */
public class ConvexHull {
  
  private static final Log LOG = LogFactory.getLog(ConvexHull.class);
  
  /**
   * Computes the convex hull of a set of points using a divide and conquer
   * in-memory algorithm. This function implements Andrew's modification to
   * the Graham scan algorithm.
   * 
   * @param points
   * @return
   */
  public static Point[] convexHull(Point[] points) {
    Stack<Point> s1 = new Stack<Point>();
    Stack<Point> s2 = new Stack<Point>();
    
    
    Arrays.sort(points);
    
    // Lower chain
    for (int i=0; i<points.length; i++) {
      while(s1.size() > 1) {
        Point p1 = s1.get(s1.size() - 2);
        Point p2 = s1.get(s1.size() - 1);
        Point p3 = points[i];
        double crossProduct = (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x);
        if (crossProduct <= 0) s1.pop();
        else break;
      }
      s1.push(points[i]);
    }
    
    // Upper chain
    for (int i=points.length - 1; i>=0; i--) {
      while(s2.size() > 1) {
        Point p1 = s2.get(s2.size() - 2);
        Point p2 = s2.get(s2.size() - 1);
        Point p3 = points[i];
        double crossProduct = (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x);
        if (crossProduct <= 0) s2.pop();
        else break;
      }
      s2.push(points[i]);
    }
    
    s1.pop();
    s2.pop();
    s1.addAll(s2);
    return s1.toArray(new Point[s1.size()]);    
  }
  
  /**
   * Computes the convex hull of an input file using a single machine algorithm.
   * The output is written to the output file. If output file is null, the
   * output is just thrown away.
   * @param inFile
   * @param outFile
   * @param overwrite
   * @throws IOException
   */
  public static void convexHullLocal(Path inFile, Path outFile,
      boolean overwrite) throws IOException {
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
    
    Point[] convex_hull = convexHull(arPoints);

    if (outFile != null) {
      if (overwrite) {
        FileSystem outFs = outFile.getFileSystem(new Configuration());
        outFs.delete(outFile, true);
      }
      GridRecordWriter<Point> out = new GridRecordWriter<Point>(outFile, null, null, null, false);
      for (Point pt : convex_hull) {
        out.write(NullWritable.get(), pt);
      }
      out.close(null);
    }
  }
  
  /**
   * Filters partitions to remove ones that do not contribute to answer.
   * A partition is pruned if it does not have any points in any of the four
   * skylines.
   * @author Ahmed Eldawy
   *
   */
  public static class ConvexHullFilter extends DefaultBlockFilter {
    
    @Override
    public void selectCells(GlobalIndex<Partition> gIndex,
        ResultCollector<Partition> output) {
      Set<Partition> non_dominated_partitions_all = new HashSet<Partition>();
      for (Skyline.Direction dir : Skyline.Direction.values()) {
        Vector<Partition> non_dominated_partitions = new Vector<Partition>();
        for (Partition p : gIndex) {
          boolean dominated = false;
          int i = 0;
          while (!dominated && i < non_dominated_partitions.size()) {
            Partition p2 = non_dominated_partitions.get(i);
            dominated = Skyline.skylineDominate(p2, p, dir, gIndex.isCompact());
            
            // Check if the new partition dominates the previously selected one
            if (Skyline.skylineDominate(p, p2, dir, gIndex.isCompact())) {
              // p2 is no longer non-dominated
              non_dominated_partitions.remove(i);
            } else {
              // Skip to next non-dominated partition
              i++;
            }
          }
          if (!dominated) {
            non_dominated_partitions.add(p);
          }
        }
        non_dominated_partitions_all.addAll(non_dominated_partitions);
      }
      
      LOG.info("Processing "+non_dominated_partitions_all.size()+" out of "+gIndex.size()+" partition");
      // Output all non-dominated partitions
      for (Partition p : non_dominated_partitions_all) {
        output.collect(p);
      }
    }
  }
  
  /**
   * An identity map function that returns values as-is with a null key. This
   * ensures that all values are reduced in one reducer.
   * @author Ahmed Eldawy
   */
  public static class IdentityMapper extends MapReduceBase implements
  Mapper<Rectangle, Point, NullWritable, Point> {
    @Override
    public void map(Rectangle dummy, Point point,
        OutputCollector<NullWritable, Point> output, Reporter reporter)
        throws IOException {
      output.collect(NullWritable.get(), point);
    }
    
  }
  
  public static class ConvexHullReducer extends MapReduceBase implements
  Reducer<NullWritable,Point,NullWritable,Point> {
    
    @Override
    public void reduce(NullWritable dummy, Iterator<Point> points,
        OutputCollector<NullWritable, Point> output, Reporter reporter)
        throws IOException {
      Vector<Point> vpoints = new Vector<Point>();
      while (points.hasNext()) {
        vpoints.add(points.next().clone());
      }
      Point[] convex_hull = convexHull(vpoints.toArray(new Point[vpoints.size()]));
      for (Point pt : convex_hull) {
        output.collect(dummy, pt);
      }
    }
  }
  
  public static void convexHullMapReduce(Path inFile, Path userOutPath,
      boolean overwrite) throws IOException {
    JobConf job = new JobConf(ConvexHull.class);
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
    job.setClass(SpatialSite.FilterClass, ConvexHullFilter.class, BlockFilter.class);
    job.setMapperClass(IdentityMapper.class);
    job.setCombinerClass(ConvexHullReducer.class);
    job.setReducerClass(ConvexHullReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Point.class);
    job.setInputFormat(ShapeInputFormat.class);
    SpatialSite.setShapeClass(job, Point.class);
    ShapeInputFormat.addInputPath(job, inFile);
    job.setOutputFormat(GridOutputFormat2.class);
    GridOutputFormat2.setOutputPath(job, outPath);
    
    JobClient.runJob(job);
   
    // If outputPath not set by user, automatically delete it
    if (userOutPath == null)
      outFs.delete(outPath, true);
  }
  
  private static void printUsage() {
    System.err.println("Computes the convex hull of an input file of shapes");
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
    
    long t1 = System.currentTimeMillis();
    convexHullMapReduce(inFile, outFile, cla.isOverwrite());
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
  
}
