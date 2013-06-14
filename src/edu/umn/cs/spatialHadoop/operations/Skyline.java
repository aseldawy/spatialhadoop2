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
import java.util.Iterator;
import java.util.Vector;

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
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat2;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

/**
 * Computes the skyline of a set of points
 * @author Ahmed Eldawy
 *
 */
public class Skyline {
  
  /**Data type for the direction of skyline to compute*/
  enum Direction {MaxMax, MaxMin, MinMax, MinMin}

  /**
   * Computes the skyline of a set of points using a divided and conquer
   * in-memory algorithm. The algorithm recursively splits the points into
   * half, computes the skyline of each half, and finally combines the two
   * skylines.
   * @param points
   * @param dir
   * @return
   */
  public static Point[] skyline(Point[] points, Direction dir) {
    Arrays.sort(points);
    return skyline(points, 0, points.length, dir);
  }
  
  /**
   * The recursive method of skyline
   * @param points
   * @param start
   * @param end
   * @param dir
   * @return
   */
  private static Point[] skyline(Point[] points, int start, int end, Direction dir) {
    if (end - start == 1) {
      // Return the one input point as the skyline
      return new Point[] {points[start]};
    }
    int mid = (start + end) / 2;
    // Find the skyline of each half
    Point[] skyline1 = skyline(points, start, mid, dir);
    Point[] skyline2 = skyline(points, mid, end, dir);
    // Merge the two skylines
    int cut_point = 0;
    while (cut_point < skyline1.length && !skylineDominate(skyline2[0], skyline1[cut_point], dir))
      cut_point++;
    Point[] result = new Point[cut_point + skyline2.length];
    System.arraycopy(skyline1, 0, result, 0, cut_point);
    System.arraycopy(skyline2, 0, result, cut_point, skyline2.length);
    return result;
  }

  private static boolean skylineDominate(Point p1, Point p2, Direction dir) {
    switch (dir) {
    case MaxMax: return p1.x >= p2.x && p1.y >= p2.y;
    case MaxMin: return p1.x >= p2.x && p1.y <= p2.y;
    case MinMax: return p1.x <= p2.x && p1.y >= p2.y;
    case MinMin: return p1.x <= p2.x && p1.y <= p2.y;
    default: throw new RuntimeException("Unknown direction: "+dir);
    }
  }
  
  public static void skylineLocal(Path inFile, Path outFile, Direction dir,
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
    
    Point[] skyline = skyline(arPoints, dir);

    if (outFile != null) {
      if (overwrite) {
        FileSystem outFs = outFile.getFileSystem(new Configuration());
        outFs.delete(outFile, true);
      }
      GridRecordWriter<Point> out = new GridRecordWriter<Point>(outFile, null, null, null, false);
      for (Point pt : skyline) {
        out.write(NullWritable.get(), pt);
      }
      out.close(null);
    }
  }
  
  private static final String SkylineDirection = "skyline.direction";
  
  private static void setDirection(Configuration conf, Direction dir) {
    conf.set(SkylineDirection, dir.toString());
  }
  
  public static class IdentityMapper extends MapReduceBase implements
  Mapper<Rectangle, Point, NullWritable, Point> {

    @Override
    public void map(Rectangle dummy, Point point,
        OutputCollector<NullWritable, Point> output, Reporter reporter)
        throws IOException {
      output.collect(NullWritable.get(), point);
    }
    
  }
  
  private static Direction getDirection(Configuration conf) {
    String strdir = conf.get(SkylineDirection, Direction.MaxMax.toString());
    return Direction.valueOf(strdir);
  }

  public static class SkylineReducer extends MapReduceBase implements
  Reducer<NullWritable,Point,NullWritable,Point> {
    
    private Direction dir;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      dir = getDirection(job);
    }
    

    @Override
    public void reduce(NullWritable dummy, Iterator<Point> points,
        OutputCollector<NullWritable, Point> output, Reporter reporter)
        throws IOException {
      Vector<Point> vpoints = new Vector<Point>();
      while (points.hasNext()) {
        vpoints.add(points.next().clone());
      }
      Point[] skyline = skyline(vpoints.toArray(new Point[vpoints.size()]), dir);
      for (Point pt : skyline) {
        output.collect(dummy, pt);
      }
    }
  }
  
  public static void skylineMapReduce(Path inFile, Path userOutPath, Direction dir,
      boolean overwrite) throws IOException {
    JobConf job = new JobConf(Skyline.class);
    Path outPath = userOutPath;
    FileSystem outFs = (userOutPath == null ? inFile : userOutPath).getFileSystem(job);
    
    if (outPath == null) {
      do {
        outPath = new Path(inFile.toUri().getPath()+
            ".skyline_"+(int)(Math.random() * 1000000));
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
    
    setDirection(job, dir);
    job.setMapperClass(IdentityMapper.class);
    job.setCombinerClass(SkylineReducer.class);
    job.setReducerClass(SkylineReducer.class);
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
    System.err.println("Computes the skyline of an input file of points");
    System.err.println("Parameters: (* marks required parameters)");
    System.err.println("<input file>: (*) Path to input file");
    System.err.println("<output file>: Path to output file");
    System.err.println("<direction (max-max|max-min|min-max|min-min)>: Direction of skyline (default is max-max)");
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
    if (!overwrite && outFile.getFileSystem(new Configuration()).exists(outFile)) {
      System.err.println("Output path already exists and overwrite flag is not set");
      return;
    }
    String strdir = cla.get("dir");
    Direction dir = Direction.MaxMax;
    if (strdir != null) {
      if (strdir.equalsIgnoreCase("maxmax") || strdir.equalsIgnoreCase("max-max")) {
        dir = Direction.MaxMax;
      } else if (strdir.equalsIgnoreCase("maxmin") || strdir.equalsIgnoreCase("max-min")) {
        dir = Direction.MaxMin;
      } else if (strdir.equalsIgnoreCase("minmax") || strdir.equalsIgnoreCase("min-max")) {
        dir = Direction.MinMax;
      } else if (strdir.equalsIgnoreCase("minmin") || strdir.equalsIgnoreCase("min-min")) {
        dir = Direction.MinMin;
      } else {
        System.err.println("Invalid direction: "+strdir);
        System.err.println("Valid directions are: max-max, max-min, min-max, and min-min");
        printUsage();
        return;
      }
    }    
    
    skylineMapReduce(inFile, outFile, dir, cla.isOverwrite());
  }
  
}
