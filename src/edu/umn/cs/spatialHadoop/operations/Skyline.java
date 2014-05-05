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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.OperationsParams.Direction;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat2;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

/**
 * Computes the skyline of a set of points
 * @author Ahmed Eldawy
 *
 */
public class Skyline {
  
  private static final Log LOG = LogFactory.getLog(Skyline.class);
  
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

  /**
   * Returns true if p1 dominates p2 according to the given direction.
   * @param p1
   * @param p2
   * @param dir
   * @return
   */
  private static boolean skylineDominate(Point p1, Point p2, Direction dir) {
    switch (dir) {
    case MaxMax: return p1.x >= p2.x && p1.y >= p2.y;
    case MaxMin: return p1.x >= p2.x && p1.y <= p2.y;
    case MinMax: return p1.x <= p2.x && p1.y >= p2.y;
    case MinMin: return p1.x <= p2.x && p1.y <= p2.y;
    default: throw new RuntimeException("Unknown direction: "+dir);
    }
  }
  
  /**
   * Returns true if r1 dominates r2 in the given direction. r1 dominates r2 if
   * one point in r1 dominates all points in r2. There are two rules for
   * rectangle domination. We will describe them here in terms of MaxMax skyline
   * but they can be expanded to cover all other skylines.
   * 1- If the lowest corner of r1 dominates the highest corner of r2.
   * 2- If the semi-lowest corner of r1 dominates the highest corner of r2.
   * The lowest (highest) corner of a rectangle is the point inside this
   * rectangle with the minimum (maximum) coordinates in all dimensions.
   * The semi-lowest corner of a rectangle is a point inside the rectangle with
   * the minimum coordinates in all dimensions except for one dimension which
   * is the maximum coordinate.
   * 
   * Rule 2 is applied only if rectangles are compact which means they are
   * minimal bounding rectangles around contained points.
   * 
   * @param r1
   * @param r2
   * @param dir
   * @param compact - whether the input rectangles are compact or not
   * @return
   */
  public static boolean skylineDominate(Rectangle r1, Rectangle r2,
      Direction dir, boolean compact) {
    switch (dir) {
    case MaxMax:
      return compact ? (r1.x2 >= r2.x2 && r1.y1 >= r2.y2) ||
                       (r1.x1 >= r2.x2 && r1.y2 >= r2.y2) :
                       (r1.x1 >= r2.x2 && r1.y1 >= r2.y2);
    case MaxMin:
      return compact ? (r1.x2 >= r2.x2 && r1.y2 <= r2.y1) ||
                       (r1.x1 >= r2.x2 && r1.y1 <= r2.y1) :
                       (r1.x1 >= r2.x2 && r1.y2 <= r2.y1);
    case MinMax:
      return compact ? (r1.x2 <= r2.x1 && r1.y2 >= r2.y2) ||
                       (r1.x1 <= r2.x1 && r1.y2 >= r2.y2) :
                       (r1.x2 <= r2.x1 && r1.y1 >= r2.y2);
    case MinMin:
      return compact ? (r1.x2 <= r2.x1 && r1.y1 <= r2.y1) ||
                       (r1.x1 <= r2.x1 && r1.y2 <= r2.y1) :
                       (r1.x2 <= r2.x1 && r1.y2 <= r2.y1);
    default: throw new RuntimeException("Unknown direction: "+dir);
    }
  }
  
  /**
   * Computes the skyline of an input file using a single machine algorithm.
   * The output is written to the output file. If output file is null, the
   * output is just thrown away.
   * @param inFile
   * @param outFile
   * @param dir
   * @param overwrite
   * @throws IOException
   */
  public static void skylineLocal(Path inFile, Path outFile,
      OperationsParams params) throws IOException {
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
    Direction dir = params.getDirection("dir", Direction.MaxMax);
    Point[] skyline = skyline(arPoints, dir);

    if (outFile != null) {
      if (params.getBoolean("overwrite", false)) {
        FileSystem outFs = outFile.getFileSystem(new Configuration());
        outFs.delete(outFile, true);
      }
      GridRecordWriter<Point> out = new GridRecordWriter<Point>(outFile, null, null, null);
      for (Point pt : skyline) {
        out.write(NullWritable.get(), pt);
      }
      out.close(null);
    }
  }
  
  public static class SkylineFilter extends DefaultBlockFilter {
    
    private Direction dir;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      dir = OperationsParams.getDirection(job, "dir", Direction.MaxMax);
    }
    
    @Override
    public void selectCells(GlobalIndex<Partition> gIndex,
        ResultCollector<Partition> output) {
      Vector<Partition> non_dominated_partitions = new Vector<Partition>();
      
      for (Partition p : gIndex) {
        boolean dominated = false;
        int i = 0;
        while (!dominated && i < non_dominated_partitions.size()) {
          Partition p2 = non_dominated_partitions.get(i);
          dominated = skylineDominate(p2, p, dir, gIndex.isCompact());
          
          // Check if the new partition dominates the previously selected one
          if (skylineDominate(p, p2, dir, gIndex.isCompact())) {
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
      
      LOG.info("Processing "+non_dominated_partitions.size()+" out of "+gIndex.size()+" partition");
      // Output all non-dominated partitions
      for (Partition p : non_dominated_partitions) {
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
  
  public static class SkylineReducer extends MapReduceBase implements
  Reducer<NullWritable,Point,NullWritable,Point> {
    
    private Direction dir;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      dir = OperationsParams.getDirection(job, "dir", Direction.MaxMax);
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
  
  private static void skylineMapReduce(Path inFile, Path userOutPath,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, Skyline.class);
    Path outPath = userOutPath;
    FileSystem outFs = (userOutPath == null ? inFile : userOutPath).getFileSystem(job);
    
    if (outPath == null) {
      do {
        outPath = new Path(inFile.toUri().getPath()+
            ".skyline_"+(int)(Math.random() * 1000000));
      } while (outFs.exists(outPath));
    }
    
    job.setJobName("Skyline");
    job.setClass(SpatialSite.FilterClass, SkylineFilter.class, BlockFilter.class);
    job.setMapperClass(IdentityMapper.class);
    job.setCombinerClass(SkylineReducer.class);
    job.setReducerClass(SkylineReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Point.class);
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    job.setOutputFormat(GridOutputFormat2.class);
    GridOutputFormat2.setOutputPath(job, outPath);
    
    JobClient.runJob(job);
    
    // If outputPath not set by user, automatically delete it
    if (userOutPath == null)
      outFs.delete(outPath, true);
  }
  
  public static void skyline(Path inFile, Path outFile, OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, FileMBR.class);
    FileInputFormat.addInputPath(job, inFile);
    ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();

    boolean autoLocal = inputFormat.getSplits(job, 1).length <= 3;
    boolean isLocal = params.is("local", autoLocal);
    
    if (!isLocal) {
      // Process with MapReduce
      skylineMapReduce(inFile, outFile, params);
    } else {
      // Process without MapReduce
      skylineLocal(inFile, outFile, params);
    }
  }
  
  private static void printUsage() {
    System.err.println("Computes the skyline of an input file of points");
    System.err.println("Parameters: (* marks required parameters)");
    System.err.println("<input file>: (*) Path to input file");
    System.err.println("<output file>: Path to output file");
    System.err.println("<direction (max-max|max-min|min-max|min-min)>: Direction of skyline (default is max-max)");
    System.err.println("-overwrite: Overwrite output file without notice");
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] paths = params.getPaths();
    if (paths.length <= 1 && !params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    if (paths.length >= 2 && !params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    Path inFile = params.getInputPath();
    Path outFile = params.getOutputPath();
    
    long t1 = System.currentTimeMillis();
    skyline(inFile, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
  
}
