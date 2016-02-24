/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat2;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.MemoryReporter;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

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
  public static <P extends Point> P[] convexHullInMemory(P[] points) {
    Stack<P> s1 = new Stack<P>();
    Stack<P> s2 = new Stack<P>();
    
    Arrays.sort(points);
    
    // Lower chain
    for (int i=0; i<points.length; i++) {
      while(s1.size() > 1) {
        P p1 = s1.get(s1.size() - 2);
        P p2 = s1.get(s1.size() - 1);
        P p3 = points[i];
        double crossProduct = (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x);
        if (crossProduct <= 0) s1.pop();
        else break;
      }
      s1.push(points[i]);
    }
    
    // Upper chain
    for (int i=points.length - 1; i>=0; i--) {
      while(s2.size() > 1) {
        P p1 = s2.get(s2.size() - 2);
        P p2 = s2.get(s2.size() - 1);
        P p3 = points[i];
        double crossProduct = (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x);
        if (crossProduct <= 0) s2.pop();
        else break;
      }
      s2.push(points[i]);
    }
    
    s1.pop();
    s2.pop();
    s1.addAll(s2);
    return s1.toArray((P[]) Array.newInstance(s1.firstElement().getClass(), s1.size()));    
  }
  
  /**
   * Computes the convex hull of an input file using a single machine algorithm.
   * The output is written to the output file. If output file is null, the
   * output is just thrown away.
   * @param inFile
   * @param outFile
   * @param params
   * @throws IOException
   * @throws InterruptedException
   */
  public static void convexHullLocal(Path inFile, Path outFile,
      final OperationsParams params) throws IOException, InterruptedException {
    if (params.getBoolean("mem", false))
      MemoryReporter.startReporting();
    // 1- Split the input path/file to get splits that can be processed
    // independently
    final SpatialInputFormat3<Rectangle, Point> inputFormat =
        new SpatialInputFormat3<Rectangle, Point>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inFile);
    final List<InputSplit> splits = inputFormat.getSplits(job);
    
    // 2- Read all input points in memory
    LOG.info("Reading points from "+splits.size()+" splits");
    List<Point[]> allLists = Parallel.forEach(splits.size(), new RunnableRange<Point[]>() {
      @Override
      public Point[] run(int i1, int i2) {
        try {
          List<Point> finalPoints = new ArrayList<Point>();
          final int MaxSize = 100000;
          Point[] points = new Point[MaxSize];
          int size = 0;
          for (int i = i1; i < i2; i++) {
            org.apache.hadoop.mapreduce.lib.input.FileSplit fsplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit) splits.get(i);
            final RecordReader<Rectangle, Iterable<Point>> reader =
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
                points[size++] = p.clone();
                if (size >= points.length) {
                  // Perform convex hull and write the result to finalPoints
                  Point[] chPoints = convexHullInMemory(points);
                  for (Point skylinePoint : chPoints)
                    finalPoints.add(skylinePoint);
                  size = 0; // reset
                }
              }
            }
            reader.close();
          }
          while (size-- > 0)
            finalPoints.add(points[size]);
          return finalPoints.toArray(new Point[finalPoints.size()]);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    }, params.getInt("parallel", Runtime.getRuntime().availableProcessors()));
    
    int totalNumPoints = 0;
    for (Point[] list : allLists)
      totalNumPoints += list.length;
    
    LOG.info("Read "+totalNumPoints+" points and merging into one list");
    Point[] allPoints = new Point[totalNumPoints];
    int pointer = 0;
    
    for (Point[] list : allLists) {
      System.arraycopy(list, 0, allPoints, pointer, list.length);
      pointer += list.length;
    }
    allLists.clear(); // To the let the GC collect it
    
    Point[] ch = convexHullInMemory(allPoints);

    if (outFile != null) {
      if (params.getBoolean("overwrite", false)) {
        FileSystem outFs = outFile.getFileSystem(new Configuration());
        outFs.delete(outFile, true);
      }
      GridRecordWriter<Point> out = new GridRecordWriter<Point>(outFile, null, null, null);
      for (Point pt : ch) {
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
      for (OperationsParams.Direction dir : OperationsParams.Direction.values()) {
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
      System.out.println("Processing "+non_dominated_partitions_all.size()+" out of "+gIndex.size()+" partition");
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
      Point[] convex_hull = convexHullInMemory(vpoints.toArray(new Point[vpoints.size()]));
      for (Point pt : convex_hull) {
        output.collect(dummy, pt);
      }
    }
  }
  
  public static void convexHullMapReduce(Path inFile, Path userOutPath,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, ConvexHull.class);
    Path outPath = userOutPath;
    FileSystem outFs = (userOutPath == null ? inFile : userOutPath).getFileSystem(job);
    Shape shape = params.getShape("shape");
    
    if (outPath == null) {
      do {
        outPath = new Path(inFile.toUri().getPath()+
            ".convex_hull_"+(int)(Math.random() * 1000000));
      } while (outFs.exists(outPath));
    } else {
      if (outFs.exists(outPath)) {
        if (params.getBoolean("overwrite", false)) {
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
    job.setOutputValueClass(shape.getClass());
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    job.setOutputFormat(GridOutputFormat2.class);
    GridOutputFormat2.setOutputPath(job, outPath);
    
    JobClient.runJob(job);
   
    // If outputPath not set by user, automatically delete it
    if (userOutPath == null)
      outFs.delete(outPath, true);
  }
  
  private static void printUsage() {
    System.out.println("Computes the convex hull of an input file of shapes");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
    System.out.println("<output file>: Path to output file");
    System.out.println("-overwrite: Overwrite output file without notice");
    
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  public static void convexHull(Path inFile, Path outFile, OperationsParams params) throws IOException, InterruptedException {
    if (OperationsParams.isLocal(params, inFile)) {
      // Process without MapReduce
      convexHullLocal(inFile, outFile, params);
    } else {
      // Process with MapReduce
      convexHullMapReduce(inFile, outFile, params);
    }
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
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
    convexHull(inFile, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
  
}
