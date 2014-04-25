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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.TopologyException;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * Computes the union of a set of shapes using a distributed MapReduce program.
 * First, a self join is carried out to relate each shape with all overlapping
 * shapes. Then, 
 * @author Ahmed Eldawy
 *
 */
public class UltimateUnion {
  public static final GeometryFactory FACTORY = new GeometryFactory();
  
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(UltimateUnion.class);

  /**
   * Computes the union of all given shapes and return one shape with all
   * segments on the boundaries of the union. This method computes the union
   * using the buffer(0) method which should be faster but is not stable.
   * Sometimes, it breaks with input polygons of very high precision.
   * @param collections
   * @return
   */
  private static Geometry combineIntoOneGeometryBroken(Collection<Geometry> collections) {
    GeometryFactory factory = new GeometryFactory();
    GeometryCollection geometryCollection = (GeometryCollection)factory.buildGeometry(collections);
    return geometryCollection.buffer(0);
  }
  
  /**
   * Computes the union of the given set of shapes. This method starts with
   * one polygon and adds other polygons one by one computing the union at
   * each step. This method is safer than using the buffer(0) method but it
   * might be slower.
   * @param collections
   * @return
   */
  private static Geometry combineIntoOneGeometrySafe(Collection<Geometry> collections) {
    Geometry res = null;
    for (Geometry g : collections)
      if (res == null) res = g;
      else res = res.union(g);
    return res;
  }
  
  /**
   * Similar to the {@link #combineIntoOneGeometrySafe(Collection)}, it computes
   * the union of a set of shapes. However, it keeps doing pair-wise union
   * between every pair of consecutive shapes (in the given list order) until
   * we end up with one polygon. This method could be faster if we can perform
   * independent pair-wise unions in parallel.
   * @param collections
   * @return
   */
  private static Geometry combineIntoOneGeometryFast(Collection<Geometry> collections) {
    Geometry[] input = collections.toArray(new Geometry[collections.size()]);
    int n = input.length; 
    while(n > 1) {
      int cnt = 0;
      for (int i=0; i<n; i+=2) 
        if (i + 1 < n) input[cnt++] = input[i].union(input[i + 1]);
        else input[cnt++] = input[i];
      n = cnt;
    }
    return input[0];
  }

  /**
   * Computes the union between the given shape with all overlapping shapes
   * and return only the segments in the result that overlap with the shape.
   * 
   * @param shape
   * @param overlappingShapes
   * @return
   */
  public static Geometry partialUnion(Geometry shape, Collection<Geometry> overlappingShapes) {
    Geometry partialResult;
    try {
      partialResult = shape.union(combineIntoOneGeometryBroken(overlappingShapes));
    } catch (TopologyException e) {
      LOG.warn("Error computing union");
      partialResult = shape.union(combineIntoOneGeometrySafe(overlappingShapes));
    }
    return shape.getBoundary().intersection(partialResult.getBoundary());
  }
  
  /**
   * A second version of partial union that takes a partition boundary along
   * with all shapes overlapping this partition and returns the part of the
   * answer that falls within the boundaries of the given partition.
   * 
   * @param partition
   * @param shapes
   * @return
   */
  public static Geometry partialUnion(Rectangle partition, Collection<Geometry> shapes) {
    Geometry shapesUnion;
    try {
      shapesUnion = combineIntoOneGeometryFast(shapes);
      Coordinate[] coords = new Coordinate[5];
      coords[0] = new Coordinate(partition.x1, partition.y1);
      coords[1] = new Coordinate(partition.x2, partition.y1);
      coords[2] = new Coordinate(partition.x2, partition.y2);
      coords[3] = new Coordinate(partition.x1, partition.y2);
      coords[4] = coords[0];
      Geometry grid = FACTORY.createPolygon(FACTORY.createLinearRing(coords), null);

      Geometry croppedUnion = shapesUnion.getBoundary().intersection(grid);
      return croppedUnion;
    } catch (TopologyException e) {
      LOG.warn("Error computing parial union", e);
      return null;
    }
  }

  /**
   * The map function for the UltimateUnion algorithm which works on a cell
   * level. It takes all shapes in a rectangular cell, and returns the portion
   * of the union that is contained in this cell. The output is of type
   * MultiLineString and contains the lines that is part of the final result
   * and contained in the given cell.
   * @author Ahmed Eldawy
   *
   * @param <S>
   */
  static class UltimateUnionMapInCell<S extends OGCJTSShape> extends MapReduceBase implements
      Mapper<Rectangle, ArrayWritable, NullWritable, Shape>{

    @Override
    public void map(Rectangle key, ArrayWritable value,
        final OutputCollector<NullWritable, Shape> output, Reporter reporter) throws IOException {
      ArrayList<Geometry> geoms = new ArrayList<Geometry>();
      Shape[] objects = (Shape[])value.get();
      for (Shape s : objects) {
        geoms.add(((OGCJTSShape)s).geom);
      }
      Geometry result = partialUnion(key, geoms);
      if (result != null)
        output.collect(NullWritable.get(), new OGCJTSShape(result));
    }
  }
  
  /**
   * The map function of the UltimateUnion algorithm which works on a shape
   * level. It takes a set of shapes, and performs a self join on these shapes
   * to relate each shape to all overlapping shapes. Later in the reduce
   * function, the union of each shape with all overlapping shapes is computed
   * and the parts that contribute to final answer is computed.
   * @author Ahmed Eldawy
   *
   */
  static class UltimateUnionMapPerShape extends MapReduceBase
      implements Mapper<Rectangle, ArrayWritable, Shape, Shape> {
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
    }

    @Override
    public void map(Rectangle key, ArrayWritable values,
        OutputCollector<Shape, Shape> output, Reporter reporter)
        throws IOException {
      // Perform a self spatial join to relate each shape with all overlapping
      // shapes. Notice that we run a filter-only join for efficiency which
      // might relate a shape to another disjoint shape. These extra shapes
      // will be handled and avoided in the reduce step.
      Shape[] shapes = (Shape[]) values.get();
      SpatialAlgorithms.SelfJoin_planeSweep(shapes, false, output);
    }
    
  }
  
  /**
   * The reduce function of the UltimateUnion algorithm which runs on a shape
   * level. It takes a shape with all overlapping shapes and performs a polygon
   * union algorithm over all these polygons. However, instead of returning the
   * union, it returns only the parts that overlap the shape.
   * @author Ahmed Eldawy
   *
   */
  static class UltimateUnionReducer extends MapReduceBase implements
      Reducer<OGCJTSShape, OGCJTSShape, NullWritable, OGCJTSShape> {

    @Override
    public void reduce(OGCJTSShape shape, Iterator<OGCJTSShape> overlaps,
        OutputCollector<NullWritable, OGCJTSShape> output, Reporter reporter)
        throws IOException {
      Vector<Geometry> overlappingShapes = new Vector<Geometry>();
      while (overlaps.hasNext()) {
        OGCJTSShape overlap = overlaps.next();
        overlappingShapes.add(overlap.geom);
      }
      Geometry result = partialUnion(shape.geom, overlappingShapes);
      if (result != null)
        output.collect(NullWritable.get(), new OGCJTSShape(result));
    }
  }
  
  private static void ultimateUnionMapReduce(Path input, Path output,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, UltimateUnion.class);
    job.setJobName("UltimateUnion");

    String method = params.get("method", "cell").toLowerCase();
    Shape shape = params.getShape("shape");
    if (method.equals("cell")) {
      // Set map and reduce
      job.setNumReduceTasks(0);
      
      job.setMapperClass(UltimateUnionMapInCell.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(shape.getClass());
    } else if (method.equals("shape")) {
      // Set map and reduce
      ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
      job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks() * 9 / 10));
      
      job.setMapperClass(UltimateUnionMapPerShape.class);
      job.setReducerClass(UltimateUnionReducer.class);
      job.setMapOutputKeyClass(shape.getClass());
      job.setMapOutputValueClass(shape.getClass());
    } else {
      throw new RuntimeException("Unknown partition method: '"+method+"'");
    }

    // Set input and output
    job.setInputFormat(ShapeArrayInputFormat.class);
    FileInputFormat.addInputPath(job, input);
    // Ensure each partition is fully read in one shot for correctness
    job.setInt(SpatialSite.MaxBytesInOneRead, -1);
    job.setInt(SpatialSite.MaxShapesInOneRead, -1);

    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // Start job
    JobClient.runJob(job);
  }

  public static void ultimateUnion(Path input, Path output, OperationsParams params) throws IOException {
    ultimateUnionMapReduce(input, output, params);
  }

  private static void printUsage() {
    System.out.println("Ultimate Union");
    System.out.println("Finds the union of all shapes in the input file.");
    System.out.println("The output is one shape that represents the union of all shapes in input file.");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to file that contains all shapes");
    System.out.println("<output file>: (*) Path to output file.");
  }

  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    
    if (!params.checkInputOutput()) {
      printUsage();
      return;
    }
    
    Path input = params.getPath();
    Path output = params.getPaths()[1];
    Shape shape = params.getShape("shape");
    
    if (shape == null || !(shape instanceof OGCJTSShape)) {
      LOG.error("Given shape must be a subclass of "+OGCJTSShape.class);
      return;
    }

    long t1 = System.currentTimeMillis();
    ultimateUnion(input, output, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
}
