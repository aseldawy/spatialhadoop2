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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.util.AssertionFailedException;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.JTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * Computes the union of a set of shapes using a distributed MapReduce program.
 * First, a self join is carried out to relate each shape with all overlapping
 * shapes. Then, 
 * @author Ahmed Eldawy
 *
 */
public class UltimateUnion {
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
    try {
      GeometryFactory factory = new GeometryFactory();
      GeometryCollection geometryCollection = (GeometryCollection)factory.buildGeometry(collections);
      return geometryCollection.buffer(0);
    } catch (AssertionFailedException e) {
      e.printStackTrace();
      System.out.println(collections);
      throw e;
    }
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
    Geometry partialResult = shape.intersection(combineIntoOneGeometrySafe(overlappingShapes));
    return shape.getBoundary().intersection(partialResult.getBoundary());
  }

  static class UltimateUnionMap<S extends JTSShape> extends MapReduceBase implements
      Mapper<Rectangle, ArrayWritable, Shape, Shape>{

    @Override
    public void map(Rectangle key, ArrayWritable value,
        final OutputCollector<Shape, Shape> output, Reporter reporter) throws IOException {
      Shape[] objects = (Shape[])value.get();
      SpatialAlgorithms.SelfJoin_planeSweep(objects, output);
    }
    
  }
  
  static class UltimateUnionReducer extends MapReduceBase implements
      Reducer<JTSShape, JTSShape, NullWritable, JTSShape> {

    @Override
    public void reduce(JTSShape shape, Iterator<JTSShape> overlaps,
        OutputCollector<NullWritable, JTSShape> output, Reporter reporter)
        throws IOException {
      Vector<Geometry> overlappingShapes = new Vector<Geometry>();
      while (overlaps.hasNext()) {
        JTSShape overlap = overlaps.next();
        overlappingShapes.add(overlap.geom);
      }
      Geometry result = partialUnion(shape.geom, overlappingShapes);
      if (result != null)
        output.collect(NullWritable.get(), new JTSShape(result));
    }
  }
  
  private static void ultimateUnionMapReduce(Path input, Path output,
      Shape shape) throws IOException {
    JobConf job = new JobConf(Union.class);
    job.setJobName("UltimateUnion");

    // Set map and reduce
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks() * 9 / 10));

    job.setMapperClass(UltimateUnionMap.class);
    job.setReducerClass(UltimateUnionReducer.class);

    job.setMapOutputKeyClass(shape.getClass());
    job.setMapOutputValueClass(shape.getClass());

    // Set input and output
    job.setInputFormat(ShapeArrayInputFormat.class);
    FileInputFormat.addInputPath(job, input);
    SpatialSite.setShapeClass(job, shape.getClass());
    // Ensure each partition is fully read in one shot for correctness
    job.setInt(SpatialSite.MaxBytesInOneRead, -1);
    job.setInt(SpatialSite.MaxShapesInOneRead, -1);

    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // Start job
    JobClient.runJob(job);
  }

  public static void ultimateUnion(Path input, Path output, Shape shape) throws IOException {
    ultimateUnionMapReduce(input, output, shape);
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
    CommandLineArguments params = new CommandLineArguments(args);
    
    if (!params.checkInputOutput()) {
      printUsage();
      return;
    }
    
    Path input = params.getPath();
    Path output = params.getPaths()[1];
    Shape shape = params.getShape("shape");
    
    if (shape == null || !(shape instanceof JTSShape)) {
      LOG.error("Given shape must be a subclass of "+JTSShape.class);
      return;
    }
    
    ultimateUnion(input, output, shape);
  }
}
