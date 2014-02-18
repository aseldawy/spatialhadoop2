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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.vividsolutions.jts.geom.Geometry;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.OSMPolygon;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.operations.DistributedJoin.DJInputFormatArray;
import edu.umn.cs.spatialHadoop.operations.DistributedJoin.DJInputFormatRTree;
import edu.umn.cs.spatialHadoop.operations.DistributedJoin.SpatialJoinFilter;

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
   * Computes the union between the given shape with all overlapping shapes
   * and return only the segments in the result that overlap with the shape.
   * 
   * @param shape
   * @param overlappingShapes
   * @return
   */
  public static Geometry partialUnion(Geometry shape, Collection<Geometry> overlappingShapes) {
    return null;
  }
  
  static class UltimateUnionReducer extends MapReduceBase implements
      Reducer<OSMPolygon, OSMPolygon, NullWritable, OSMPolygon> {

    @Override
    public void reduce(OSMPolygon shape, Iterator<OSMPolygon> overlaps,
        OutputCollector<NullWritable, OSMPolygon> output, Reporter reporter)
        throws IOException {
      Vector<Geometry> overlappingShapes = new Vector<Geometry>();
      while (overlaps.hasNext()) {
        OSMPolygon overlap = overlaps.next();
        overlappingShapes.add(overlap.geom);
      }
      Geometry result = partialUnion(shape.geom, overlappingShapes);
      if (result != null)
        output.collect(NullWritable.get(), new OSMPolygon(result));
    }
  }
  
  private static void ultimateUnionMapReduce(Path input, Path output,
      Shape shape) throws IOException {
    JobConf job = new JobConf(Union.class);
    job.setJobName("UltimateUnion");

    // Set map and reduce
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks() * 9 / 10));

    job.setMapperClass(DistributedJoin.RedistributeJoinMap.class);
    job.setReducerClass(UltimateUnionReducer.class);
    
    job.setMapOutputKeyClass(shape.getClass());
    job.setMapOutputValueClass(shape.getClass());

    // Set input and output
    FileSystem fs = input.getFileSystem(job);
    if (SpatialSite.isRTree(fs, input)) {
      job.setInputFormat(DJInputFormatRTree.class);
    } else {
      job.setInputFormat(DJInputFormatArray.class);
    }
    FileInputFormat.addInputPath(job, input);
    FileInputFormat.addInputPath(job, input);
    job.setClass(SpatialSite.FilterClass, SpatialJoinFilter.class, BlockFilter.class);
    SpatialSite.setShapeClass(job, shape.getClass());
    
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
    Shape shape = new OSMPolygon();
    
    ultimateUnion(input, output, shape);
  }
}
