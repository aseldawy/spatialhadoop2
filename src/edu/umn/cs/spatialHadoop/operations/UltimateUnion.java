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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms.RectangleID;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;

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
   * Union a set of geometries by combining them into one GeometryCollection
   * and taking its buffer
   * @param shapes
   * @return
   */
  public static Geometry unionUsingBuffer(List<Geometry> shapes) {
    List<Geometry> basicShapes = new Vector<Geometry>();
    for (int i = 0; i < shapes.size(); i++) {
      Geometry geom = shapes.get(i);
      if (geom instanceof GeometryCollection) {
        GeometryCollection coll = (GeometryCollection) geom;
        for (int n = 0; n < coll.getNumGeometries(); n++)
          shapes.add(coll.getGeometryN(n));
      } else {
        basicShapes.add(geom);
      }
      shapes.set(i, null);
    }
    
    GeometryCollection allInOne = FACTORY.createGeometryCollection(
        basicShapes.toArray(new Geometry[basicShapes.size()]));
    
    return allInOne.buffer(0);
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
  static class UltimateUnionMap<S extends OGCJTSShape> extends 
      Mapper<Rectangle, Iterable<Shape>, NullWritable, Shape> {
    
    @Override
    protected void map(Rectangle key, Iterable<Shape> shapes, Context context)
        throws IOException, InterruptedException {
      long t1 = System.currentTimeMillis();
      OGCJTSShape templateShape = null;
      Vector<Geometry> vgeoms = new Vector<Geometry>();
      Vector<RectangleID> rects = new Vector<RectangleID>();
      for (Shape s : shapes) {
        if (templateShape == null)
          templateShape = (OGCJTSShape) s;
        vgeoms.add(((OGCJTSShape)s).geom);
        rects.add(new RectangleID(rects.size(), s.getMBR()));
      }
      
      RectangleID[] mbrs = rects.toArray(new RectangleID[rects.size()]);
      rects = null;
      // Parent link of the Set Union Find data structure
      final int[] parent = new int[mbrs.length];
      Arrays.fill(parent, -1);
      
      // Group records in clusters by overlapping
      SpatialAlgorithms.SelfJoin_rectangles(mbrs, new OutputCollector<RectangleID, RectangleID>(){
        @Override
        public void collect(RectangleID r, RectangleID s)
            throws IOException {
          int rid = r.id;
          while (parent[rid] != -1) {
            int pid = parent[rid];
            if (parent[pid] != -1)
              parent[rid] = parent[pid];
            rid = pid;
          }
          int sid = s.id;
          while (parent[sid] != -1) {
            int pid = parent[sid];
            if (parent[pid] != -1)
              parent[sid] = parent[pid];
            sid = pid;
          }
          if (rid != sid)
            parent[rid] = sid;
        }}, context);
      mbrs = null;
      // Put all records in one cluster as a list
      Map<Integer, List<Geometry>> groups = new HashMap<Integer, List<Geometry>>();
      for (int i = 0; i < parent.length; i++) {
        int root = parent[i];
        if (root == -1)
          root = i;
        while (parent[root] != -1) {
          root = parent[root];
        }
        List<Geometry> group = groups.get(root);
        if (group == null) {
          group = new Vector<Geometry>();
          groups.put(root, group);
        }
        group.add(vgeoms.get(i));
      }
      // Early clean some memory
      vgeoms = null;
      long t2 = System.currentTimeMillis();
      LOG.info("Grouped "+parent.length+" shapes into "+groups.size()+" clusters in "+(t2-t1)/1000.0+" seconds");
      
      // Compute a separate union for each cluster
      t1 = System.currentTimeMillis();
      NullWritable nullKey = NullWritable.get();
      Coordinate[] coords = new Coordinate[5];
      coords[0] = new Coordinate(key.x1, key.y1);
      coords[1] = new Coordinate(key.x2, key.y1);
      coords[2] = new Coordinate(key.x2, key.y2);
      coords[3] = new Coordinate(key.x1, key.y2);
      coords[4] = coords[0];
      Geometry partitionMBR = FACTORY.createPolygon(FACTORY.createLinearRing(coords), null);

      for (List<Geometry> group : groups.values()) {
        Geometry theUnion = unionUsingBuffer(group);
        context.progress();
        if (theUnion != null) {
          Geometry croppedUnion = theUnion.getBoundary().intersection(partitionMBR);
          if (croppedUnion != null) {
            templateShape.geom = croppedUnion;
            context.write(nullKey, templateShape);
          }
        }
      }
      t2 = System.currentTimeMillis();
      LOG.info("Computed the union in "+(t2-t1)/1000.0+" seconds");
    }
  }
  
  private static Job ultimateUnionMapReduce(Path input, Path output,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = new Job(params, "UltimateUnion");
    Configuration jobConf = job.getConfiguration();
    job.setJarByClass(UltimateUnion.class);

    Shape shape = params.getShape("shape");
    // Set map and reduce
    job.setMapperClass(UltimateUnionMap.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setNumReduceTasks(0);

    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.addInputPath(job, input);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // Submit the job
    if (!params.getBoolean("background", false)) {
      job.waitForCompletion(false);
    } else {
      job.submit();
    }
    return job;
  }

  public static Job ultimateUnion(Path input, Path output,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    return ultimateUnionMapReduce(input, output, params);
  }

  private static void printUsage() {
    System.out.println("Ultimate Union");
    System.out.println("Finds the union of all shapes in the input file.");
    System.out.println("The output is one shape that represents the union of all shapes in input file.");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to file that contains all shapes");
    System.out.println("<output file>: (*) Path to output file.");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
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
