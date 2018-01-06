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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.TopologyException;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.util.Progressable;

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
  static final Log LOG = LogFactory.getLog(UltimateUnion.class);

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
      Mapper<Rectangle, Iterable<Shape>, NullWritable, OGCJTSShape> {
    
    @Override
    protected void map(Rectangle key, Iterable<Shape> shapes, final Context context)
        throws IOException, InterruptedException {
      List<Geometry> vgeoms = new ArrayList<Geometry>();
      for (Shape s : shapes)
        vgeoms.add(((OGCJTSShape) s).geom);
      Geometry[] geoms = vgeoms.toArray(new Geometry[vgeoms.size()]);
      
      // Compute the union and clip to the partition MBR
      Coordinate[] coords = new Coordinate[5];
      coords[0] = new Coordinate(key.x1, key.y1);
      coords[1] = new Coordinate(key.x2, key.y1);
      coords[2] = new Coordinate(key.x2, key.y2);
      coords[3] = new Coordinate(key.x1, key.y2);
      coords[4] = coords[0];
      final Geometry partitionMBR = FACTORY.createPolygon(FACTORY.createLinearRing(coords), null);
      
      ResultCollector<Geometry> resultCollector = new ResultCollector<Geometry>() {
        NullWritable nullKey = NullWritable.get();
        OGCJTSShape value = new OGCJTSShape();
        
        @Override
        public void collect(Geometry r) {
          try {
            Geometry croppedUnion = r.getBoundary().intersection(partitionMBR);
            if (croppedUnion != null) {
              value.geom = croppedUnion;
              context.write(nullKey, value);
            }
          } catch (TopologyException e) {
            LOG.warn("Error in cropping", e);
            // Unavoidable TopologyException. Skip the clipping just to ensure
            // we write something to the output
            value.geom = r;
            try {
              context.write(nullKey, value);
            } catch (IOException e1) {
              e1.printStackTrace();
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      
      SpatialAlgorithms.multiUnion(geoms,
          new Progressable.TaskProgressable(context), resultCollector);
    }
  }
  
  private static Job ultimateUnionMapReduce(Path input, Path output,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = new Job(params, "UltimateUnion");
    job.setJarByClass(UltimateUnion.class);

    // Set map and reduce
    job.setMapperClass(UltimateUnionMap.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(OGCJTSShape.class);
    job.setNumReduceTasks(0);

    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.addInputPath(job, input);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);

    // Submit the job
    if (!params.getBoolean("background", false)) {
      job.waitForCompletion(false);
      if (!job.isSuccessful())
        throw new RuntimeException("Job failed!");
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
