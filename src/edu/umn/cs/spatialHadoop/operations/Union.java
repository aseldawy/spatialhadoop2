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
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;

/**
 * Computes the union of a set of shapes using a distributed MapReduce program.
 * The file is split into n partitions, the union of each partition is computed
 * separately, and finally the results are merged into one reducer. 
 * @author Ahmed Eldawy
 *
 */
public class Union {
  public static final GeometryFactory FACTORY = new GeometryFactory();
  
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(Union.class);

  /**
   * The map function for the BasicUnion algorithm which works on a set of
   * shapes. It computes the union of all these shapes and writes the result
   * to the output.
   * @author Ahmed Eldawy
   *
   * @param <S>
   */
  static class UnionMap<S extends OGCJTSShape> extends 
      Mapper<Rectangle, Iterable<S>, NullWritable, Shape> {
    
    @Override
    protected void map(Rectangle dummy, Iterable<S> shapes, Context context)
        throws IOException, InterruptedException {
      S templateShape = null;
      Vector<Geometry> vgeoms = new Vector<Geometry>();
      Iterator<S> i = shapes.iterator();
      while (i.hasNext()) {
        templateShape = i.next();
        if (templateShape.geom != null)
          vgeoms.add(templateShape.geom);
      }

      LOG.info("Computing the union of "+vgeoms.size()+" geoms");
      Geometry theUnion = UltimateUnion.unionUsingBuffer(vgeoms, context);
      templateShape.geom = theUnion;
      context.write(NullWritable.get(), templateShape);
      LOG.info("Union computed");
    }
  }
  
  static class UnionReduce<S extends OGCJTSShape> extends
    Reducer<NullWritable, S, NullWritable, S> {
    
    @Override
    protected void reduce(NullWritable dummy, Iterable<S> shapes,
        Context context) throws IOException, InterruptedException {
      S templateShape = null;
      Vector<Geometry> vgeoms = new Vector<Geometry>();
      Iterator<S> i = shapes.iterator();
      while (i.hasNext()) {
        templateShape = i.next();
        vgeoms.add(templateShape.geom);
      }

      LOG.info("Computing the union of "+vgeoms.size()+" geoms");
      Geometry theUnion = UltimateUnion.unionUsingBuffer(vgeoms, context);
      templateShape.geom = theUnion;
      context.write(dummy, templateShape);
      LOG.info("Union computed");
    }
  }
  
  private static Job ultimateUnionMapReduce(Path input, Path output,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = new Job(params, "BasicUnion");
    job.setJarByClass(Union.class);

    Shape shape = params.getShape("shape");
    // Set map and reduce
    job.setMapperClass(UnionMap.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setCombinerClass(UnionReduce.class);
    job.setReducerClass(UnionReduce.class);
    job.setNumReduceTasks(1);

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
    System.out.println("Union");
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
