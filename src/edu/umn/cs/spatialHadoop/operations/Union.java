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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

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
  
  private static Job unionMapReduce(Path input, Path output,
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
  
  public static <S extends OGCJTSShape> void unionLocal(Path inPath, Path outPath,
      final OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    // 1- Split the input path/file to get splits that can be processed independently
    final SpatialInputFormat3<Rectangle, S> inputFormat =
        new SpatialInputFormat3<Rectangle, S>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inPath);
    final List<InputSplit> splits = inputFormat.getSplits(job);
    int parallelism = params.getInt("parallel", Runtime.getRuntime().availableProcessors());
    
    // 2- Process splits in parallel
    Vector<Geometry> results = Parallel.forEach(splits.size(), new RunnableRange<Geometry>() {
      @Override
      public Geometry run(int i1, int i2) {
        final int batchSize = 10000;
        int numGeoms = 0;
        Geometry[] geoms = new Geometry[batchSize];
        Geometry unionResult = null;
        for (int i = i1; i < i2; i++) {
          try {
            FileSplit fsplit = (FileSplit) splits.get(i);
            final RecordReader<Rectangle, Iterable<S>> reader =
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
              Iterable<S> shapes = reader.getCurrentValue();
              for (S s : shapes) {
                geoms[numGeoms++] = s.geom;
                if (numGeoms >= batchSize) {
                  Geometry batchUnion = UltimateUnion.unionUsingBuffer(Arrays.asList(geoms), null);
                  if (unionResult == null)
                    unionResult = batchUnion;
                  else
                    unionResult = batchUnion.union(unionResult);
                }
              }
            }
            reader.close();
          } catch (IOException e) {
            LOG.error("Error processing split "+splits.get(i), e);
          } catch (InterruptedException e) {
            LOG.error("Error processing split "+splits.get(i), e);
          }
        }
        return unionResult;
      }
    }, parallelism);
    
    // Write result to output
    S outShape = (S) params.getShape("shape");
    Geometry finalResult = UltimateUnion.unionUsingBuffer(results, null);
    FileSystem outFS = outPath.getFileSystem(params);
    PrintStream out = new PrintStream(outFS.create(outPath));
    Text line = new Text2();
    if (finalResult instanceof GeometryCollection) {
      GeometryCollection coll = (GeometryCollection) finalResult;
      for (int i = 0; i < coll.getNumGeometries(); i++) {
        outShape.geom = coll.getGeometryN(i);
        line.clear();
        outShape.toText(line);
        out.println(line);
      }
    } else {
      outShape.geom = finalResult;
      outShape.toText(line);
      out.println(line);
    }
    out.close();
  }

  public static Job union(Path inPath, Path outPath,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    if (OperationsParams.isLocal(params, inPath)) {
      unionLocal(inPath, outPath, params);
      return null;
    } else {
      return unionMapReduce(inPath, outPath, params);
    }
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
    union(input, output, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
}
