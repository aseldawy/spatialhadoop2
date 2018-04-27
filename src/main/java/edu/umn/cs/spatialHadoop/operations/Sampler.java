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

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapreduce.SampleInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;


/**
 * Reads a sample out of a set of files using MapReduce
 * @author Ahmed Eldawy
 *
 */
@OperationMetadata(shortName = "sample",
    description = "Reads a random sample from a text file")
public class Sampler {
  private static final Log LOG = LogFactory.getLog(Sampler.class);

  private static final String ConverterClass = "Sampler.Converter";

  interface TextConverter {
    void setup(Configuration conf);

    /**
     * Convert the given text value in place. The return value indicates whether
     * the conversion was successful or not. If the conversion is not successful,
     * e.g., the input is in an incorrect format, the caller should discard the
     * text value.
     * @param inout
     * @return {@code true} iff the conversion was succesfful
     */
    boolean convert(Text inout);
  }

  static class Identity implements TextConverter {

    @Override public void setup(Configuration conf) { }
    @Override public boolean convert(Text inout) { return true; }
  }

  static abstract class ShapeConverter implements TextConverter {
    Shape shape;

    @Override public void setup(Configuration conf) {
      shape = OperationsParams.getShape(conf, "shape");
    }

    @Override public boolean convert(Text inout) {
      shape.fromText(inout);
      return true;
    }
  }

  static class PointConverter extends ShapeConverter {
    Point pt = new Point();

    @Override public boolean convert(Text inout) {
      super.convert(inout);
      if (shape instanceof Point) {
        Point ptshape = (Point) shape;
        pt.set(ptshape.x, ptshape.y);
      } else if (shape instanceof Rectangle) {
        Rectangle rect = (Rectangle) shape;
        pt.set((rect.x1+rect.x2)/2, (rect.y1+rect.y2)/2);
      } else {
        Rectangle mbr = shape.getMBR();
        inout.clear();
        // If no MBR, skip this record
        if (mbr == null)
          return false;
        pt.set((mbr.x1 + mbr.x2) / 2, (mbr.y1 + mbr.y2) / 2);
      }
      pt.toText(inout);
      return true;
    }
  }

  public static class ConverterMap
      extends Mapper<Object, Text, Object, Text> {

    /**To convert samples records on the fly*/
    private TextConverter converter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      Configuration conf = context.getConfiguration();
      Class<? extends TextConverter> convClass = conf.getClass(ConverterClass, Identity.class, TextConverter.class);
      try {
        this.converter = convClass.newInstance();
        this.converter.setup(conf);
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      if (converter.convert(value))
        context.write(key, value);
    }
  }

  public static Job sampleMapReduce(Path[] files, Path output, OperationsParams params) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(params, "Sampler");
    job.setJarByClass(Sampler.class);

    // Set input and output
    job.setInputFormatClass(SampleInputFormat.class);
    SampleInputFormat.setInputPaths(job, files);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);
    
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(ConverterMap.class);
    job.setNumReduceTasks(0); // No reducer needed

    Class<? extends Shape> outShapeClass = SpatialSite.getShape(params.get("outshape"));
    if (outShapeClass == Point.class) {
      job.getConfiguration().setClass(ConverterClass, PointConverter.class, TextConverter.class);
    }

    // Use multithreading in case the job is running locally
    job.getConfiguration().setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());

    // Start the job
    if (params.getBoolean("background", false)) {
      // Run in background
      job.submit();
    } else {
      job.waitForCompletion(params.getBoolean("verbose", false));
    }
    return job;
  }

  public static String[] takeSample(Path[] files, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
    FileSystem fs = files[0].getFileSystem(params);
    Path tempPath;
    do {
      tempPath = new Path(String.format("temp_%06d", (int)(Math.random()*1000000)));
    } while (fs.exists(tempPath));
    Job job = sampleMapReduce(files, tempPath, params);
    job.waitForCompletion(false);
    int outputSize = (int) job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();

    // Read the file back
    String[] lines =  Head.head(fs, tempPath, outputSize);
    // Delete the temporary path with all its contents
    fs.delete(tempPath, true);

    return lines;
  }
  
  private static void printUsage() {
    System.out.println("Reads a random sample of an input file. Sample is written to the output path.");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("shape:<s> - Type of shapes stored in the file");
    System.out.println("outshape:<s> - Shapes to write to output. If not specified, sampled records are not converted.");
    System.out.println("ratio:<r> - ratio of random sample to read [0, 1]");
    System.out.println("seed:<s> - random seed to use while reading the sample");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * @param args
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    
    Path[] input = params.getInputPaths();
    Path output = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    sampleMapReduce(input, output, params);
    long t2 = System.currentTimeMillis();
    
    System.out.println("Total time for sampling "+(t2-t1)+" millis");
  }

}
