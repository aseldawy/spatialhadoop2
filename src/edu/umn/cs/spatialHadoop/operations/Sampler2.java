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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapreduce.SampleInputFormat;

/**
 * Reads a sample out of a set of files using MapReduce
 * @author Ahmed Eldawy
 *
 */
public class Sampler2 {
  private static final Log LOG = LogFactory.getLog(Sampler2.class);
  
  public static Job sampleMapReduce(Path[] files, Path output, OperationsParams params) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(params, "Sampler2");
    job.setJarByClass(Sampler2.class);

    // Set input and output
    job.setInputFormatClass(SampleInputFormat.class);
    SampleInputFormat.setInputPaths(job, files);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);
    
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(0); // No reducer needed
    
    // Start the job
    if (params.getBoolean("background", false)) {
      // Run in background
      job.submit();
    } else {
      job.waitForCompletion(params.getBoolean("verbose", false));
    }
    return job;
  }
  
  private static void printUsage() {
    System.out.println("Reads a random sample of an input file. Sample is written to stdout");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("shape:<s> - Type of shapes stored in the file");
    System.out.println("outshape:<s> - Shapes to write to output");
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
