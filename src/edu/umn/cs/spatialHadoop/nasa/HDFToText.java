/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;

/**
 * This operation transforms one or more HDF files into text files which can
 * be used with other operations. Each point in the HDF file will be represented
 * as one line in the text output.
 * @author Ahmed Eldawy
 *
 */
public class HDFToText {
  public static class HDFToTextMap extends
      Mapper<NASADataset, Iterable<? extends NASAShape>, Rectangle, NASAShape> {
    
    @Override
    protected void map(
        NASADataset dataset,
        Iterable<? extends NASAShape> values,
        Context context)
        throws IOException, InterruptedException {
      for (NASAShape s : values)
        context.write(dataset, s);
    }
  }
  
  /**
   * Performs an HDF to text operation as a MapReduce job and returns total
   * number of points generated.
   * @param inPath
   * @param outPath
   * @param datasetName
   * @param skipFillValue
   * @return
   * @throws IOException
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   */
  public static long HDFToTextMapReduce(Path inPath, Path outPath,
      String datasetName, boolean skipFillValue) throws IOException,
      InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance();
    Configuration conf = job.getConfiguration();
    job.setJarByClass(HDFToText.class);
    job.setJobName("HDFToText");

    // Set Map function details
    job.setMapperClass(HDFToTextMap.class);
    job.setMapOutputKeyClass(Rectangle.class);
    job.setMapOutputValueClass(NASAPoint.class);
    job.setNumReduceTasks(0);
    
    // Set input information
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inPath);
    conf.setClass("shape", NASAPoint.class, Shape.class);
    conf.set("dataset", datasetName);
    conf.setBoolean("skipfillvalue", skipFillValue);
    
    // Set output information
    job.setOutputFormatClass(TextOutputFormat3.class);
    TextOutputFormat3.setOutputPath(job, outPath);
    
    // Run the job
    job.waitForCompletion(false);
    Counters counters = job.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();
    
    return resultCount;
  }

  private static void printUsage() {
    System.out.println("Converts a set of HDF files to text format");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
    System.out.println("<output file>: (*) Path to output file");
    System.out.println("dataset:<dataset>: (*) Name of the dataset to read from HDF");
    System.out.println("-skipfillvalue: Skip fill value");
  }
  /**
   * @param args
   * @throws IOException 
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] paths = params.getPaths();
    if (paths.length < 2) {
      printUsage();
      System.err.println("Please provide both input and output files");
      return;
    }
    Path inPath = paths[0];
    Path outPath = paths[1];
    
    FileSystem fs = inPath.getFileSystem(params);
    if (!fs.exists(inPath)) {
      printUsage();
      System.err.println("Input file does not exist");
      return;
    }
    
    boolean overwrite = params.getBoolean("overwrite", false);
    FileSystem outFs = outPath.getFileSystem(params);
    if (outFs.exists(outPath)) {
      if (overwrite)
        outFs.delete(outPath, true);
      else
        throw new RuntimeException("Output file exists and overwrite flag is not set");
    }

    String datasetName = params.get("dataset");
    if (datasetName == null) {
      printUsage();
      System.err.println("Please specify the dataset you want to extract");
      return;
    }
    boolean skipFillValue = params.getBoolean("skipfillvalue", true);
    
    HDFToTextMapReduce(inPath, outPath, datasetName, skipFillValue);
  }
}
