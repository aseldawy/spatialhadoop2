/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.NASADataset;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;
import edu.umn.cs.spatialHadoop.nasa.NASAShape;

/**
 * Computes the minimum and maximum values for a set of input HDF files.
 * Used to find a tight range to plot heat maps.
 * @author Ahmed Eldawy
 *
 */
public class Aggregate {
  /**Logger*/
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(Aggregate.class);
  
  /**
   * A structure to hold the minimum and maximum values for aggregation.
   * @author Ahmed Eldawy
   *
   */
  public static class MinMax implements Writable, TextSerializable {
    public int minValue, maxValue;

    public MinMax() {
      minValue = Integer.MAX_VALUE;
      maxValue = Integer.MIN_VALUE;
    }
    
    public MinMax(int min, int max) {
      this.minValue = min;
      this.maxValue = max;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(minValue);
      out.writeInt(maxValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      minValue = in.readInt();
      maxValue = in.readInt();
    }
    
    @Override
    public String toString() {
      return "Value Range: "+minValue+", "+maxValue;
    }

    @Override
    public Text toText(Text text) {
      TextSerializerHelper.serializeLong(minValue, text, ',');
      TextSerializerHelper.serializeLong(maxValue, text, '\0');
      return text;
    }

    @Override
    public void fromText(Text text) {
      minValue = TextSerializerHelper.consumeInt(text, ',');
      maxValue = TextSerializerHelper.consumeInt(text, '\0');
    }

    public void expand(MinMax value) {
      if (value.minValue < minValue)
        minValue = value.minValue;
      if (value.maxValue > maxValue)
        maxValue = value.maxValue;
    }

    public void expand(int value) {
      if (value < minValue)
        minValue = value;
      if (value > maxValue)
        maxValue = value;
    }
    
    public void expand(Float value) {
        if (value < minValue)
          minValue = value.intValue();
        if (value > maxValue)
          maxValue = value.intValue();
      }
  }

  public static class Map extends MapReduceBase implements
      Mapper<NASADataset, NASAShape, NullWritable, MinMax> {

    private MinMax minMax = new MinMax();
    public void map(NASADataset dummy, NASAShape point,
        OutputCollector<NullWritable, MinMax> output, Reporter reporter)
            throws IOException {
      minMax.minValue = minMax.maxValue = point.getValue();
      output.collect(NullWritable.get(), minMax);
    }
  }
  
  public static class Reduce extends MapReduceBase implements
  Reducer<NullWritable, MinMax, NullWritable, MinMax> {
    @Override
    public void reduce(NullWritable dummy, Iterator<MinMax> values,
        OutputCollector<NullWritable, MinMax> output, Reporter reporter)
            throws IOException {
      MinMax agg = new MinMax();
      while (values.hasNext()) {
        agg.expand(values.next());
      }
      output.collect(dummy, agg);
    }
  }
  
  /**
   * Counts the exact number of lines in a file by issuing a MapReduce job
   * that does the thing
   * @param files
   * @param params
   * @return
   * @throws IOException
   */
  public static MinMax aggregateMapReduce(Path[] files, OperationsParams params)
      throws IOException {
    Shape plotRange = params.getShape("rect");
    JobConf job = new JobConf(params, Aggregate.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path("agg_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
    job.setJobName("Aggregate");
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(MinMax.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    job.setInputFormat(ShapeInputFormat.class);
    job.setClass("shape", NASAPoint.class, Shape.class);
    if (plotRange != null) {
      job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    }

    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, files);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    JobClient.runJob(job);
      
    // Read job result
    FileStatus[] results = outFs.listStatus(outputPath);
    MinMax minMax = new MinMax();
    for (FileStatus status : results) {
      if (status.getLen() > 0 && status.getPath().getName().startsWith("part-")) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(outFs.open(status.getPath())));
        String line;
        MinMax value = new MinMax();
        while ((line = reader.readLine()) != null) {
          value.fromText(new Text(line));
          minMax.expand(value);
        }
        reader.close();
      }
    }
    
    outFs.delete(outputPath, true);
    
    return minMax;
  }
  
  /**
   * Computes the minimum and maximum values of readings in input. Useful as
   * a preparatory step before drawing.
   * @param inFiles A list of input files.
   * @param params
   * @return
   * @throws IOException
   */
  public static MinMax aggregate(Path[] inFiles, OperationsParams params)
      throws IOException {
    boolean forceCompute = params.getBoolean("force", false);

    // Check if we have hard-coded cached values for the given dataset
    String inPathStr = inFiles[0].toString();
    if (!forceCompute &&
        (inPathStr.contains("MOD11A1") || inPathStr.contains("MYD11A1"))) {
      MinMax min_max = new MinMax();
      // Land temperature
      min_max = new MinMax();
      //min_max.minValue = 10000; // 200 K, -73 C, -100 F
      //min_max.maxValue = 18000; // 360 K,  87 C,  188 F
      min_max.minValue = 13650; // 273 K,  0 C
      min_max.maxValue = 17650; // 353 K, 80 C
      return min_max;
    }

    // Need to process input files to get stats from it and calculate its size
    JobConf job = new JobConf(params, FileMBR.class);
    FileInputFormat.setInputPaths(job, inFiles);
    ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();

    return aggregateMapReduce(inFiles, params);
  }
  
  private static void printUsage() {
    System.out.println("Calculates the minimum and maximum values from HDF files");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
    System.out.println("dataset: The dataset to read from HDF flies");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInput()) {
      printUsage();
      System.exit(1);
    }

    long t1 = System.currentTimeMillis();
    MinMax minmax = aggregate(params.getPaths(), params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total processing time: "+(t2-t1)+" millis");
    System.out.println("MinMax of readings is "+minmax);
  }
}
