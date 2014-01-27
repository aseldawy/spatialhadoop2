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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.nasa.NASADataset;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;
import edu.umn.cs.spatialHadoop.nasa.NASAShape;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeFilter;

/**
 * Computes a number of aggregate functions for an input file
 * @author Ahmed Eldawy
 *
 */
public class Aggregate {
  
  /**
   * A structure to hold some aggregate values used to draw HDF files efficiently
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
      return super.toString()+","+minValue+","+maxValue;
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
   * @param conf
   * @param fs
   * @param file
   * @return
   * @throws IOException 
   */
  public static MinMax aggregateMapReduce(FileSystem fs, Path[] files, Shape plotRange)
      throws IOException {
    JobConf job = new JobConf(Aggregate.class);
    
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
    if (plotRange != null) {
      job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
      RangeFilter.setQueryRange(job, plotRange); // Set query range for filter
    }

    SpatialSite.setShapeClass(job, NASAPoint.class);
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
   * Counts the exact number of lines in a file by opening the file and
   * reading it line by line
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static MinMax aggregateLocal(FileSystem fs, Path[] files, Shape plotRange) throws IOException {
    MinMax minMax = new MinMax();
    for (Path file : files) {
      long file_size = fs.getFileStatus(file).getLen();
      
      // HDF file
      HDFRecordReader reader = new HDFRecordReader(new Configuration(),
          new FileSplit(file, 0, file_size, new String[] {}), null, true);
      NASADataset key = reader.createKey();
      NASAShape point = reader.createValue();
      while (reader.next(key, point)) {
        if (plotRange == null || plotRange.isIntersected(point)) {
          if (point.getValue() < minMax.minValue)
            minMax.minValue = point.getValue();
          if (point.getValue() > minMax.maxValue)
            minMax.maxValue = point.getValue();
        }
      }
      reader.close();
    }
    
    return minMax;
  }
  
  /**
   * Computes the minimum and maximum values of readings in input. Useful as
   * a preparatory step before drawing.
   * @param fs - File system of input files.
   * @param inFiles - A list of input files.
   * @param plotRange - The spatial range to plot
   * @param forceCompute - Ignore any preset values and force computation of
   *   the aggregate functions.
   * @return
   * @throws IOException
   */
  public static MinMax aggregate(FileSystem fs, Path[] inFiles,
      Shape plotRange, boolean forceCompute) throws IOException {
    MinMax min_max = new MinMax();

    // Check if we have cached values for the given dataset
    String inPathStr = inFiles[0].toString();
    if (!forceCompute &&
        (inPathStr.contains("MOD11A1") || inPathStr.contains("MYD11A1"))) {
      // Land temperature
      min_max = new MinMax();
      //min_max.minValue = 10000; // 200 K, -73 C, -100 F
      //min_max.maxValue = 18000; // 360 K,  87 C,  188 F
      min_max.minValue = 13650; // 273 K,  0 C
      min_max.maxValue = 17650; // 353 K, 80 C
    } else {
      // Need to process input files to get stats from it and calculate its size
      if (inFiles.length == 1) {
        FileSystem inFs = inFiles[0].getFileSystem(new Configuration());
        FileStatus inFStatus = inFs.getFileStatus(inFiles[0]);
        
        if (inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 3) {
          // Either a directory of file or a large file
          min_max = aggregateMapReduce(fs, inFiles, plotRange);
        } else {
          // A single small file, process it without MapReduce
          min_max = aggregateLocal(fs, inFiles, plotRange);
        }
      } else {
        min_max = aggregateMapReduce(fs, inFiles, plotRange);
      }

    }
    
    return min_max;
  }

}
