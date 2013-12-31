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

import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.nasa.NASADataset;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;

/**
 * Computes a number of aggregate functions for an input file
 * @author Ahmed Eldawy
 *
 */
public class Aggregate {
  
  /**
   * Keeps track of the size of last processed file. Used to determine the
   * uncompressed size of a file.
   */
  public static long sizeOfLastProcessedFile;
  
  /**
   * A structure to hold some aggregate values used to draw HDF files efficiently
   * @author Ahmed Eldawy
   *
   */
  public static class MinMax extends Rectangle implements Writable, TextSerializable {
    int minValue, maxValue;

    public MinMax() {
      super(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
      minValue = Integer.MAX_VALUE;
      maxValue = Integer.MIN_VALUE;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(minValue);
      out.writeInt(maxValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
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
      TextSerializerHelper.serializeLong(maxValue, text, ',');
      return super.toText(text);
    }

    @Override
    public void fromText(Text text) {
      minValue = TextSerializerHelper.consumeInt(text, ',');
      maxValue = TextSerializerHelper.consumeInt(text, ',');
      super.fromText(text);
    }

    public void expand(MinMax value) {
      super.expand(value);
      if (value.minValue < minValue)
        minValue = value.minValue;
      if (value.maxValue > maxValue)
        maxValue = value.maxValue;
    }
  }

  public static class Map extends MapReduceBase implements
      Mapper<NASADataset, NASAPoint, NullWritable, MinMax> {

    private MinMax minMax = new MinMax();
    public void map(NASADataset dummy, NASAPoint point,
        OutputCollector<NullWritable, MinMax> output, Reporter reporter)
            throws IOException {
      minMax.minValue = minMax.maxValue = point.value;
      // Set spatial boundaries
      minMax.set(point);
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
  public static MinMax aggregateMapReduce(FileSystem fs, Path file)
      throws IOException {
    JobConf job = new JobConf(Aggregate.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(file.toUri().getPath()+".agg_"+(int)(Math.random()*1000000));
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
    SpatialSite.setShapeClass(job, NASAPoint.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
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
  public static MinMax aggregateLocal(FileSystem fs, Path file) throws IOException {
    long file_size = fs.getFileStatus(file).getLen();
    
    MinMax minMax = new MinMax();
    
    // HDF file
    HDFRecordReader reader = new HDFRecordReader(new Configuration(),
        new FileSplit(file, 0, file_size, new String[] {}), null, true);
    NASADataset key = reader.createKey();
    NASAPoint point = reader.createValue();
    while (reader.next(key, point)) {
      if (point.value < minMax.minValue)
        minMax.minValue = point.value;
      if (point.value > minMax.maxValue)
        minMax.maxValue = point.value;
      minMax.expand(point);
    }
    reader.close();
    return minMax;
  }
  
  public static MinMax aggregate(FileSystem fs, Path inFile) throws IOException {
    // Compute file size by adding up sizes of all files assuming they are
    // not compressed
    long totalLength = 0;
    FileStatus[] matches;
    if (inFile.toString().contains("*") || inFile.toString().contains("?")) {
      // Wild card
      matches = fs.globStatus(inFile);
    } else if (fs.getFileStatus(inFile).isDir()) {
      matches = fs.listStatus(inFile);
    } else {
      matches = new FileStatus[] {fs.getFileStatus(inFile)};
    }
    for (FileStatus match : matches) {
      totalLength += match.getLen();
    }
    sizeOfLastProcessedFile = totalLength;

    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    
    if (inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 3) {
      // Either a directory of file or a large file
      return aggregateMapReduce(fs, inFile);
    } else {
      // A single small file, process it without MapReduce
      return aggregateLocal(fs, inFile);
    }
  }

}
