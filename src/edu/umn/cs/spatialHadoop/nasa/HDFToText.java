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
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat3;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;

/**
 * This operation transforms one or more HDF files into text files which can
 * be used with other operations. Each point in the HDF file will be represented
 * as one line in the text output.
 * @author Ahmed Eldawy
 *
 */
public class HDFToText {
  public static int date;
  public static class HDFToTextMap extends MapReduceBase implements
      Mapper<NASADataset, NASAShape, Rectangle, NASAShape> {

    public void map(NASADataset dataset, NASAShape value,
        OutputCollector<Rectangle, NASAShape> output, Reporter reporter)
            throws IOException {
      output.collect(dataset, value);
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
   */
  public static long HDFToTextMapReduce(Path inPath, Path outPath,
      String datasetName, boolean skipFillValue) throws IOException {
    JobConf job = new JobConf(HDFToText.class);
    job.setJobName("HDFToText");

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();

    // Set Map function details
    job.setMapperClass(HDFToTextMap.class);
    job.setMapOutputKeyClass(Rectangle.class);
    job.setMapOutputValueClass(NASAPoint.class);
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(0);
    
    // Set input information
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.setInputPaths(job, inPath);
    job.setClass("shape", NASAPoint.class, Shape.class);
    job.set(HDFRecordReader.DatasetName, datasetName);
    job.setBoolean(HDFRecordReader.SkipFillValue, skipFillValue);
    
    // Set output information
    job.setOutputFormat(GridOutputFormat3.class);
    GridOutputFormat3.setOutputPath(job, outPath);
    
    // Run the job
    RunningJob lastSubmittedJob = JobClient.runJob(job);
    Counters counters = lastSubmittedJob.getCounters();
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
   */
  public static void main(String[] args) throws IOException {
    OperationsParams cla = new OperationsParams(new GenericOptionsParser(args));
    JobConf conf = new JobConf(HDFToText.class);
    Path[] paths = cla.getPaths();
    if (paths.length < 2) {
      printUsage();
      System.err.println("Please provide both input and output files");
      return;
    }
    Path inPath = paths[0];
    Path outPath = paths[1];
    
    FileSystem fs = inPath.getFileSystem(conf);
    if (!fs.exists(inPath)) {
      printUsage();
      System.err.println("Input file does not exist");
      return;
    }
    
    boolean overwrite = cla.is("overwrite");
    FileSystem outFs = outPath.getFileSystem(conf);
    if (outFs.exists(outPath)) {
      if (overwrite)
        outFs.delete(outPath, true);
      else
        throw new RuntimeException("Output file exists and overwrite flag is not set");
    }

    String datasetName = cla.get("dataset");
    if (datasetName == null) {
      printUsage();
      System.err.println("Please specify the dataset you want to extract");
      return;
    }
    boolean skipFillValue = cla.is("skipfillvalue", true);
    date = cla.getInt("date", 0);
    

    HDFToTextMapReduce(inPath, outPath, datasetName, skipFillValue);
  }
}
