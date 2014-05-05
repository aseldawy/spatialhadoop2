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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;


/**
 * Shuffle the lines in an input text file.
 * @author Ahmed Eldawy
 *
 */
public class Shuffle {

  private static final String NumOfPartitions =
      "edu.umn.cs.spatialHadoop.operations.LineRandomizer";
  
  public static class Map extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, Text> {
    /**Total number of partitions to generate*/
    private int numOfPartitions;
    
    /**Temporary key used to generate intermediate records*/
    private IntWritable tempKey = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      numOfPartitions = job.getInt(NumOfPartitions, 1);
    }
    
    public void map(LongWritable key, Text line,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      tempKey.set((int) (Math.random() * numOfPartitions));
      output.collect(tempKey, line);
    }
  }
  
  public static class Reduce extends MapReduceBase implements
      Reducer<IntWritable, Text, NullWritable, Text> {

    private NullWritable dummy = NullWritable.get();
    
    @Override
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException {
      // Retrieve all lines in this reduce group
      Vector<Text> all_lines = new Vector<Text>();
      while (values.hasNext()) {
        Text t = values.next();
        all_lines.add(new Text(t));
      }
      
      // Randomize lines within this group
      Collections.shuffle(all_lines);
      
      // Output lines in the randomized order
      for (Text line : all_lines) {
        output.collect(dummy, line);
      }
    }
  }
  
  /**
   * Counts the exact number of lines in a file by issuing a MapReduce job
   * that does the thing
   * @param conf
   * @param infs
   * @param infile
   * @return
   * @throws IOException 
   */
  public static void randomizerMapReduce(Path infile, Path outfile,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(Shuffle.class);
    
    job.setJobName("Randomizer");
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setMapperClass(Map.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);

    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

    job.setInt(NumOfPartitions, Math.max(1, clusterStatus.getMaxReduceTasks()));
    
    job.setInputFormat(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, infile);
    
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outfile);
    
    // Submit the job
    JobClient.runJob(job);
  }
  
  private static void printUsage() {
    System.out.println("Shuffles the lines of an input text file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
    System.out.println("<output file>: (*) Path to output file");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    Path inputFile = params.getInputPath();
    Path outputFile = params.getOutputPath();
    randomizerMapReduce(inputFile, outputFile, params);
  }

}
