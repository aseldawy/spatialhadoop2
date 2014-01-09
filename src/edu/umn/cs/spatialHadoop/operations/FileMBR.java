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
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.nasa.NASADataset;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;

/**
 * Finds the minimal bounding rectangle for a file.
 * @author Ahmed Eldawy
 *
 */
public class FileMBR {
  /**
   * Keeps track of the size of last processed file. Used to determine the
   * uncompressed size of a file.
   */
  public static long sizeOfLastProcessedFile;

  /**Last submitted MBR MapReduce job*/
  public static RunningJob lastSubmittedJob;

  public static class Map extends MapReduceBase implements
      Mapper<Rectangle, Shape, Text, Rectangle> {
    
    /**Last input split processed (initially null)*/
    private InputSplit lastSplit = null;
    
    /**Name of the file currently being processed*/
    private Text fileName;
    
    public void map(Rectangle dummy, Shape shape,
        OutputCollector<Text, Rectangle> output, Reporter reporter)
            throws IOException {
      if (lastSplit != reporter.getInputSplit()) {
        lastSplit = reporter.getInputSplit();
        fileName = new Text(((FileSplit)lastSplit).getPath().getName());
      }
      Rectangle mbr = shape.getMBR();

      if (mbr != null) {
        output.collect(fileName, mbr);
      }
    }
  }
  
  public static class Combine extends MapReduceBase implements
  Reducer<Text, Rectangle, Text, Rectangle> {
    @Override
    public void reduce(Text filename, Iterator<Rectangle> values,
        OutputCollector<Text, Rectangle> output, Reporter reporter)
            throws IOException {
      Rectangle mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
      while (values.hasNext()) {
        Rectangle rect = values.next();
        mbr.expand(rect);
      }
      output.collect(filename, mbr);
    }
  }
  
  public static class Reduce extends MapReduceBase implements
  Reducer<Text, Rectangle, NullWritable, Partition> {
    @Override
    public void reduce(Text filename, Iterator<Rectangle> values,
        OutputCollector<NullWritable, Partition> output, Reporter reporter)
            throws IOException {
      Rectangle mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
      while (values.hasNext()) {
        Rectangle rect = values.next();
        mbr.expand(rect);
      }
      Partition partition = new Partition(filename.toString(), mbr);
      partition.cellId = Math.abs(filename.hashCode());
      output.collect(NullWritable.get(), partition);
    }
  }
  
  public static class MBROutputCommitter extends FileOutputCommitter {
    // If input is a directory, save the MBR to a _master file there
    @Override
    public void commitJob(JobContext context) throws IOException {
      try {
        super.commitJob(context);
        // Store the result back in the input file if it is a directory
        JobConf job = context.getJobConf();

        Path[] inPaths = SpatialInputFormat.getInputPaths(job);
        Path inPath = inPaths[0]; // TODO Handle multiple file input
        FileSystem inFs = inPath.getFileSystem(job);
        if (!inFs.getFileStatus(inPath).isDir())
          return;
        Path gindex_path = new Path(inPath, "_master.grid");
        // Answer has been already cached (may be by another job)
        if (inFs.exists(gindex_path))
          return;
        PrintStream gout = new PrintStream(inFs.create(gindex_path, false));

        // Read job result and concatenate everything to the master file
        Path outPath = TextOutputFormat.getOutputPath(job);
        FileSystem outFs = outPath.getFileSystem(job);
        FileStatus[] results = outFs.listStatus(outPath);
        for (FileStatus fileStatus : results) {
          if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
            LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
            Text text = new Text();
            while (lineReader.readLine(text) > 0) {
              gout.println(text);
            }
            lineReader.close();
          }
        }
        gout.close();
      } catch (RuntimeException e) {
        // This might happen of the input directory is read only
        LOG.info("Error caching the output of FileMBR");
      }
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
  public static <S extends Shape> Rectangle fileMBRMapReduce(FileSystem fs,
      Path file, S stockShape, boolean background) throws IOException {
    // Quickly get file MBR if it is globally indexed
    GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(fs, file);
    if (globalIndex != null) {
      // Return the MBR of the global index.
      // Compute file size by adding up sizes of all files assuming they are
      // not compressed
      long totalLength = 0;
      for (Partition p : globalIndex) {
        Path filePath = new Path(file, p.filename);
        if (fs.exists(filePath))
          totalLength += fs.getFileStatus(filePath).getLen();
      }
      sizeOfLastProcessedFile = totalLength;
      return globalIndex.getMBR();
    }
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(file.toUri().getPath()+".mbr_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
    job.setJobName("FileMBR");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Rectangle.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Combine.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    job.setInputFormat(ShapeInputFormat.class);
    SpatialSite.setShapeClass(job, stockShape.getClass());
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setOutputCommitter(MBROutputCommitter.class);
    
    // Submit the job
    if (background) {
      JobClient jc = new JobClient(job);
      lastSubmittedJob = jc.submitJob(job);
      return null;
    } else {
      lastSubmittedJob = JobClient.runJob(job);
      Counters counters = lastSubmittedJob.getCounters();
      Counter inputBytesCounter = counters.findCounter(Task.Counter.MAP_INPUT_BYTES);
      FileMBR.sizeOfLastProcessedFile = inputBytesCounter.getValue();
      
      // Read job result
      FileStatus[] results = outFs.listStatus(outputPath);
      Rectangle mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
          mbr.expand(fileMBRLocal(outFs, fileStatus.getPath(), new Partition()));
        }
      }
      
      outFs.delete(outputPath, true);
      
      return mbr;
    }
  }
  
  /**
   * Counts the exact number of lines in a file by opening the file and
   * reading it line by line
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static <S extends Shape> Rectangle fileMBRLocal(FileSystem fs,
      Path file, S shape) throws IOException {
    // Try to get file MBR from the global index (if possible)
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, file);
    if (gindex != null) {
      return gindex.getMBR();
    }
    long file_size = fs.getFileStatus(file).getLen();
    sizeOfLastProcessedFile = file_size;
    
    Rectangle mbr = null;
    
    if (file.getName().matches("(?i:.*\\.hdf$)")) {
      // HDF file
      HDFRecordReader reader = new HDFRecordReader(new Configuration(),
          new FileSplit(file, 0, file_size, new String[] {}), null, true);
      NASADataset key = reader.createKey();
      NASAPoint point = reader.createValue();
      if (reader.next(key, point)) {
        mbr = key.getMBR();
      }
      reader.close();
    } else {
      ShapeRecordReader<Shape> reader = new ShapeRecordReader<Shape>(
          new Configuration(), new FileSplit(file, 0, file_size, new String[] {}));
      
      mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);

      Rectangle key = reader.createKey();
      
      while (reader.next(key, shape)) {
        Rectangle rect = shape.getMBR();
        if (rect != null)
          mbr.expand(rect);
      }
      reader.close();
    }
    return mbr;
  }
  
  public static Rectangle fileMBR(FileSystem fs, Path inFile, Shape stockShape) throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 3) {
      // Either a directory of file or a large file
      return fileMBRMapReduce(fs, inFile, stockShape, false);
    } else {
      // A single small file, process it without MapReduce
      return fileMBRLocal(fs, inFile, stockShape);
    }
  }

  private static void printUsage() {
    System.out.println("Finds the MBR of an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
  }
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(FileMBR.class);
    Path inputFile = cla.getPath();
    if (inputFile == null) {
      System.err.println("Please provide the input file");
      printUsage();
      return;
    }
    
    FileSystem fs = inputFile.getFileSystem(conf);
    if (!fs.exists(inputFile)) {
      System.err.println("Input file '"+inputFile+"' does not exist");
      printUsage();
      return;
    }

    Shape stockShape = cla.getShape(true);
    long t1 = System.currentTimeMillis();
    Rectangle mbr = fileMBR(fs, inputFile, stockShape);
    long t2 = System.currentTimeMillis();
    System.out.println("Total processing time: "+(t2-t1)+" millis");
    System.out.println("MBR of records in file "+inputFile+" is "+mbr);
  }

}
