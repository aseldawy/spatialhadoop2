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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.SpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.NASADataset;

/**
 * Finds the minimal bounding rectangle for a file.
 * @author Ahmed Eldawy
 *
 */
public class FileMBR {
  /**Logger for FileMBR*/
  private static final Log LOG = LogFactory.getLog(FileMBR.class);

  /**
   * Keeps track of the size of last processed file. Used to determine the
   * uncompressed size of a file which is helpful to calculate the number
   * of required partitions to index a file.
   */
  public static long sizeOfLastProcessedFile;

  /**Last submitted MBR MapReduce job*/
  public static RunningJob lastSubmittedJob;

  public static class FileMBRMapper extends MapReduceBase implements
      Mapper<Rectangle, Text, Text, Partition> {
    
    /**Last input split processed (initially null)*/
    private InputSplit lastSplit = null;
    
    /**Name of the file currently being processed*/
    private Text fileName;
    
    /**Shared value*/
    private Partition value;

    /**Stock shape to parse input file*/
    private Shape shape;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      value = new Partition();
      value.recordCount = 1;
      this.shape = OperationsParams.getShape(job, "shape");
    }
    
    public void map(Rectangle dummy, Text text,
        OutputCollector<Text, Partition> output, Reporter reporter)
            throws IOException {
      if (lastSplit != reporter.getInputSplit()) {
        lastSplit = reporter.getInputSplit();
        value.filename = ((FileSplit)lastSplit).getPath().getName();
        fileName = new Text(value.filename);
      }
      value.size = text.getLength() + 1; // +1 for new line
      shape.fromText(text);
      Rectangle mbr = shape.getMBR();

      if (mbr != null) {
        value.set(mbr);
        output.collect(fileName, value);
      }
    }
  }
  
  public static class Combine extends MapReduceBase implements
  Reducer<Text, Partition, Text, Partition> {
    @Override
    public void reduce(Text filename, Iterator<Partition> values,
        OutputCollector<Text, Partition> output, Reporter reporter)
            throws IOException {
      if (values.hasNext()) {
        Partition partition = values.next().clone();
        while (values.hasNext()) {
          partition.expand(values.next());
        }
        output.collect(filename, partition);
      }
    }
  }
  
  public static class Reduce extends MapReduceBase implements
    Reducer<Text, Partition, NullWritable, Partition> {
    @Override
    public void reduce(Text filename, Iterator<Partition> values,
        OutputCollector<NullWritable, Partition> output, Reporter reporter)
            throws IOException {
      if (values.hasNext()) {
        Partition partition = values.next().clone();
        while (values.hasNext()) {
          partition.expand(values.next());
        }
        partition.cellId = Math.abs(filename.hashCode());
        output.collect(NullWritable.get(), partition);
      }
    }
  }
  
  /**
   * This output committer caches the MBR calculated for the input file such
   * that subsequent calls to FileMBR will return the answer right away. This
   * is only possible if the input is a directory as it stores the answer in
   * a hidden file inside this directory.
   * @author Ahmed Eldawy
   *
   */
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
        Path gindex_path = new Path(inPath, "_master.heap");
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
   * Computes the MBR of the input file using an aggregate MapReduce job.
   * 
   * @param file - Path to input file
   * @param params - Additional operation parameters
   * @return
   * @throws IOException
   */
  private static <S extends Shape> Partition fileMBRMapReduce(Path file,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, FileMBR.class);
      
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(file.getName()+".mbr_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
    job.setJobName("FileMBR");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Partition.class);

    job.setMapperClass(FileMBRMapper.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Combine.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    job.setInputFormat(ShapeLineInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setOutputCommitter(MBROutputCommitter.class);
    
    // Submit the job
    if (params.is("background")) {
      JobClient jc = new JobClient(job);
      lastSubmittedJob = jc.submitJob(job);
      return null;
    } else {
      lastSubmittedJob = JobClient.runJob(job);
      FileStatus[] outFiles = outFs.listStatus(outputPath,
          SpatialSite.NonHiddenFileFilter);
      Partition mbr = new Partition();
      mbr.set(Double.MAX_VALUE, Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
      OperationsParams localMBRParams = new OperationsParams(params);
      localMBRParams.setBoolean("local", true);
      localMBRParams.setClass("shape", Partition.class, Shape.class);
      for (FileStatus outFile : outFiles) {
        if (outFile.isDir())
          continue;
        Partition p = FileMBR.fileMBRLocal(outFile.getPath(), localMBRParams);
        mbr.expand(p);
      }
      return mbr;
    }
  }

  /**
   * Returns the MBR of a file given that it is globally indexed.
   * @param file
   * @return
   * @throws IOException 
   */
  private static Partition fileMBRCached(Path file, OperationsParams params) throws IOException {
    FileSystem inFs = file.getFileSystem(params);
    // Quickly get file MBR if it is globally indexed
    GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, file);
    if (globalIndex == null)
      return null;
    Partition p = new Partition();
    p.set(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Partition part : globalIndex) {
      p.expand(part);
    }
    sizeOfLastProcessedFile = p.size;
    return p;
  }

  private static Partition fileMBRLocal(Path inFile, OperationsParams params)
      throws IOException {    
    JobConf job = new JobConf(params);
    ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();
    ShapeInputFormat.addInputPath(job, inFile);
    InputSplit[] splits = inputFormat.getSplits(job, 1);

    // Prepare master file if needed
    // Keep partition information for each file in input dataset
    Map<String, Partition> partitionPerFile = new HashMap<String, Partition>();
    Partition total = new Partition();
    total.set(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);

    for (InputSplit split : splits) {
      FileSplit fsplit = (FileSplit) split;
      Partition p = new Partition();
      p.set(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
      p.filename = fsplit.getPath().getName();
      p.size = split.getLength(); // TODO handle compressed files
      p.recordCount = 0;
      RecordReader<Rectangle, Shape> reader = inputFormat.getRecordReader(split, job, null);
      
      Rectangle key = (Rectangle) reader.createKey();
      Shape value = (Shape) reader.createValue();
      if (key instanceof NASADataset) {
        // For HDF file, extract MBR from the file header
        if (reader.next(key, value)) {
          p.expand(key);
        }
      } else {
        while (reader.next(key, value)) {
          p.recordCount++;
          Rectangle shapeMBR = value.getMBR();
          if (shapeMBR != null)
            p.expand(shapeMBR);;
        }
      }
      // Merge partition information with an existing one (if any)
      total.expand(p);
      Partition oldP = partitionPerFile.get(p.filename);
      if (oldP == null) {
        p.cellId = partitionPerFile.size() + 1;
        partitionPerFile.put(p.filename, p);
      } else {
        oldP.expand(p);
      }
      reader.close();
    }
    FileSystem inFS = inFile.getFileSystem(params);
    if (inFS.getFileStatus(inFile).isDir()) {
      Path masterFilePath = new Path(inFile, "_master.heap");
      PrintStream masterFileOut = new PrintStream(inFS.create(masterFilePath));
      for (Map.Entry<String, Partition> entry : partitionPerFile.entrySet()) {
        masterFileOut.println(entry.getValue().toText(new Text()));
      }
      masterFileOut.close();
    }

    sizeOfLastProcessedFile = total.size;

    return total;
  }
  
  public static Partition fileMBR(Path file, OperationsParams params) throws IOException {
    Partition cachedMBR = fileMBRCached(file, params);
    if (cachedMBR != null)
      return cachedMBR;
    if (!params.autoDetectShape()) {
      LOG.error("shape of input files is not set and cannot be auto detected");
      return null; 
    }
    
    JobConf job = new JobConf(params, FileMBR.class);
    FileInputFormat.addInputPath(job, file);
    ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();

    boolean autoLocal = inputFormat.getSplits(job, 1).length <= 3;
    boolean isLocal = params.is("local", autoLocal);
    
    if (!isLocal) {
      // Process with MapReduce
      return fileMBRMapReduce(file, params);
    } else {
      // Process without MapReduce
      return fileMBRLocal(file, params);
    }
  }

  private static void printUsage() {
    System.out.println("Finds the MBR of an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
    System.out.println("shape:<input shape>: (*) Input file format");
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
    Path inputFile = params.getInputPath();
    
    if (params.getShape("shape") == null) {
      LOG.error("Input file format not specified");
      printUsage();
      return;
    }
    long t1 = System.currentTimeMillis();
    Rectangle mbr = fileMBR(inputFile, params);
    long t2 = System.currentTimeMillis();
    if (mbr == null) {
      LOG.error("Error computing the MBR");
      System.exit(1);
    }
      
    System.out.println("Total processing time: "+(t2-t1)+" millis");
    System.out.println("MBR of records in file '"+inputFile+"' is "+mbr);
  }

}
