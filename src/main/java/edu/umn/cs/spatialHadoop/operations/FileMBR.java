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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

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
  
  public static Partition fileMBRLocal(Path[] inFiles, final OperationsParams params)
      throws IOException, InterruptedException {
    // 1- Split the input path/file to get splits that can be processed independently
    final SpatialInputFormat3<Rectangle, Shape> inputFormat =
        new SpatialInputFormat3<Rectangle, Shape>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inFiles);
    final List<org.apache.hadoop.mapreduce.InputSplit> splits = inputFormat.getSplits(job);
    int parallelism = params.getInt("parallel", Runtime.getRuntime().availableProcessors());

    // 2- Process splits in parallel
    List<Map<String, Partition>> allMbrs = Parallel.forEach(splits.size(), new RunnableRange<Map<String, Partition>>() {
      @Override
      public Map<String, Partition> run(int i1, int i2) {
        Map<String, Partition> mbrs = new HashMap<String, Partition>();
        for (int i = i1; i < i2; i++) {
          try {
            org.apache.hadoop.mapreduce.lib.input.FileSplit fsplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit) splits.get(i);
            final RecordReader<Rectangle, Iterable<Shape>> reader =
                inputFormat.createRecordReader(fsplit, null);
            if (reader instanceof SpatialRecordReader3) {
              ((SpatialRecordReader3)reader).initialize(fsplit, params);
            } else if (reader instanceof RTreeRecordReader3) {
              ((RTreeRecordReader3)reader).initialize(fsplit, params);
            } else if (reader instanceof HDFRecordReader) {
              ((HDFRecordReader)reader).initialize(fsplit, params);
            } else {
              throw new RuntimeException("Unknown record reader");
            }
            Partition p = mbrs.get(fsplit.getPath().getName());
            if (p == null) {
              p = new Partition();
              p.filename = fsplit.getPath().getName();
              p.cellId = p.filename.hashCode();
              p.size = 0;
              p.recordCount = 0;
              p.set(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
              mbrs.put(p.filename, p);
            }
            Text temp = new Text2();
            while (reader.nextKeyValue()) {
              Iterable<Shape> shapes = reader.getCurrentValue();
              for (Shape s : shapes) {
                Rectangle mbr = s.getMBR();
                if (mbr != null)
                  p.expand(mbr);
                p.recordCount++;
                temp.clear();
                s.toText(temp);
                p.size += temp.getLength() + 1;
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return mbrs;
      }
    }, parallelism);
    Map<String, Partition> mbrs = allMbrs.remove(allMbrs.size() - 1);
    for (Map<String, Partition> list : allMbrs) {
      for (Partition p1 : list.values()) {
        Partition p2 = mbrs.get(p1.filename);
        if (p2 != null) {
          p2.expand(p1);
        } else {
          mbrs.put(p1.filename, p1);
        }
      }
    }
    
    // Cache the final result, if needed
    for (Path inFile : inFiles) {
      FileSystem inFs = inFile.getFileSystem(params);
      if (!inFs.getFileStatus(inFile).isDir())
        continue;
      Path gindex_path = new Path(inFile, "_master.heap");
      // Answer has been already cached (may be by another job)
      if (inFs.exists(gindex_path))
        continue;
      FileStatus[] files = inFs.listStatus(inFile, SpatialSite.NonHiddenFileFilter);
      PrintStream wktout = new PrintStream(inFs.create(new Path(inFile, "_heap.wkt"), false));
      PrintStream gout = new PrintStream(inFs.create(gindex_path, false));
      
      Text text = new Text2();
      for (FileStatus file : files) {
        text.clear();
        Partition p = mbrs.get(file.getPath().getName());
        gout.println(p.toText(text).toString());
        wktout.println(p.toWKT());
      }
      
      wktout.close();
      gout.close();
    }

    // Return the final answer
    Partition finalResult = new Partition();
    finalResult.size = finalResult.recordCount = 0;
    finalResult.x1 = finalResult.y1 = Double.MAX_VALUE;
    finalResult.x2 = finalResult.y2 = -Double.MAX_VALUE;
    for (Partition p2 : mbrs.values())
      finalResult.expand(p2);
    return finalResult;
  }

  /**
   * Computes the MBR of the input file using an aggregate MapReduce job.
   * 
   * @param inFile - Path to input file
   * @param params - Additional operation parameters
   * @return
   * @throws IOException
   * @throws InterruptedException 
   */
  private static <S extends Shape> Partition fileMBRMapReduce(Path[] inFiles,
      OperationsParams params) throws IOException, InterruptedException {
    JobConf job = new JobConf(params, FileMBR.class);
      
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(inFiles[0].getName()+".mbr_"+(int)(Math.random()*1000000));
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
    
    ShapeInputFormat.setInputPaths(job, inFiles);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setOutputCommitter(MBROutputCommitter.class);
    
    // Submit the job
    if (OperationsParams.isLocal(job, inFiles)) {
      // Enforce local execution if explicitly set by user or for small files
      job.set("mapred.job.tracker", "local");
      // Use multithreading too
      job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    }
    
    if (params.getBoolean("background", false)) {
      JobClient jc = new JobClient(job);
      lastSubmittedJob = jc.submitJob(job);
      return null;
    } else {
      lastSubmittedJob = JobClient.runJob(job);
      Counters counters = lastSubmittedJob.getCounters();
      Counter outputSizeCounter = counters.findCounter(Task.Counter.MAP_INPUT_BYTES);
      sizeOfLastProcessedFile = outputSizeCounter.getCounter();
      
      FileStatus[] outFiles = outFs.listStatus(outputPath,
          SpatialSite.NonHiddenFileFilter);
      Partition mbr = new Partition();
      mbr.set(Double.MAX_VALUE, Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
      OperationsParams localMBRParams = new OperationsParams(params);
      localMBRParams.setBoolean("local", true); // Enforce local execution
      localMBRParams.setClass("shape", Partition.class, Shape.class);
      for (FileStatus outFile : outFiles) {
        if (outFile.isDir())
          continue;
        ShapeRecordReader<Partition> reader = new ShapeRecordReader<Partition>
            (localMBRParams, new FileSplit(outFile.getPath(), 0, outFile.getLen(), new String[0]));
        Rectangle key = reader.createKey();
        Partition p = reader.createValue();
        while (reader.next(key, p)) {
          mbr.expand(p);
        }
        reader.close();
      }
      
      outFs.delete(outputPath, true);
      return mbr;
    }
  }

  /**
   * Returns the MBR of a file given that it is globally indexed.
   * @param file
   * @return
   * @throws IOException 
   */
  private static Partition fileMBRCached(Path[] files, OperationsParams params) throws IOException {
    Partition p = new Partition();
    for (Path file : files) {
      FileSystem inFs = file.getFileSystem(params);
      // Quickly get file MBR if it is globally indexed
      GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, file);
      if (globalIndex == null)
        return null;
      p.set(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
      for (Partition part : globalIndex) {
        p.expand(part);
      }
      sizeOfLastProcessedFile = p.size;
    }
    return p;
  }

  public static Partition fileMBR(Path file, OperationsParams params) throws IOException, InterruptedException {
    return fileMBR(new Path[] {file}, params);
  }
  
  public static Partition fileMBR(Path[] files, OperationsParams params) throws IOException, InterruptedException {
    Partition cachedMBR = fileMBRCached(files, params);
    if (cachedMBR != null)
      return cachedMBR;
    if (!params.autoDetectShape()) {
      LOG.error("shape of input files is not set and cannot be auto detected");
      return null; 
    }
    
    // Process with MapReduce
    if (OperationsParams.isLocal(params, files)) {
      return fileMBRLocal(files, params);
    } else {
      return fileMBRMapReduce(files, params);
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
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    Path[] inputFiles = params.getInputPaths();
    
    if (params.getShape("shape") == null) {
      LOG.error("Input file format not specified");
      printUsage();
      return;
    }
    long t1 = System.currentTimeMillis();
    Rectangle mbr = fileMBR(inputFiles, params);
    long t2 = System.currentTimeMillis();
    if (mbr == null) {
      LOG.error("Error computing the MBR");
      System.exit(1);
    }
      
    System.out.println("Total processing time: "+(t2-t1)+" millis");
    System.out.println("MBR of records in file '"+inputFiles+"' is "+mbr);
  }

}
