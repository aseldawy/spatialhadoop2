/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.util.WritableByteArray;

/**
 * Copy file using a distributed MapReduce job. Unlike the shell command
 * 'distcp' which creates a map task for each file, this version creates a map
 * task for each block which allows one file to be copied in parallel using
 * multiple machines.
 * @author Ahmed Eldawy
 *
 */
public class DistributedCopy {
  /**Logger for DistributedCopy*/
  private static final Log LOG = LogFactory.getLog(DistributedCopy.class);
  
  /**
   * Holds information about a block in a file with a specific index.
   * @author Ahmed Eldawy
   *
   */
  public static class FileBlockIndex implements WritableComparable<FileBlockIndex> {
    public Path path;
    public int index;
    
    public FileBlockIndex() {
    }
    
    public FileBlockIndex(Path path, int index) {
      this.path = path;
      this.index = index;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(path.toString());
      out.writeInt(index);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.path = new Path(in.readUTF());
      this.index = in.readInt();
    }

    @Override
    public int compareTo(FileBlockIndex o) {
      int diff = this.path.compareTo(o.path);
      if (diff != 0)
        return diff;
      return this.index - o.index;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj == null)
        return false;
      FileBlockIndex other = (FileBlockIndex) obj;
      return this.path.equals(other.path) && this.index == other.index;
    }
    
    @Override
    public int hashCode() {
      return this.path.hashCode() + this.index;
    }
  }
  
  /**
   * An extended version of FileSplit that also stores the position of the
   * block in the file.
   * @author Ahmed Eldawy
   *
   */
  public static class FileBlockSplit extends FileSplit {
    /**Index (position) of this block in the file*/
    private int index;
    
    public FileBlockSplit() {
      super(null, 0, 0, new String[0]);
    }
    
    public FileBlockSplit(Path path, long start, long length, String[] locations, int index) throws IOException {
      super(path, start, length, locations);
      this.index = index;
    }
    
    public int getIndex() {
      return index;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(index);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      this.index = in.readInt();
    }
    
    @Override
    public String toString() {
      return super.toString() + " Split #"+String.format("%05d", index);
    }
  }
  
  /**
   * Reads blocks of binary arrays from input file.
   * @author Ahmed Eldawy
   *
   */
  public static class BlockRecordReader implements RecordReader<FileBlockIndex, WritableByteArray> {
    
    /**Buffer to use when reading from input file*/
    private byte[] buffer;
    /**The key of the current block which contains its index*/
    private FileBlockIndex key;
    /**Input stream which points to the input file*/
    private FSDataInputStream in;
    /**Start position to read from input file*/
    private long start;
    /**Last position to read from input file*/
    private long end;

    public BlockRecordReader(InputSplit split, JobConf job, Reporter reporter)
        throws IOException {
      FileBlockSplit fsplit = (FileBlockSplit) split;
      buffer = new byte[1024 * 1024];
      this.key = new FileBlockIndex(fsplit.getPath(), fsplit.getIndex());
      // Initialize input stream
      Path inPath = fsplit.getPath();
      FileSystem inFs = inPath.getFileSystem(job);
      in = inFs.open(inPath);
      in.seek(this.start = fsplit.getStart());
      this.end = fsplit.getStart() + fsplit.getLength();
      reporter.setStatus("Copying "+inPath+"["+start+","+end+")");
    }
    
    @Override
    public FileBlockIndex createKey() {
      return key;
    }
    
    @Override
    public WritableByteArray createValue() {
      return new WritableByteArray(buffer);
    }
    
    @Override
    public boolean next(FileBlockIndex key, WritableByteArray value)
        throws IOException {
      int bytesRead = in.read(buffer, 0, Math.min(buffer.length, (int)(end - in.getPos())));
      if (bytesRead == 0)
        return false;
      if (bytesRead < buffer.length) {
        // Copy to a smaller buffer
        value.write(buffer, 0, bytesRead);
      }
      return true;
    }

    @Override
    public long getPos() throws IOException {
      return in.getPos();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public float getProgress() throws IOException {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f,
          (in.getPos() - start) / (float)(end - start));
      }
    }
  }
  
  /**
   * An input format that returns blocks of binary data as values.
   * @author Ahmed Eldawy
   *
   */
  public static class BlockInputFormat extends FileInputFormat<FileBlockIndex, WritableByteArray> {
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      long minSize = job.getLong("mapred.min.split.size", 10*1024*1024);
      Path outPath = BlockOutputFormat.getOutputPath(job);
      FileSystem outFs = outPath.getFileSystem(job);
      long outputBlockSize = outFs.getDefaultBlockSize(outPath);
      Vector<FileBlockSplit> splits = new Vector<FileBlockSplit>();
      Queue<FileStatus> files = new ArrayDeque<FileStatus>();
      Path[] dirs = getInputPaths(job);
      if (dirs.length == 0)
        throw new IOException("No input paths specified in job");
      for (Path dir : dirs)
        files.add(dir.getFileSystem(job).getFileStatus(dir));
      while (!files.isEmpty()) {
        FileStatus file = files.poll();
        FileSystem fs = file.getPath().getFileSystem(job);
        if (file.isDir()) {
          // Directory, recursively retrieve all sub directories
          files.addAll(Arrays.asList(fs.listStatus(file.getPath())));
        } else {
          // A file, add its splits to the list of splits
          long fileLength = file.getLen();
          long pos = 0;
          int index = 0;
          while (pos < fileLength) {
            long splitLength = fileLength - pos;
            if ((float)splitLength / outputBlockSize > SPLIT_SLOP &&
                splitLength - outputBlockSize > minSize) {
              // Split length is larger than target block size, trim it.
              splitLength = outputBlockSize;
            }
            BlockLocation[] locations = fs.getFileBlockLocations(file, pos, splitLength);
            Set<String> hosts = new HashSet<String>();
            for (int iBlock = 0; iBlock < locations.length; iBlock++) {
              for (String host : locations[iBlock].getHosts()) {
                hosts.add(host);
              }
            }
            splits.add(new FileBlockSplit(file.getPath(), pos, splitLength,
                hosts.toArray(new String[hosts.size()]), index++));
            pos += splitLength;
          }
        }
      }
      return splits.toArray(new FileBlockSplit[splits.size()]);
    }

    @Override
    public RecordReader<FileBlockIndex, WritableByteArray> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new BlockRecordReader(split, job, reporter);
    }
  }
  
  public static class BlockOutputFormat extends FileOutputFormat<FileBlockIndex, WritableByteArray> {

    private Progressable progress;

    class BlockRecordWriter implements RecordWriter<FileBlockIndex, WritableByteArray> {

      /**File system where the output will be stored*/
      private FileSystem outFs;
      /**A path assigned to this task to write its output*/
      private Path taskOutputPath;
      /**Keeps track of output streams that are currently open*/
      private Map<FileBlockIndex, FSDataOutputStream> cachedStreams;
      /**The path of the input as the user specified it.
       * Used to find the relative path of each file in the input and copy it
       * to the corresponding path in the output path*/
      private String userSpecifiedInputPath;

      public BlockRecordWriter(FileSystem fs, Path outputPath,
          Path userSpecifiedInputPath) {
        this.outFs = fs;
        this.userSpecifiedInputPath = userSpecifiedInputPath.toString();
        this.taskOutputPath = outputPath;
        this.cachedStreams = new HashMap<FileBlockIndex, FSDataOutputStream>();
      }

      @Override
      public void write(FileBlockIndex key, WritableByteArray value)
          throws IOException {
        FSDataOutputStream outFs = getOrCreateOutputStream(key);
        if (value.getLength() == 0) {
          // This indicates an end of block
          outFs.close();
          cachedStreams.remove(key);
        } else {
          outFs.write(value.getData(), 0, value.getLength());
        }
        progress.progress();
      }

      private FSDataOutputStream getOrCreateOutputStream(FileBlockIndex key) throws IOException {
        FSDataOutputStream cachedStream = cachedStreams.get(key);
        if (cachedStream == null) {
          String inputAbsolutePath = key.path.toString();
          if (!inputAbsolutePath.startsWith(userSpecifiedInputPath))
            throw new RuntimeException("Input '"+inputAbsolutePath+
                "' does not contain the prefix '"+userSpecifiedInputPath+"'");
          // Remove the prefix (+1 is to remove the path separator as well)
          String inputRelativePath =
              inputAbsolutePath.substring(userSpecifiedInputPath.length()+1);
          Path outDirPath = new Path(taskOutputPath, inputRelativePath);
          Path outFilePath = new Path(outDirPath, String.format("part-%05d", key.index));
          if (outFs.exists(outFilePath)) {
            LOG.info("Appending to an existing file '"+outFilePath+"'");
            cachedStream = outFs.append(outFilePath);
          } else {
            outFs.mkdirs(outDirPath);
            cachedStream = outFs.create(outFilePath);
          }
          // Cache the new stream
          cachedStreams.put(key, cachedStream);
        }
        return cachedStream;
      }

      @Override
      public void close(Reporter reporter) throws IOException {
        for (Map.Entry<FileBlockIndex, FSDataOutputStream> entry : cachedStreams.entrySet()) {
          LOG.warn("Closing stream at BlockRecordWriter#close() for "+entry.getKey());
          entry.getValue().close();
          reporter.progress();
        }
        cachedStreams.clear();
      }
      
      
    }


    @Override
    public RecordWriter<FileBlockIndex, WritableByteArray> getRecordWriter(
        FileSystem ignored, JobConf job, String name, Progressable progress)
        throws IOException {
      this.progress = progress;
      Path taskOutputPath = FileOutputFormat.getTaskOutputPath(job, name).getParent();
      return new BlockRecordWriter(taskOutputPath.getFileSystem(job),
          taskOutputPath, BlockInputFormat.getInputPaths(job)[0]);
    }
  }
  
  /**
   * Concatenates output files to generate an exact copy of the input dir.
   * @author Ahmed Eldawy
   *
   */
  public static class BlockOutputCommitter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      JobConf job = context.getJobConf();
      Path outPath = BlockOutputFormat.getOutputPath(job);
      FileSystem outFs = outPath.getFileSystem(job);

      // Collect all output folders
      FileStatus[] reducesOut = outFs.listStatus(outPath);
      Queue<FileStatus> dirsToJoin = new ArrayDeque<FileStatus>(Arrays.asList(reducesOut));
      while (!dirsToJoin.isEmpty()) {
        FileStatus dirToConcat = dirsToJoin.poll();
        FileStatus[] dirContents = outFs.listStatus(dirToConcat.getPath());
        Vector<Path> filesToConcat = new Vector<Path>();
        for (FileStatus content : dirContents) {
          if (content.isDir()) {
            // Go deeper in this subdirectory
            dirsToJoin.add(content);
          } else if (content.getPath().getName().startsWith("part-")) {
            filesToConcat.add(content.getPath());
          }
        }
        if (filesToConcat.size() == 1) {
          // Just rename the file
          Path tmpPath = dirToConcat.getPath().suffix("_tmp");
          outFs.rename(filesToConcat.get(0), tmpPath);
          outFs.delete(dirToConcat.getPath(), true);
          outFs.rename(tmpPath, dirToConcat.getPath());
        } else if (!filesToConcat.isEmpty()) {
          Path concatPath = filesToConcat.remove(0);
          try {
            outFs.concat(concatPath, filesToConcat.toArray(new Path[filesToConcat.size()]));
          } catch (Exception e) {
            concatPath = dirToConcat.getPath().suffix("_off");
            FSDataOutputStream concatenated = outFs.create(concatPath);
            byte[] buffer = new byte[1024*1024];
            for (Path fileToContact : filesToConcat) {
              FSDataInputStream in = outFs.open(fileToContact);
              int bytesRead;
              do {
                bytesRead = in.read(buffer, 0, buffer.length);
                if (bytesRead > 0)
                  concatenated.write(buffer, 0, bytesRead);
              } while (bytesRead > 0);
            }
            concatenated.close();
          }
          Path tmpPath = dirToConcat.getPath().suffix("_tmp");
          outFs.rename(concatPath, tmpPath);
          outFs.delete(dirToConcat.getPath(), true);
          outFs.rename(tmpPath, dirToConcat.getPath());
        }
      }
    }
  }
  
  
  private static void distributedCopy(Path inputPath, Path outputPath,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, DistributedCopy.class);
    job.setJobName("distcp3");
    // Set input
    job.setInputFormat(BlockInputFormat.class);
    BlockInputFormat.addInputPath(job, inputPath);

    // Set output
    job.setOutputFormat(BlockOutputFormat.class);
    BlockOutputFormat.setOutputPath(job, outputPath);
    job.setOutputCommitter(BlockOutputCommitter.class);
    
    // Set number of mappers/reducers
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(0);
    
    // Run the job
    JobClient.runJob(job);
  }

  private static void printUsage() {
    System.out.println("Copies a file or filder using distributed copy");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input path>: (*) Path to input file");
    System.out.println("<output path>: (*) Path to the output file");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    Path inputFile = params.getInputPath();
    Path outputFile = params.getOutputPath();
    
    long t1 = System.currentTimeMillis();
    distributedCopy(inputFile, outputFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total processing time: "+(t2-t1)+" millis");
  }

}
