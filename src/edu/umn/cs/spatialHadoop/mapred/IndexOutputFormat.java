/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Partitioner;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;

/**
 * @author Ahmed Eldawy
 *
 */
public class IndexOutputFormat<S extends Shape>
  extends FileOutputFormat<IntWritable, S> {
  
  private static final Log LOG = LogFactory.getLog(IndexOutputFormat.class);
  
  
  /**New line marker to separate records*/
  protected static byte[] NEW_LINE;
  
  static {
    try {
      NEW_LINE = System.getProperty("line.separator", "\n").getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      throw new RuntimeException("Cannot retrieve system line separator", e);
    }
  }
  
  public static class IndexRecordWriter<S extends Shape> implements RecordWriter<IntWritable, S> {

    /**The partitioner used by the current job*/
    private Partitioner partitioner;
    /**The output file system*/
    private FileSystem outFS;
    /**The path where output files are written*/
    private Path outPath;
    
    /**Information of partitions being written*/
    private Map<Integer, Partition> partitionsInfo = new HashMap<Integer, Partition>();
    /**
     * DataOutputStream for all partitions being written. It needs to be an
     * instance of stream so that it can be closed later.
     */
    private Map<Integer, DataOutputStream> partitionsOutput = new HashMap<Integer, DataOutputStream>();
    /**A temporary text to serialize objects to before writing to output file*/
    private Text tempText = new Text2();
    /**A list of all threads that are closing partitions in the background*/
    private Vector<Thread> closingThreads = new Vector<Thread>();
    /**To indicate progress*/
    private Progressable progress;
    /**The master file contains information about all written partitions*/
    private DataOutputStream masterFile;
    /**List of errors that happened by a background thread*/
    private Vector<Throwable> listOfErrors = new Vector<Throwable>();
    /**Whether records are replicated in the index or distributed*/
    private boolean replicated;

    public IndexRecordWriter(JobConf job, Path outPath) throws IOException {
      this(job, null, outPath, null);
    }
    
    public IndexRecordWriter(JobConf job, String name, Path outPath,
        Progressable progress)
        throws IOException {
      String sindex = job.get("sindex");
      this.replicated = job.getBoolean("replicate", false);
      this.progress = progress;
      this.outFS = outPath.getFileSystem(job);
      this.outPath = outPath;
      this.partitioner = Partitioner.getPartitioner(job);
      Path masterFilePath = name == null ?
          new Path(outPath, String.format("_master_.%s", sindex)) :
            new Path(outPath, String.format("_master_%s_.%s", name, sindex));
      this.masterFile = outFS.create(masterFilePath);
    }

    public IndexRecordWriter(Partitioner partitioner, boolean replicate,
        String sindex, Path outPath, Configuration conf)
            throws IOException {
      this.replicated = replicate;
      this.outFS = outPath.getFileSystem(conf);
      this.outPath = outPath;
      this.partitioner = partitioner;
      Path masterFilePath =  new Path(outPath, "_master_."+ sindex);
      this.masterFile = outFS.create(masterFilePath);
    }
    
    @Override
    public void write(IntWritable partitionID, S value) throws IOException {
      int id = partitionID.get();
      if (id < 0) {
        // An indicator to close a partition
        int partitionToClose = -(id + 1);
        this.closePartition(partitionToClose);
      } else {
        // An actual object that we need to write
        DataOutput output = getOrCreateDataOutput(id);
        tempText.clear();
        value.toText(tempText);
        byte[] bytes = tempText.getBytes();
        output.write(bytes, 0, tempText.getLength());
        output.write(NEW_LINE);
        Partition partition = partitionsInfo.get(id);
        partition.recordCount++;
        partition.size += tempText.getLength() + NEW_LINE.length;
        partition.expand(value);
      }
      if (progress != null)
        progress.progress();
    }

    /**
     * Close a file that is currently open for a specific partition. Returns a
     * background thread that will continue all close-related logic.
     * 
     * @param partitionToClose
     */
    private void closePartition(final int id) {
      final Partition partitionInfo = partitionsInfo.get(id);
      final DataOutputStream outStream = partitionsOutput.get(id);
      Thread closeThread = new Thread() {
        @Override
        public void run() {
          try {
            outStream.close();
            if (replicated) {
              // If data is replicated, we can shrink down the size of the
              // partition to keep partitions disjoint
              partitionInfo.set(partitionInfo.getIntersection(partitioner.getPartition(id)));
            }
            Text partitionText = partitionInfo.toText(new Text());
            synchronized (masterFile) {
              // Write partition information to the master file
              masterFile.write(partitionText.getBytes(), 0, partitionText.getLength());
              masterFile.write(NEW_LINE);
            }
            if (!closingThreads.remove(Thread.currentThread())) {
              throw new RuntimeException("Couldn't remove closing thread");
            }
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error closing partition: "+partitionInfo, e);
          }
        }
      };
      
      closeThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          listOfErrors.add(e);
        }
      });
      
      // Clear partition information to indicate we can no longer write to it
      partitionsInfo.remove(id);
      partitionsOutput.remove(id); 

      // Start the thread in the background and keep track of it
      closingThreads.add(closeThread);
      closeThread.start();
    }

    /**
     * Returns a DataOutput for the given partition. If a file is already open
     * for that partition, the corresponding DataOutput is returned. Otherwise,
     * a new file is created for that partition and the corresponding DataOutput
     * is returned.
     * 
     * @param id - the ID of the partition
     * @return
     * @throws IOException 
     */
    private DataOutput getOrCreateDataOutput(int id) throws IOException {
      DataOutputStream out = partitionsOutput.get(id);
      if (out == null) {
        Path path = getPartitionFile(id);
        out = outFS.create(path);
        Partition partition = new Partition();
        partition.cellId = id;
        // Set the rectangle to the opposite universe so that we can keep
        // expanding it to get the MBR of this partition
        partition.set(Double.MAX_VALUE, Double.MAX_VALUE,
            -Double.MAX_VALUE, -Double.MAX_VALUE);
        partition.filename = path.getName();
        // Store in the hashtables for further user
        partitionsOutput.put(id,  out);
        partitionsInfo.put(id, partition);
      }
      return out;
    }

    /**
     * Returns a unique name for a file to write the given partition
     * @param id
     * @return
     * @throws IOException 
     */
    private Path getPartitionFile(int id) throws IOException {
      Path partitionPath = new Path(outPath, String.format("part-%05d", id));
      if (outFS.exists(partitionPath)) {
        int i = 0;
        do {
          partitionPath = new Path(outPath, String.format("part-%05d-%03d", id, ++i));
        } while (outFS.exists(partitionPath));
      }
      return partitionPath;
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      try {
        // Close any open partitions
        for (Integer id : partitionsInfo.keySet()) {
          closePartition(id);
          if (reporter != null)
            reporter.progress();
        }
        // Wait until all background threads are close
        // NOTE: Have to use an integer iterator to avoid conflicts if threads
        // are removed in the background while iterating over them
        for (int i = 0; i < closingThreads.size(); i++) {
          Thread thread = closingThreads.elementAt(i);
          while (thread.isAlive()) {
            try {
              thread.join(10000);
              if (reporter != null)
                reporter.progress();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
        // All threads are now closed. Check if errors happened
        if (!listOfErrors.isEmpty()) {
          for (Throwable t : listOfErrors)
            LOG.error(t);
          throw new RuntimeException("Encountered "+listOfErrors.size()+" in background thread");
        }
      } finally {
        // Close the master file to ensure there are no open files
        masterFile.close();
      }
    }
  }

  @Override
  public RecordWriter<IntWritable, S> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {
    Path taskOutputPath = getTaskOutputPath(job, name).getParent();
    return new IndexRecordWriter<S>(job, name, taskOutputPath, progress);
  }

  
}
