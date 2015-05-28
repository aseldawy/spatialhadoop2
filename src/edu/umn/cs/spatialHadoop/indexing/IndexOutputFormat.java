/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

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

import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;

/**
 * @author Ahmed Eldawy
 *
 */
public class IndexOutputFormat<S extends Shape>
  extends FileOutputFormat<IntWritable, S> {
  
  private static final Log LOG = LogFactory.getLog(IndexOutputFormat.class);
  
  /**Maximum number of active closing threads*/
  private static final int MaxClosingThreads = Runtime.getRuntime().availableProcessors() * 2;
  
  
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
    private Map<Integer, Partition> partitionsInfo = new ConcurrentHashMap<Integer, Partition>();
    /**Temporary files written for each partition before it is locally indexed*/
    private Map<Integer, File> tempFiles = new ConcurrentHashMap<Integer, File>();
    /**
     * DataOutputStream for all partitions being written. It needs to be an
     * instance of stream so that it can be closed later.
     */
    private Map<Integer, DataOutputStream> partitionsOutput = new ConcurrentHashMap<Integer, DataOutputStream>();
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
    /**Type of shapes written to the output. Needed to build local indexes*/
    private S shape;
    /**Local indexer used to index each partition (optional)*/
    private LocalIndexer localIndexer;

    public IndexRecordWriter(JobConf job, Path outPath) throws IOException, InterruptedException {
      this(job, null, outPath, null);
    }
    
    public IndexRecordWriter(JobConf job, String name, Path outPath,
        Progressable progress)
        throws IOException, InterruptedException {
      String sindex = job.get("sindex");
      this.replicated = job.getBoolean("replicate", false);
      this.progress = progress;
      this.outFS = outPath.getFileSystem(job);
      this.outPath = outPath;
      this.partitioner = Partitioner.getPartitioner(job);
      Class<? extends LocalIndexer> localIndexerClass = job.getClass(LocalIndexer.LocalIndexerClass, null, LocalIndexer.class);
      if (localIndexerClass != null) {
        try {
          this.localIndexer = localIndexerClass.newInstance();
          localIndexer.setup(job);
        } catch (InstantiationException e) {
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
      Path masterFilePath = name == null ?
          new Path(outPath, String.format("_master.%s", sindex)) :
            new Path(outPath, String.format("_master_%s.%s", name, sindex));
      this.masterFile = outFS.create(masterFilePath);
    }

    public IndexRecordWriter(Partitioner partitioner, boolean replicate,
        String sindex, Path outPath, Configuration conf)
            throws IOException, InterruptedException {
      this.replicated = replicate;
      this.outFS = outPath.getFileSystem(conf);
      this.outPath = outPath;
      this.partitioner = partitioner;
      Class<? extends LocalIndexer> localIndexerClass = conf.getClass(
          LocalIndexer.LocalIndexerClass, null, LocalIndexer.class);
      if (localIndexerClass != null) {
        try {
          this.localIndexer = localIndexerClass.newInstance();
          localIndexer.setup(conf);
        } catch (InstantiationException e) {
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
      Path masterFilePath =  new Path(outPath, "_master."+ sindex);
      this.masterFile = outFS.create(masterFilePath);
    }
    
    @Override
    public void write(IntWritable partitionID, S value) throws IOException {
      int id = partitionID.get();
      if (id < 0) {
        // An indicator to close a partition
        int partitionToClose = -(id + 1);
        this.closePartition(partitionToClose, progress);
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
        if (shape == null)
          shape = (S) value.clone();
      }
      if (progress != null)
        progress.progress();
    }

    /**
     * Close a file that is currently open for a specific partition. Returns a
     * background thread that will continue all close-related logic.
     * @param progress 
     * 
     * @param partitionToClose
     */
    private void closePartition(final int id, Progressable progress) {
      while (closingThreads.size() >= MaxClosingThreads) {
        // Wait if there are too many closing threads
        try {
          closingThreads.firstElement().join(10000);
          progress.progress();
        } catch (RuntimeException e) {
        } catch (InterruptedException e) {
        }
      }
      final Partition partitionInfo = partitionsInfo.get(id);
      final DataOutputStream outStream = partitionsOutput.get(id);
      final File tempFile = tempFiles.get(id);
      Thread closeThread = new Thread() {
        @Override
        public void run() {
          try {
            outStream.close();
            
            if (localIndexer != null) {
              // Build a local index for that file
              try {
                Path indexedFilePath = getPartitionFile(id);
                partitionInfo.filename = indexedFilePath.getName();
                localIndexer.buildLocalIndex(tempFile, indexedFilePath, shape);
                // Temporary file no longer needed
                tempFile.delete();
              } catch (InterruptedException e) {
                throw new RuntimeException("Error building local index", e);
              }
            }
            
            if (replicated) {
              // If data is replicated, we need to shrink down the size of the
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
      tempFiles.remove(id);

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
        // First time to write in this partition. Store its information
        Partition partition = new Partition();

        if (localIndexer == null) {
          // No local index needed. Write to the final file directly
          Path path = getPartitionFile(id);
          out = outFS.create(path);
          partition.filename = path.getName();
        } else {
          // Write to a temporary file that will later get indexed
          File tempFile = File.createTempFile(String.format("part-%05d", id), "lindex");
          out = new DataOutputStream(new FileOutputStream(tempFile));
          tempFiles.put(id, tempFile);
        }
        partition.cellId = id;
        // Set the rectangle to the opposite universe so that we can keep
        // expanding it to get the MBR of this partition
        partition.set(Double.MAX_VALUE, Double.MAX_VALUE,
            -Double.MAX_VALUE, -Double.MAX_VALUE);
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
      String format = "part-%05d";
      if (localIndexer != null)
        format += "."+localIndexer.getExtension();
      Path partitionPath = new Path(outPath, String.format(format, id));
      if (outFS.exists(partitionPath)) {
        format = "part-%05d-%03d";
        if (localIndexer != null)
          format += "."+localIndexer.getExtension();
        int i = 0;
        do {
          partitionPath = new Path(outPath, String.format(format, id, ++i));
        } while (outFS.exists(partitionPath));
      }
      return partitionPath;
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      try {
        // Close any open partitions
        for (Integer id : partitionsInfo.keySet()) {
          closePartition(id, reporter);
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
            LOG.error("Error in thread", t);
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
    try {
      return new IndexRecordWriter<S>(job, name, taskOutputPath, progress);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error initializing record writer", e);
    }
  }

  
}
