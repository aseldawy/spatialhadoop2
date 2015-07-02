/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.State;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
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
  
  public static class IndexRecordWriter<S extends Shape> extends RecordWriter<IntWritable, S> {

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
    private Map<Integer, OutputStream> partitionsOutput = new ConcurrentHashMap<Integer, OutputStream>();
    /**A temporary text to serialize objects to before writing to output file*/
    private Text tempText = new Text2();
    /**A list of all threads that are closing partitions in the background*/
    private Vector<Thread> closingThreads = new Vector<Thread>();
    /**The master file contains information about all written partitions*/
    private OutputStream masterFile;
    /**List of errors that happened by a background thread*/
    private Vector<Throwable> listOfErrors = new Vector<Throwable>();
    /**Whether records are replicated in the index or distributed*/
    private boolean replicated;
    /**Type of shapes written to the output. Needed to build local indexes*/
    private S shape;
    /**Local indexer used to index each partition (optional)*/
    private LocalIndexer localIndexer;

    public IndexRecordWriter(TaskAttemptContext task, Path outPath) throws IOException, InterruptedException {
      this(task, Integer.toString(task.getTaskAttemptID().getTaskID().getId()), outPath, null);
    }
    
    public IndexRecordWriter(TaskAttemptContext task, String name, Path outPath,
        Progressable progress)
        throws IOException, InterruptedException {
      Configuration conf = task.getConfiguration();
      String sindex = conf.get("sindex");
      this.replicated = conf.getBoolean("replicate", false);
      this.outFS = outPath.getFileSystem(conf);
      this.outPath = outPath;
      this.partitioner = Partitioner.getPartitioner(conf);
      Class<? extends LocalIndexer> localIndexerClass = conf.getClass(LocalIndexer.LocalIndexerClass, null, LocalIndexer.class);
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
        int partitionToClose = -id - 1;
        this.closePartition(partitionToClose);
      } else {
        // An actual object that we need to write
        OutputStream output = getOrCreateDataOutput(id);
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
    }

    /**
     * Close a file that is currently open for a specific partition. Returns a
     * background thread that will continue all close-related logic.
     * @param progress 
     * 
     * @param partitionToClose
     */
    private void closePartition(final int id) {
      final Partition partitionInfo = partitionsInfo.get(id);
      final OutputStream outStream = partitionsOutput.get(id);
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
              throw new RuntimeException("Could not remove closing thread");
            }
            // Start more background threads if needed
            int numRunningThreads = 0;
            try {
              for (int i_thread = 0; i_thread < closingThreads.size() &&
                  numRunningThreads < MaxClosingThreads; i_thread++) {
                Thread thread = closingThreads.elementAt(i_thread);
                synchronized(thread) {
                  switch (thread.getState()) {
                  case NEW:
                    // Start the thread and fall through to increment the counter
                    thread.start();
                  case RUNNABLE:
                  case BLOCKED:
                  case WAITING:
                  case TIMED_WAITING:
                    // No need to start. Just increment number of threads
                    numRunningThreads++;
                    break;
                  case TERMINATED: // Do nothing.
                    // Should never happen as each thread removes itself from
                    // the list before completion 
                  }
                }
              }
            } catch (ArrayIndexOutOfBoundsException e) {
              // No problem. The array of threads might have gone empty
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

      if (closingThreads.size() < MaxClosingThreads) {
        // Start the thread in the background and make sure it started before
        // adding it to the list of threads to avoid an exception when other
        // thread tries to start it after it is in the queue
        closeThread.start();
        try {
          while (closeThread.getState() == State.NEW) {
            Thread.sleep(1000);
            LOG.info("Waiting for thread #"+closeThread.getId()+" to start");
          }
        } catch (InterruptedException e) {}
      }
      closingThreads.add(closeThread);
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
    private OutputStream getOrCreateDataOutput(int id) throws IOException {
      OutputStream out = partitionsOutput.get(id);
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
          out = new BufferedOutputStream(new FileOutputStream(tempFile));
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
    public void close(TaskAttemptContext task) throws IOException {
      try {
        // Close any open partitions
        for (Integer id : partitionsInfo.keySet()) {
          closePartition(id);
          if (task != null)
            task.progress();
        }
        if (task != null)
          task.setStatus("Closing! "+closingThreads.size()+" remaining");
        // Wait until all background threads are closed
        try {
          while (!closingThreads.isEmpty()) {
            Thread thread = closingThreads.firstElement();
            while (thread.isAlive()) {
              try {
                thread.join(10000);
                if (task != null)
                  task.progress();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            if (task != null)
              task.setStatus("Closing! "+closingThreads.size()+" remaining");
          }
        } catch (ArrayIndexOutOfBoundsException e) {
          // The array of threads has gone empty. Nothing to do
        }
        if (task != null)
          task.setStatus("All closed");
        // All threads are now closed. Check if errors happened
        if (!listOfErrors.isEmpty()) {
          for (Throwable t : listOfErrors)
            LOG.error("Error in thread", t);
          throw new RuntimeException("Encountered "+listOfErrors.size()+" errors in background thread");
        }
      } finally {
        // Close the master file to ensure there are no open files
        masterFile.close();
      }
    }
  }
  
  
  /**
   * Output committer that concatenates all master files into one master file.
   * @author Ahmed Eldawy
   *
   */
  public static class IndexerOutputCommitter extends FileOutputCommitter {
    
    /**Job output path*/
    private Path outPath;

    public IndexerOutputCommitter(Path outputPath, TaskAttemptContext context)
        throws IOException {
      super(outputPath, context);
      this.outPath = outputPath;
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      Configuration conf = context.getConfiguration();
      
      FileSystem outFs = outPath.getFileSystem(conf);

      // Concatenate all master files into one file
      FileStatus[] resultFiles = outFs.listStatus(outPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().contains("_master");
        }
      });
      
      if (resultFiles.length == 0) {
        LOG.warn("No _master files were written by reducers");
      } else {
        String sindex = conf.get("sindex");
        Path masterPath = new Path(outPath, "_master." + sindex);
        OutputStream destOut = outFs.create(masterPath);
        Path wktPath = new Path(outPath, "_"+sindex+".wkt");
        PrintStream wktOut = new PrintStream(outFs.create(wktPath));
        wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
        Text tempLine = new Text2();
        Partition tempPartition = new Partition();
        final byte[] NewLine = new byte[] {'\n'};
        for (FileStatus f : resultFiles) {
          LineReader in = new LineReader(outFs.open(f.getPath()));
          while (in.readLine(tempLine) > 0) {
            destOut.write(tempLine.getBytes(), 0, tempLine.getLength());
            destOut.write(NewLine);
            tempPartition.fromText(tempLine);
            wktOut.println(tempPartition.toWKT());
          }
          in.close();
          outFs.delete(f.getPath(), false); // Delete the copied file
        }
        wktOut.close();
        destOut.close();
      }
    }
  }

  
  @Override
  public RecordWriter<IntWritable, S> getRecordWriter(TaskAttemptContext task)
      throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(task, "").getParent();
    return new IndexRecordWriter<S>(task, file);
  }
  
  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext task)
      throws IOException {
    Path jobOutputPath = getOutputPath(task);
    return new IndexerOutputCommitter(jobOutputPath, task);
  }
}
