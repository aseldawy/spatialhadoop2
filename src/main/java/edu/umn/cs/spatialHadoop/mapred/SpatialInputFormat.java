/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.nasa.HTTPFileSystem;

/**
 * An input format used with spatial data. It filters generated splits before
 * creating record readers.
 * 
 * This class is @deprecated in favor to {@link SpatialInputFormat3}
 * @author Ahmed Eldawy
 *
 */
@Deprecated
public abstract class SpatialInputFormat<K, V> extends FileInputFormat<K, V> {
  
  /**
   * Used to check whether files are compressed or not. Some compressed files
   * (e.g., gz) are not splittable.
   */
  private CompressionCodecFactory compressionCodecs = null;
  
  /**
   * We need to use this way of constructing readers to be able to pass it to
   * CmobineFileRecordReader
   **/
  @SuppressWarnings("rawtypes")
  static final Class[] constructorSignature = new Class[] {
      Configuration.class, FileSplit.class };

  @SuppressWarnings("rawtypes")
  protected Class<? extends RecordReader> rrClass;
  
  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    // Create compressionCodecs to be used by isSplitable method
    if (compressionCodecs == null)
      compressionCodecs = new CompressionCodecFactory(job);
    if (split instanceof FileSplit) {
      FileSplit fsplit = (FileSplit) split;
      try {
        @SuppressWarnings("rawtypes")
        Constructor<? extends RecordReader> rrConstructor;
        rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
        rrConstructor.setAccessible(true);
        return rrConstructor.newInstance(new Object [] {job, fsplit});
      } catch (SecurityException e) {
        throw new RuntimeException("Cannot generate a record reader", e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Cannot generate a record reader", e);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException("Cannot generate a record reader", e);
      } catch (InstantiationException e) {
        throw new RuntimeException("Cannot generate a record reader", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Cannot generate a record reader", e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException("Cannot generate a record reader", e);
      }
    } else {
      throw new RuntimeException("Cannot handle splits of type "+split.getClass());
    }
  }
  
  protected void listStatus(final FileSystem fs, Path dir,
      final List<FileStatus> result, BlockFilter filter) throws IOException {
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, dir);
    if (gindex == null) {
      FileStatus[] listStatus;
      if (OperationsParams.isWildcard(dir)) {
        // Wild card
        listStatus = fs.globStatus(dir);
      } else {
        listStatus = fs.listStatus(dir, SpatialSite.NonHiddenFileFilter);
      }
      // Add all files under this directory
      for (FileStatus status : listStatus) {
        if (status.isDir()) {
          listStatus(fs, status.getPath(), result, filter);
        } else if (status.getPath().getName().toLowerCase().endsWith(".list")) {
          LineRecordReader in = new LineRecordReader(fs.open(status.getPath()), 0, status.getLen(), Integer.MAX_VALUE);
          LongWritable key = in.createKey();
          Text value = in.createValue();
          while (in.next(key, value)) {
            result.add(fs.getFileStatus(new Path(status.getPath().getParent(), value.toString())));
          }
          in.close();
        } else {
          result.add(status);
        }
      }
    } else {
      final Path indexDir = OperationsParams.isWildcard(dir)?
          dir.getParent() : dir;
      // Use the global index to limit files
      filter.selectCells(gindex, new ResultCollector<Partition>() {
        @Override
        public void collect(Partition partition) {
          try {
            Path cell_path = new Path(indexDir, partition.filename);
            if (!fs.exists(cell_path))
              LOG.warn("Matched file not found: "+cell_path);
            result.add(fs.getFileStatus(cell_path));
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }
  
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    try {
      // Create the compressionCodecs to be used later by isSplitable
      if (compressionCodecs == null)
        compressionCodecs = new CompressionCodecFactory(job);
      
      // Retrieve the BlockFilter set by the developers in the JobConf
      Class<? extends BlockFilter> blockFilterClass =
          job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      if (blockFilterClass == null) {
        LOG.info("No block filter specified");
        // No block filter specified by user
        return super.listStatus(job);
      }
      // Get all blocks the user wants to process
      BlockFilter blockFilter;
      blockFilter = blockFilterClass.newInstance();
      blockFilter.configure(job);
      
      // Filter files based on user specified filter function
      List<FileStatus> result = new ArrayList<FileStatus>();
      Path[] inputDirs = getInputPaths(job);
      
      for (Path dir : inputDirs) {
        FileSystem fs = dir.getFileSystem(job);
        listStatus(fs, dir, result, blockFilter);
      }
      
      LOG.info("Spatial filter function matched with "+result.size()+" cells");
      
      return result.toArray(new FileStatus[result.size()]);
    } catch (InstantiationException e) {
      LOG.warn(e);
      return super.listStatus(job);
    } catch (IllegalAccessException e) {
      LOG.warn(e);
      return super.listStatus(job);
    }
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path file) {
    // HDF files are not splittable
    if (file.getName().toLowerCase().endsWith(".hdf"))
      return false;
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec != null && !(codec instanceof SplittableCompressionCodec))
      return false;
    
    try {
      // To avoid opening the file and checking the first 8-bytes to look for
      // an R-tree signature, we never split a file read over HTTP
      if (fs instanceof HTTPFileSystem)
        return false;
      // ... and never split a file less than 150MB to perform better with many small files
      if (fs.getFileStatus(file).getLen() < 150 * 1024 * 1024)
        return false;
      return !SpatialSite.isRTree(fs, file);
    } catch (IOException e) {
      return super.isSplitable(fs, file);
    }
  }
}
