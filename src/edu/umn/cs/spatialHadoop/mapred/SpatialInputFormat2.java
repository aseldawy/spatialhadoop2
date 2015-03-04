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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.nasa.HTTPFileSystem;
import edu.umn.cs.spatialHadoop.operations.RangeFilter;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * This input format is used to write any spatial file. It automatically decides
 * the format of the file and instantiates the correct record reader based
 * on its file format.
 * 
 * Notice: The key has to be Rectangle (not Partition) because HDF files
 * generate a key of NASADataset which is not Partition. The actual instance
 * of the key is still returned as a Partition if the input file is partitioned.
 * @author Ahmed Eldawy
 *
 */
public class SpatialInputFormat2<K extends Rectangle, V extends Shape>
    extends FileInputFormat<K, Iterable<V>> {
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public RecordReader<K, Iterable<V>> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    if (split instanceof FileSplit) {
      FileSplit fsplit = (FileSplit) split;
      String extension = FileUtil.getExtensionWithoutCompression(fsplit.getPath());
      // If this extension is for a compression, skip it and take the previous
      // extension
      if (extension.equals("hdf")) {
        // HDF File. Create HDFRecordReader
        return (RecordReader)new HDFRecordReader(job, fsplit,
            job.get(HDFRecordReader.DatasetName),
            job.getBoolean(HDFRecordReader.SkipFillValue, true));
      }
      if (extension.equals("rtree")) {
        // File is locally indexed as RTree
        return (RecordReader)new RTreeRecordReader2<V>(job, (FileSplit)split, reporter);
      }
      // For backward compatibility, check if the file is RTree indexed
      if (SpatialSite.isRTree(fsplit.getPath().getFileSystem(job), fsplit.getPath())) {
        return (RecordReader)new RTreeRecordReader2<V>(job, (FileSplit)split, reporter);
      }
      // Read a non-indexed file
      return (RecordReader)new SpatialRecordReader2<V>(job, (FileSplit)split, reporter);
    } else {
      throw new RuntimeException("Cannot handle splits of type "+split.getClass());
    }
  }
  
  protected void listStatus(final FileSystem fs, Path dir,
      final List<FileStatus> result, BlockFilter filter) throws IOException {
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, dir);
    if (gindex == null || filter == null) {
      // No global index which means we cannot use the filter function
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
          // Recursively go in subdir
          listStatus(fs, status.getPath(), result, filter);
        } else {
          // A file, just add it
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
      // The block filter associated with this job
      BlockFilter blockFilter = null;
      if (job.get(RangeFilter.QueryRange) != null) {
        // This job requires a range query
        blockFilter = new RangeFilter();
      }
      // Retrieve the BlockFilter set by the developers in the JobConf
      Class<? extends BlockFilter> blockFilterClass =
          job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      if (blockFilterClass != null) {
        blockFilter = new CombineBlockFilter(blockFilter,
            blockFilterClass.newInstance());
      }
      if (blockFilter == null) {
        // No block filter specified by user
        LOG.info("No block filter specified");
        return super.listStatus(job);
      }
      // Get all blocks the user wants to process
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
    // Avoid splitting any file in HTTP because it's not supported yet
    if (fs instanceof HTTPFileSystem)
      return false;
    // HDF files are not splittable
    String extension = FileUtil.getExtensionWithoutCompression(file).toLowerCase();
    if (extension.equals("hdf") || extension.equals("rtree"))
      return false;
    final CompressionCodec codec = FileUtil.getCodec(file);
    if (codec != null && !(codec instanceof SplittableCompressionCodec))
      return false;
    // Avoid splitting small files
    try {
      if (fs.getFileStatus(file).getLen() < 150 * 1024 * 1024)
        return false;
      if (SpatialSite.isRTree(fs, file))
        return false;
    } catch (IOException e) {
    }
    return super.isSplitable(fs, file);
  }
}
