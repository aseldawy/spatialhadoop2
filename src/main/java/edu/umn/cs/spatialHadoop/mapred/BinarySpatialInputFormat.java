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
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.net.NetworkTopology;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;

class IndexedRectangle extends Rectangle {
  int index;

  public IndexedRectangle(int index, Rectangle r) {
    super(r);
    this.index = index;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    return index == ((IndexedRectangle)obj).index;
  }
}

/**
 * An input format that reads a pair of files simultaneously and returns
 * a key for one of them and the value as a pair of values.
 * It generates a CombineFileSplit for each pair of blocks returned by the
 * BlockFilter. 
 * @author Ahmed Eldawy
 *
 */
public abstract class BinarySpatialInputFormat<K extends Writable, V extends Writable>
    extends FileInputFormat<PairWritable<K>, PairWritable<V>> {
  
  private static final Log LOG = LogFactory.getLog(BinarySpatialInputFormat.class);
  
  private static final double SPLIT_SLOP = 1.1;   // 10% slop
  
  @SuppressWarnings("unchecked")
  @Override
  public InputSplit[] getSplits(final JobConf job, int numSplits) throws IOException {
    // Get a list of all input files. There should be exactly two files.
    final Path[] inputFiles = getInputPaths(job);
    GlobalIndex<Partition> gIndexes[] = new GlobalIndex[inputFiles.length];
    
    BlockFilter blockFilter = null;
    try {
      Class<? extends BlockFilter> blockFilterClass =
        job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      if (blockFilterClass != null) {
        // Get all blocks the user wants to process
        blockFilter = blockFilterClass.newInstance();
        blockFilter.configure(job);
      }
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    }

    if (blockFilter != null) {
      // Extract global indexes from input files

      for (int i_file = 0; i_file < inputFiles.length; i_file++) {
        FileSystem fs = inputFiles[i_file].getFileSystem(job);
        gIndexes[i_file] = SpatialSite.getGlobalIndex(fs, inputFiles[i_file]);
      }
    }
    
    final Vector<CombineFileSplit> matchedSplits = new Vector<CombineFileSplit>();
    if (gIndexes[0] == null || gIndexes[1] == null) {
      // Join every possible pair (Cartesian product)
      InputSplit[][] inputSplits = new InputSplit[inputFiles.length][];
      
      for (int i_file = 0; i_file < inputFiles.length; i_file++) {
        JobConf temp = new JobConf(job);
        setInputPaths(temp, inputFiles[i_file]);
        inputSplits[i_file] = super.getSplits(temp, 1);
      }
      LOG.info("Doing a Cartesian product of blocks: "+
          inputSplits[0].length+"x"+inputSplits[1].length);
      for (InputSplit split1 : inputSplits[0]) {
        for (InputSplit split2 : inputSplits[1]) {
          CombineFileSplit combinedSplit = (CombineFileSplit) FileSplitUtil
              .combineFileSplits(job, (FileSplit)split1, (FileSplit)split2);
          matchedSplits.add(combinedSplit);
        }
      }
    } else {
      // Filter block pairs by the BlockFilter
      blockFilter.selectCellPairs(gIndexes[0], gIndexes[1],
        new ResultCollector2<Partition, Partition>() {
          @Override
          public void collect(Partition p1, Partition p2) {
              try {
                List<FileSplit> splits1 = new ArrayList<FileSplit>();
                Path path1 = new Path(inputFiles[0], p1.filename);
                splitFile(job, path1, splits1);
                
                List<FileSplit> splits2 = new ArrayList<FileSplit>();
                Path path2 = new Path(inputFiles[1], p2.filename);
                splitFile(job, path2, splits2);
                
                for (FileSplit split1 : splits1) {
                  for (FileSplit split2 : splits2) {
                    matchedSplits.add((CombineFileSplit) FileSplitUtil
                        .combineFileSplits(job, split1, split2));
                  }
                }
                
              } catch (IOException e) {
                e.printStackTrace();
              }
          }
        }
      );
    }

    LOG.info("Matched "+matchedSplits.size()+" combine splits");

    // Return all matched splits
    return matchedSplits.toArray(new InputSplit[matchedSplits.size()]);
  }

  public void splitFile(JobConf job, Path path, List<FileSplit> splits)
      throws IOException {
    NetworkTopology clusterMap = new NetworkTopology();
    FileSystem fs = path.getFileSystem(job);
    FileStatus file = fs.getFileStatus(path);
    long length = file.getLen();
    BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
    if (length != 0) { 
      long blockSize = file.getBlockSize();
      long splitSize = blockSize;

      long bytesRemaining = length;
      while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
        String[] splitHosts = getSplitHosts(blkLocations, 
            length-bytesRemaining, splitSize, clusterMap);
        splits.add(new FileSplit(path, length-bytesRemaining, splitSize, 
            splitHosts));
        bytesRemaining -= splitSize;
      }
      
      if (bytesRemaining != 0) {
        splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, 
                   blkLocations[blkLocations.length-1].getHosts()));
      }
    } else if (length != 0) {
      String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
      splits.add(new FileSplit(path, 0, length, splitHosts));
    } else { 
      //Create empty hosts array for zero length files
      splits.add(new FileSplit(path, 0, length, new String[0]));
    }
  }

}
