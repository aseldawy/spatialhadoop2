/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

/**
 * An interface for spatially partitioning data into partitions.
 * @author Ahmed Eldawy
 *
 */
public abstract class Partitioner implements Writable {
  /**Configuration line for partitioner class*/
  private static final String PartitionerClass = "Partitioner.Class";
  private static final String PartitionerValue = "Partitioner.Value";

  /**
   * Overlap a shape with partitions and calls a matcher for each overlapping
   * partition.
   * @param shape
   * @param matcher
   * @return
   */
  public abstract void overlapPartitions(Shape shape, ResultCollector<Integer> matcher);
  
  /**
   * Returns the details of a specific partition.
   * @param partitionID
   * @return
   */
  public abstract CellInfo getPartition(int partitionID);
  
  /**
   * Returns total number of partitions
   * @return
   */
  public abstract int getPartitionCount();
  
  /**
   * Sets the class and value of a partitioner in the given job
   * @param conf
   * @param partitioner
   * @throws IOException
   */
  public static void setPartitioner(Configuration conf, Partitioner partitioner) throws IOException {
    conf.setClass(PartitionerClass, partitioner.getClass(), Partitioner.class);
    Path tempFile;
    FileSystem fs = FileSystem.get(conf);
    do {
      tempFile = new Path("cells_"+(int)(Math.random()*1000000)+".cells");
    } while (fs.exists(tempFile));
    FSDataOutputStream out = fs.create(tempFile);
    partitioner.write(out);
    out.close();
    
    fs.deleteOnExit(tempFile);

    DistributedCache.addCacheFile(tempFile.toUri(), conf);
    conf.set(PartitionerValue, tempFile.getName());
  }
  
  /**
   * Retrieves the value of a partitioner for a given job.
   * @param conf
   * @return
   */
  public static Partitioner getPartitioner(Configuration conf) {
    Class<? extends Partitioner> klass = conf.getClass(PartitionerClass, Partitioner.class).asSubclass(Partitioner.class);
    if (klass == null)
      return null;
    try {
      Partitioner partitioner = klass.newInstance();

      String partitionerFile = conf.get(PartitionerValue);
      if (partitionerFile != null) {
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
        for (Path cacheFile : cacheFiles) {
          if (cacheFile.getName().contains(partitionerFile)) {
            FSDataInputStream in = FileSystem.getLocal(conf).open(cacheFile);
            partitioner.readFields(in);
            in.close();
          }
        }
      }
      return partitioner;
    } catch (InstantiationException e) {
      Log.warn("Error instantiating partitioner", e);
      return null;
    } catch (IllegalAccessException e) {
      Log.warn("Error instantiating partitioner", e);
      return null;
    } catch (IOException e) {
      Log.warn("Error retrieving partitioner value", e);
      return null;
    }
  }
}
