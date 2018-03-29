/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.IllegalArgumentException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import edu.umn.cs.spatialHadoop.io.Text2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;
import org.mortbay.log.Log;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * An interface for spatially partitioning data into partitions.
 * @author Ahmed Eldawy
 *
 */
public abstract class Partitioner implements Writable {
  /**Configuration line for partitioner class*/
  static final String PartitionerClass = "Partitioner.Class";
  static final String PartitionerValue = "Partitioner.Value";
  public static final String PartitionerDisjoint = "Partitioner.Disjoint";

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface GlobalIndexerMetadata {
    /**Whether this global indexer supports disjoint partitions or not*/
    boolean disjoint();

    /**The extension used with the global index*/
    String extension();
  }

  /**
   * Overlap a shape with partitions and calls a matcher for each overlapping
   * partition.
   * @param shape
   * @param matcher
   */
  public abstract void overlapPartitions(Shape shape, ResultCollector<Integer> matcher);
  
  /**
   * Returns only one overlapping partition. If the given shape overlaps more
   * than one partitions, the partitioner returns only one of them according to
   * its own criteria. If it does not overlap any partition, it returns -1
   * which is an invalid partition ID.
   * 
   * @param shape
   * @return
   */
  public abstract int overlapPartition(Shape shape);
  
  /**
   * Returns the details of a specific partition given its ID.
   * @param partitionID
   * @return
   */
  public abstract CellInfo getPartition(int partitionID);
  
  /**
   * Returns the detail of a partition given its index starting at zero
   * and ending at partitionCount() - 1
   * @param index
   * @return
   */
  public abstract CellInfo getPartitionAt(int index);
  
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
      tempFile = new Path("cells_"+(int)(Math.random()*1000000)+".partitions");
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

  /**
   * Create a WKT file form an existing master file to use with GIS software.
   * @param fs
   * @param masterPath
   */
  public static void generateMasterWKT(FileSystem fs, Path masterPath) throws IOException {
    // Write the WKT-formatted master file
    String name = masterPath.getName();
    int lastDot = name.lastIndexOf('.');
    String globalIndexExtension = name.substring(lastDot + 1);
    Path wktPath = new Path(masterPath.getParent(), "_"+globalIndexExtension+".wkt");
    PrintStream wktOut = new PrintStream(fs.create(wktPath));
    wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
    Text tempLine = new Text2();
    Partition tempPartition = new Partition();
    LineReader in = new LineReader(fs.open(masterPath));
    while (in.readLine(tempLine) > 0) {
      tempPartition.fromText(tempLine);
      wktOut.println(tempPartition.toWKT());
    }
    in.close();
    wktOut.close();
  }
}
