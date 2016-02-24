/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.indexing.RTree;

public class RTreeGridRecordWriter<S extends Shape> extends GridRecordWriter<S> {
  public static final Log LOG = LogFactory.getLog(RTreeGridRecordWriter.class);
  
  /**
   * Whether to use the fast mode for building RTree or not.
   * {@link RTree#bulkLoadWrite(byte[], int, int, int, java.io.DataOutput, Shape, boolean)}
   */
  protected boolean fastRTree;
  
  /**The maximum storage (in bytes) that can be accepted by the user*/
  protected int maximumStorageOverhead;

  /**
   * Initializes a new RTreeGridRecordWriter.
   * @param outDir Output path for the job
   * @param job The corresponding job
   * @param prefix A prefix to use for output files for uniqueness
   * @param cells The cells used to partition the written shapes
   * @throws IOException
   */
  public RTreeGridRecordWriter(Path outDir, JobConf job, String prefix,
      CellInfo[] cells) throws IOException {
    super(outDir, job, prefix, cells);
    LOG.info("Writing to RTrees");

    // Determine the size of each RTree to decide when to flush a cell
    Configuration conf = fileSystem.getConf();
    this.fastRTree = conf.get(SpatialSite.RTREE_BUILD_MODE, "fast").equals("fast");
    this.maximumStorageOverhead =
        (int) (conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f) * blockSize);
  }
  
  @Override
  protected synchronized void writeInternal(int cellIndex, S shape)
      throws IOException {
    if (cellIndex < 0) {
      // This indicates a close cell command
      super.writeInternal(cellIndex, shape);
      return;
    }
    // Convert to text representation to test new file size
    text.clear();
    shape.toText(text);
    // Check if inserting this object will increase the degree of the R-tree
    // above the threshold
    int new_data_size =
        intermediateCellSize[cellIndex] + text.getLength() + NEW_LINE.length;
    int bytes_available = (int) (blockSize - 8 - new_data_size);
    if (bytes_available < maximumStorageOverhead) {
      // Check if writing this new record will take storage overhead beyond the
      // available bytes in the block
      int degree = 4096 / RTree.NodeSize;
      int rtreeStorageOverhead =
          RTree.calculateStorageOverhead(intermediateCellRecordCount[cellIndex], degree);
      if (rtreeStorageOverhead > bytes_available) {
        LOG.info("Early flushing an RTree with data "+
            intermediateCellSize[cellIndex]);
        // Writing this element will get the degree above the threshold
        // Flush current file and start a new file
        super.writeInternal(-cellIndex, null);
      }
    }
    
    super.writeInternal(cellIndex, shape);
  }
  
  /**
   * Closes a cell by writing all outstanding objects and closing current file.
   * Then, the file is read again, an RTree is built on top of it and, finally,
   * the file is written again with the RTree built.
   */
  @Override
  protected Path flushAllEntries(Path intermediateCellPath,
      OutputStream intermediateCellStream, Path finalCellPath) throws IOException {
    // Close stream to current intermediate file.
    intermediateCellStream.close();

    // Read all data of the written file in memory
    byte[] cellData = new byte[(int) new File(intermediateCellPath.toUri()
        .getPath()).length()];
    InputStream cellIn = new FileInputStream(intermediateCellPath.toUri()
        .getPath());
    cellIn.read(cellData);
    cellIn.close();

    // Build an RTree over the elements read from file
    // It should create a new stream
    DataOutputStream cellStream =
      (DataOutputStream) createFinalCellStream(finalCellPath);
    cellStream.writeLong(SpatialSite.RTreeFileMarker);
    int degree = 4096 / RTree.NodeSize;
    RTree.bulkLoadWrite(cellData, 0, cellData.length, degree, cellStream,
        stockObject.clone(), fastRTree);
    cellStream.close();
    cellData = null; // To allow GC to collect it
    
    return finalCellPath;
  }
  
  @Override
  protected OutputStream getIntermediateCellStream(int cellIndex)
      throws IOException {
    if (intermediateCellStreams[cellIndex] == null) {
      // For grid file, we write directly to the final file
      File tempFile = File.createTempFile(String.format("%05d", cellIndex), "rtree");
      intermediateCellStreams[cellIndex] = new BufferedOutputStream(
          new FileOutputStream(tempFile));
      intermediateCellPath[cellIndex] = new Path(tempFile.getPath());
    }
    return intermediateCellStreams[cellIndex];
  }
  
  @Override
  protected Path getFinalCellPath(int cellIndex) throws IOException {
    Path finalCellPath = super.getFinalCellPath(cellIndex);
    return new Path(finalCellPath.getParent(), finalCellPath.getName()+".rtree");
  }
}
