/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
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

public class RTreeGridRecordWriter<S extends Shape> extends GridRecordWriter<S> {
  public static final Log LOG = LogFactory.getLog(RTreeGridRecordWriter.class);
  
  /**
   * Keeps the number of elements written to each cell so far.
   * Helps calculating the overhead of RTree indexing
   */
  private int[] cellCount;

  /**Size in bytes of intermediate files written so far*/
  private int[] intermediateFileSize;

  /**
   * Whether to use the fast mode for building RTree or not.
   * @see RTree#bulkLoadWrite(byte[], int, int, int, java.io.DataOutput, boolean)
   */
  protected boolean fastRTree;
  
  /**The maximum storage (in bytes) that can be accepted by the user*/
  protected int maximumStorageOverhead;

  /**
   * Initializes a new RTreeGridRecordWriter.
   * @param fileSystem - of output file
   * @param outDir - output file path
   * @param cells - the cells used to partition the input
   * @param overwrite - whether to overwrite existing files or not
   * @throws IOException
   */
  public RTreeGridRecordWriter(Path outDir, JobConf job, String prefix,
      CellInfo[] cells) throws IOException {
    super(outDir, job, prefix, cells);
    LOG.info("Writing to RTrees");

    // Initialize the counters for each cell
    cellCount = new int[this.cells.length];
    intermediateFileSize = new int[this.cells.length];
    
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
        intermediateFileSize[cellIndex] + text.getLength() + NEW_LINE.length;
    int bytes_available = (int) (blockSize - 8 - new_data_size);
    if (bytes_available < maximumStorageOverhead) {
      // Check if writing this new record will take storage overhead beyond the
      // available bytes in the block
      int degree = 4096 / RTree.NodeSize;
      int rtreeStorageOverhead =
          RTree.calculateStorageOverhead(cellCount[cellIndex], degree);
      if (rtreeStorageOverhead > bytes_available) {
        LOG.info("Early flushing an RTree with data "+
            intermediateFileSize[cellIndex]);
        // Writing this element will get the degree above the threshold
        // Flush current file and start a new file
        super.writeInternal(-cellIndex, null);
      }
    }
    
    super.writeInternal(cellIndex, shape);
    intermediateFileSize[cellIndex] += text.getLength() + NEW_LINE.length;
    cellCount[cellIndex]++;
  }
  
  protected void closeCell(int cellIndex) throws IOException {
    super.closeCell(cellIndex);
    cellCount[cellIndex] = 0;
    intermediateFileSize[cellIndex] = 0;
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
    RTree<S> rtree = new RTree<S>();
    rtree.setStockObject((S) stockObject.clone());
    // It should create a new stream
    DataOutputStream cellStream =
      (DataOutputStream) createFinalCellStream(finalCellPath);
    cellStream.writeLong(SpatialSite.RTreeFileMarker);
    int degree = 4096 / RTree.NodeSize;
    rtree.bulkLoadWrite(cellData, 0, cellData.length, degree, cellStream,
        fastRTree);
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
}
