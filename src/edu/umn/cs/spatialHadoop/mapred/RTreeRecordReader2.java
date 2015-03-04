/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.RTree;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

/**
 * Parses an unindexed input file into spatial records
 * @author Ahmed Eldawy
 *
 */
public class RTreeRecordReader2<V extends Shape>
  implements RecordReader<Partition, RTree<V>> {
  
  private static final Log LOG = LogFactory.getLog(RTreeRecordReader2.class);
  
  /**The codec used with the input file*/
  private CompressionCodec codec;
  /**The decompressor (instance) used to decompress the input file*/
  private Decompressor decompressor;

  /** File system of the file being parsed */
  private FileSystem fs;
  /**The path of the input file to read*/
  private Path path;
  /**The offset to start reading the raw (uncompressed) file*/
  private long start;
  /**The last byte to read in the raw (uncompressed) file*/
  private long end;
  
  /** The boundary of the partition currently being read */
  protected Rectangle cellMBR;
  
  /**
   * The input stream that reads directly from the input file.
   * If the file is not compressed, this stream is the same as #in.
   * Otherwise, this is the raw (compressed) input stream. This stream is used
   * only to calculate the progress of the input file.
   */
  private FSDataInputStream directIn;
  /** Input stream that reads data from input file */
  private DataInputStream in;
  /**An object that is used to read the current file position*/
  private Seekable filePosition;

  /**The shape used to parse input lines*/
  private V stockShape;

  /**Start offset of the next tree*/
  private long offsetOfNextTree;

  /**Reporter used to report progress to the MapReduce framework*/
  private Reporter reporter;

  public RTreeRecordReader2(Configuration job, FileSplit split,
      Reporter reporter) throws IOException {
    LOG.info("Open a SpatialRecordReader to split: "+split);
    this.path = split.getPath();
    this.start = split.getStart();
    this.end = this.start + split.getLength();
    this.fs = this.path.getFileSystem(job);
    this.directIn = fs.open(this.path);
    codec = new CompressionCodecFactory(job).getCodec(this.path);
    
    if (codec != null) {
      // Input is compressed, create a decompressor to decompress it
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        // A splittable compression codec, can seek to the desired input pos
        final SplitCompressionInputStream cIn =
            ((SplittableCompressionCodec)codec).createInputStream(
                directIn, decompressor, start, end,
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = new DataInputStream(cIn);
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        // take pos from compressed stream as we adjusted both start and end
        // to match with the compressed file
        filePosition = cIn;
      } else {
        // Non-splittable input, need to start from the beginning
        CompressionInputStream cIn = codec.createInputStream(directIn, decompressor);
        in = new DataInputStream(cIn);
        filePosition = cIn;
      }
    } else {
      // Non-compressed file, seek to the desired position and use this stream
      // to get the progress and position
      directIn.seek(start);
      in = directIn;
      filePosition = directIn;
    }
    byte[] signature = new byte[8];
    in.readFully(signature);
    if (!Arrays.equals(signature, SpatialSite.RTreeFileMarkerB)) {
      throw new RuntimeException("Incorrect signature for RTree");
    }
    this.stockShape = (V) OperationsParams.getShape(job, "shape");
    this.reporter = reporter;
    
    // Check if there is an associated global index to read cell boundaries
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, path.getParent());
    if (gindex == null) {
      cellMBR = new Rectangle();
      cellMBR.invalidate();
    } else {
      // Set from the associated partition in the global index
      for (Partition p : gindex) {
        if (p.filename.equals(this.path.getName()))
          cellMBR = p;
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      in.close();
      in = null;
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
        (directIn.getPos() - start) / (float)(end - start));
    }
  }
  
  /**
   * Returns the current position in the data file. If the file is not
   * compressed, this is equal to the value returned by {@link #getFilePosition()}.
   * However, if the file is compressed, this value indicates the position
   * in the decompressed stream.
   */
  @Override
  public long getPos() throws IOException {
    return filePosition.getPos();
  }

  
  @Override
  public Partition createKey() {
    return new Partition();
  }
  
  /**
   * Creates a shape iterator and associated it with the correct shape type
   */
  @Override
  public RTree<V> createValue() {
    RTree<V> rtree = new RTree<V>();
    rtree.setStockObject(stockShape);
    return rtree;
  }
  
  @Override
  public boolean next(Partition key, RTree<V> value) throws IOException {
    if (offsetOfNextTree > 0) {
      if (codec == null) {
        // Input is not compressed. Just seek to the next RTree
        filePosition.seek(offsetOfNextTree);
      } else {
        // Input is compressed. We must have read the whole R-tree already
      }
    }
    long pos1 = getPos();
    if (getPos() >= end)
      return false;
    key.set(cellMBR);
    value.readFields(in);
    this.offsetOfNextTree = value.getEndOffset();

    // Report progress to the underlying reporter
    if (reporter != null) {
      reporter.incrCounter(Task.Counter.MAP_INPUT_BYTES, value.getEndOffset() - pos1);
      reporter.incrCounter(Task.Counter.MAP_INPUT_RECORDS, value.getElementCount());
      reporter.progress();
    }
    return value.getElementCount() > 0;
  }
}
