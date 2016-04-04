/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.indexing.RTree;

/**
 * A base class to read shapes from files. It reads either single shapes,
 * list of shapes, or R-trees. It automatically detects the format of the
 * underlying block and parses it accordingly.
 * 
 * The class implement the RecordReader interface allowing it to be used in
 * MapReduce programs with an appropriate InputFormat. The key is always
 * a {@link Rectangle} that indicates the MBR of the corresponding partition.
 * In case of a non-indexed file, the key is an invalid rectangle. See
 * {@link Rectangle#isValid()}.
 * 
 * @see ShapeLineRecordReader
 * @see ShapeRecordReader
 * @see ShapeArrayRecordReader
 * @see RTreeRecordReader
 * @author Ahmed Eldawy
 *
 */
public abstract class SpatialRecordReader<K, V> implements RecordReader<K, V> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReader.class);
  
  /**Maximum number of shapes to read in one operation to return as array*/
  private int maxShapesInOneRead;
  /**Maximum size in bytes that can be read in one read*/
  private int maxBytesInOneRead;
  
  enum BlockType { HEAP, RTREE};
  
  /** First offset that is read from the input */
  protected long start;
  /** Last offset to stop at */
  protected long end;
  /** Position of the next byte to read/prase from the input */
  protected long pos;
  /** Input stream that reads from file */
  private InputStream in;
  
  private Seekable filePosition;
  private CompressionCodec codec;
  private Decompressor decompressor;
  
  /** Reads lines from text files */
  protected LineReader lineReader;
  /** A temporary text to read lines from lineReader */
  protected Text tempLine = new Text();
  /** Some bytes that were read from the stream but not parsed yet */
  protected byte[] buffer;

  /** File system of the file being parsed */
  private FileSystem fs;

  /** The path of the parsed file */
  private Path path;

  /** Block size for the read file. Used with RTrees */
  protected long blockSize;

  /** The boundary of the partition currently being read */
  protected Rectangle cellMbr;

  /**The type of the currently parsed block*/
  protected BlockType blockType;

  /**
   * The input stream that reads directly from the input file.
   * If the file is not compressed, this stream is the same as the in.
   * Otherwise, this is the raw (compressed) input stream. This stream is used
   * to calculate the progress of the input file.
   */
  private FSDataInputStream directIn;
  
  /**
   * Initialize from an input split
   * @param split
   * @param conf
   * @param reporter
   * @param index
   * @throws IOException
   */
  public SpatialRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    this(conf, split.getStartOffsets()[index], split.getLength(index),
        split.getPath(index));
  }
  
  /**
   * Initialize from a FileSplit
   * @param job
   * @param split
   * @throws IOException
   */
  public SpatialRecordReader(Configuration job, FileSplit split) throws IOException {
    this(job, split.getStart(), split.getLength(), split.getPath());
  }

  /**
   * Initialize from a path and file range
   * @param job
   * @param s
   * @param l
   * @param p
   * @throws IOException
   */
  public SpatialRecordReader(Configuration job, long s, long l, Path p) throws IOException {
    this.start = s;
    this.end = s + l;
    this.path = p;
    LOG.info("Open a SpatialRecordReader to file: "+p+"["+s+","+(s+l)+")");
    this.fs = this.path.getFileSystem(job);
    this.directIn = fs.open(this.path);
    this.blockSize = fs.getFileStatus(this.path).getBlockSize();
    this.cellMbr = new Rectangle();
    

    codec = new CompressionCodecFactory(job).getCodec(this.path);

    if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn =
            ((SplittableCompressionCodec)codec).createInputStream(
                directIn, decompressor, start, end,
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = cIn;
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn; // take pos from compressed stream
      } else {
        in = codec.createInputStream(directIn, decompressor);
        filePosition = directIn;
      }
    } else {
      directIn.seek(start);
      in = directIn;
      filePosition = directIn;
    }
    this.pos = start;
    this.maxShapesInOneRead = job.getInt(SpatialSite.MaxShapesInOneRead, 1000000);
    this.maxBytesInOneRead = job.getInt(SpatialSite.MaxBytesInOneRead, 32*1024*1024);

    initializeReader();
  }
  
  /**
   * Construct from an input stream already set to the first byte to read.
   * @param in
   * @param offset
   * @param endOffset
   * @throws IOException
   */
  public SpatialRecordReader(InputStream in, long offset, long endOffset)
      throws IOException {
    this.in = in;
    this.start = offset;
    this.end = endOffset;
    this.pos = offset;
    this.cellMbr = new Rectangle();
    initializeReader();
  }
  
  /**
   * Returns the current position of the file being parsed. This is equal to
   * the number of bytes consumed from disk regardless of whether the file is
   * compressed or not.
   * This function is used to report the progress.
   * @return
   * @throws IOException
   */
  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }
  
  /**
   * Tells whether the file being parsed is compressed or not
   * @return
   */
  private boolean isCompressedInput() {
    return codec != null;
  }

  /**
   * Returns the current position in the data file. If the file is not
   * compressed, this is equal to the value returned by {@link #getFilePosition()}.
   * However, if the file is compressed, this value indicates the position
   * in the decompressed stream.
   */
  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void close() throws IOException {
    try {
    if (lineReader != null) {
      lineReader.close();
    } else if (in != null) {
      in.close();
    }
    lineReader = null;
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
   * Initializes the reader to read from the input stream or file.
   * First, it initializes the MBR of the partition being read if the file
   * is globally indexed. It also detects whether the file is R-tree indexed
   * or not which allows it to skip the R-tree if not needed to be read.
   * @throws IOException
   */
  protected boolean initializeReader() throws IOException {
    // Get the cell info for the current block
    cellMbr.invalidate(); // Initialize to invalid rectangle
    if (path != null) {
      GlobalIndex<Partition> globalIndex =
          SpatialSite.getGlobalIndex(fs, path.getParent());
      if (globalIndex != null) {
        for (Partition partition : globalIndex) {
          if (partition.filename.equals(path.getName())) {
            cellMbr.set(partition);
          }
        }
      }
    }
    
    // Read the first part of the block to determine its type
    buffer = new byte[8];
    int bufferLength = in.read(buffer);
    if (bufferLength <= 0) {
      buffer = null;
    } else if (bufferLength < buffer.length) {
      byte[] old_buffer = buffer;
      buffer = new byte[bufferLength];
      System.arraycopy(old_buffer, 0, buffer, 0, bufferLength);
    }
    if (buffer != null && Arrays.equals(buffer, SpatialSite.RTreeFileMarkerB)) {
      blockType = BlockType.RTREE;
      pos += 8;
      // Ignore the signature
      buffer = null;
    } else {
      blockType = BlockType.HEAP;
      // The read buffer might contain some data that must be read
      // File is text file
      lineReader = new LineReader(in);
  
      // Skip the first line unless we are reading the first block in file
      // For globally indexed blocks, never skip the first line in the block
      boolean skipFirstLine = getPos() != 0;
      if (buffer != null && skipFirstLine) {
        // Search for the first occurrence of a new line
        int eol = RTree.skipToEOL(buffer, 0);
        // If we found an end of line in the buffer, we do not need to skip
        // a line from the open stream. This happens if the EOL returned is
        // beyond the end of buffer and the buffer is not a complete line
        // by itself
        boolean skip_another_line_from_stream = eol >= buffer.length &&
            buffer[buffer.length - 1] != '\n';
        if (eol < buffer.length) {
          // Found an EOL in the buffer and there are some remaining bytes
          byte[] tmp = new byte[buffer.length - eol];
          System.arraycopy(buffer, eol, tmp, 0, tmp.length);
          buffer = tmp;
          // Advance current position to skip the first partial line
          this.pos += eol;
        } else {
          // Did not find an EOL in the buffer or found it at the very end
          pos += buffer.length;
          // Buffer does not contain any useful data
          buffer = null;
        }
        
        if (skip_another_line_from_stream) {
          // Didn't find an EOL in the buffer, need to skip it from the stream
          pos += lineReader.readLine(tempLine, Integer.MAX_VALUE, (int)(end - pos));
          if (pos >= end) {
            // Special case when the whole split is in the middle of a line
            // Skip the split
            // Increase position beyond end to ensure the next call to
            // nextLine would return false
            pos++;
          }
        }
      }
    }
    
    
    return true;
  }

  /**
   * Reads the next line from input and return true if a line was read.
   * If no more lines are available in this split, a false is returned.
   * @param value
   * @return
   * @throws IOException
   */
  protected boolean nextLine(Text value) throws IOException {
    if (blockType == BlockType.RTREE && pos == 8) {
      // File is positioned at the RTree header
      // Skip the header and go to first data object in file
      pos += RTree.skipHeader(in);
      LOG.info("Skipped R-tree to position: "+pos);
      // Reinitialize record reader at the new position
      lineReader = new LineReader(in);
    }
    while (getFilePosition() <= end) {
      value.clear();
      int b = 0;
      if (buffer != null) {
        // Read the first line encountered in buffer
        int eol = RTree.skipToEOL(buffer, 0);
        b += eol;
        value.append(buffer, 0, eol);
        if (eol < buffer.length) {
          // There are still some bytes remaining in buffer
          byte[] tmp = new byte[buffer.length - eol];
          System.arraycopy(buffer, eol, tmp, 0, tmp.length);
          buffer = tmp;
        } else {
          buffer = null;
        }
        // Check if a complete line has been read from the buffer
        byte last_byte = value.getBytes()[value.getLength()-1];
        if (last_byte == '\n' || last_byte == '\r')
          return true;
      }
      
      // Read the first line from stream
      Text temp = new Text();
      b += lineReader.readLine(temp);
      if (b == 0) {
        // Indicates an end of stream
        return false;
      }
      pos += b;
      
      // Append the part read from stream to the part extracted from buffer
      value.append(temp.getBytes(), 0, temp.getLength());
      
      if (value.getLength() > 1) {
        // Read a non-empty line. Note that end-of-line character is included
        return true;
      }
    }
    // Reached end of file
    return false;
  }

  /**
   * Reads next shape from input and returns true. If no more shapes are left
   * in the split, a false is returned. This function first reads a line
   * by calling the method {@link #nextLine(Text)} then parses the returned
   * line by calling {@link Shape#fromText(Text)} on that line. If no stock
   * shape is set, a {@link NullPointerException} is thrown.
   * @param s
   * @return
   * @throws IOException 
   */
  protected boolean nextShape(Shape s) throws IOException {
    if (!nextLine(tempLine))
      return false;
    s.fromText(tempLine);
    return true;
  }
  
  /**
   * Reads all shapes left in the current block in one shot. This function
   * runs a loop where it keeps reading shapes by calling the method
   * {@link #nextShape(Shape)} until one of the following conditions happen.
   * 1. The whole file is read. No more records to read.
   * 2. Number of parsed records reaches the threshold defined by the
   *    configuration parameter spatialHadoop.mapred.MaxShapesPerRead.
   *    To disable this check, set the configuration parameter to -1
   * 3. Total size of parsed data from file reaches the threshold defined by
   *    the configuration parameter spatialHadoop.mapred.MaxBytesPerRead.
   *    To disable this check, set the configuration parameter to -1.
   * 
   * @param shapes
   * @return
   * @throws IOException
   */
  protected boolean nextShapes(ArrayWritable shapes) throws IOException {
    // Prepare a vector that will hold all objects in this 
    Vector<Shape> vshapes = new Vector<Shape>();
    try {
      Shape stockObject = (Shape) shapes.getValueClass().newInstance();
      // Reached the end of this split
      if (getFilePosition() >= end)
        return false;
      
      long initialReadPos = getPos();
      long readBytes = 0;
      
      // Read all shapes in this block
      while ((maxShapesInOneRead <= 0 || vshapes.size() < maxShapesInOneRead) &&
          (maxBytesInOneRead <= 0 || readBytes < maxBytesInOneRead) &&
          nextShape(stockObject)) {
        vshapes.add(stockObject.clone());
        readBytes = getPos() - initialReadPos;
      }

      // Store them in the return value
      shapes.set(vshapes.toArray(new Shape[vshapes.size()]));
      
      return !vshapes.isEmpty();
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    } catch (OutOfMemoryError e) {
      LOG.error("Error reading shapes. Stopped with "+vshapes.size()+" shapes");
      throw e;
    }
    return false;
  }
  
  /**
   * Returns an iterator that iterates over all remaining shapes in the file.
   * @param iter
   * @return
   * @throws IOException
   */
  protected boolean nextShapeIter(ShapeIterator iter) throws IOException {
    iter.setSpatialRecordReader((SpatialRecordReader<?, ? extends Shape>) this);
    return iter.hasNext();
  }
  
  /**
   * An iterator that iterates over all shapes in the input file
   * @author Eldawy
   */
  public static class ShapeIterator implements Iterator<Shape>, Iterable<Shape> {
    protected Shape shape;
    protected Shape nextShape;
    private SpatialRecordReader<?, ? extends Shape> srr;
    
    public ShapeIterator() {
    }

    public void setSpatialRecordReader(SpatialRecordReader<?, ? extends Shape> srr) {
      this.srr = srr;
      try {
        if (shape != null)
          nextShape = shape.clone();
        if (nextShape != null && !srr.nextShape(nextShape))
            nextShape = null;
      } catch (IOException e) {
      }
    }
    
    public void setShape(Shape shape) {
      this.shape = shape;
      this.nextShape = shape.clone();
      try {
        if (srr != null && !srr.nextShape(nextShape))
            nextShape = null;
      } catch (IOException e) {
      }
    }

    public boolean hasNext() {
      if (nextShape == null)
        return false;
      return nextShape != null;
    }

    @Override
    public Shape next() {
      try {
        if (nextShape == null)
          return null;
        // Swap Shape and nextShape and read next
        Shape temp = shape;
        shape = nextShape;
        nextShape = temp;
        
        if (!srr.nextShape(nextShape))
          nextShape = null;
        return shape;
      } catch (IOException e) {
        return null;
      }
    }

    @Override
    public Iterator<Shape> iterator() {
      return this;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Unsupported method ShapeIterator#remove");
    }
    
  }
  
  /**
   * Reads the next RTree from file. The file must be part of an R-tree index.
   * If the file is not locally indexed using an R-tree, a runtime exception
   * is thrown. If the file is locally indexed using an R-tree, the R-tree
   * is consumed from the file and parsed by calling
   * {@link RTree#readFields(DataInput)} on the input stream.
   * @param rtree
   * @return
   * @throws IOException
   */
  protected boolean nextRTree(RTree<? extends Shape> rtree) throws IOException {
    if (blockType == BlockType.RTREE) {
      if (getPos() != 8)
        return false;
      // Signature was already read in initialization.
      buffer = null;
      DataInput dataIn = in instanceof DataInput?
          (DataInput) in : new DataInputStream(in);
      rtree.readFields(dataIn);
      pos++;
      return true;
    } else {
      throw new RuntimeException("Not implemented");
    }
  }
}
