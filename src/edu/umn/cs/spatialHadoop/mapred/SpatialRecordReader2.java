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
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
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
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * Parses an unindexed input file into spatial records
 * @author Ahmed Eldawy
 *
 */
public class SpatialRecordReader2<V extends Shape>
  implements RecordReader<Partition, SpatialRecordReader2.ShapeIterator<V>> {
  
  private static final Log LOG = LogFactory.getLog(SpatialRecordReader2.class);
  
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
  protected Rectangle cellMbr;
  
  /**
   * The input stream that reads directly from the input file.
   * If the file is not compressed, this stream is the same as #in.
   * Otherwise, this is the raw (compressed) input stream. This stream is used
   * only to calculate the progress of the input file.
   */
  private FSDataInputStream directIn;
  /** Input stream that reads data from input file */
  private InputStream in;
  /**An object that is used to read the current file position*/
  private Seekable filePosition;
  /**The offset of last byte read from the input*/
  private long pos;

  /**Used to read text lines from the input*/
  private LineReader lineReader;

  /**The shape used to parse input lines*/
  private V stockShape;

  private Text tempLine;

  public SpatialRecordReader2(Configuration job, FileSplit split,
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
        in = cIn;
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        // take pos from compressed stream as we adjusted both start and end
        // to match with the compressed file
        filePosition = cIn;
      } else {
        // Non-splittable input, need to start from the beginning
        in = codec.createInputStream(directIn, decompressor);
        filePosition = directIn;
      }
    } else {
      // Non-compressed file, seek to the desired position and use this stream
      // to get the progress and position
      directIn.seek(start);
      in = directIn;
      filePosition = directIn;
    }
    this.pos = start;
    this.lineReader = new LineReader(in);
    
    this.stockShape = (V) OperationsParams.getShape(job, "shape");
    this.tempLine = new Text();
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
    if (codec != null && filePosition != null) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
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
  public Partition createKey() {
    return new Partition();
  }
  
  /**
   * Creates a shape iterator and associated it with the correct shape type
   */
  @Override
  public ShapeIterator<V> createValue() {
    ShapeIterator<V> shapeIter = new ShapeIterator<V>();
    shapeIter.setShape(stockShape);
    return shapeIter;
  }
  
  /**
   * Reads the next line from input and return true if a line was read.
   * If no more lines are available in this split, a false is returned.
   * @param value
   * @return
   * @throws IOException
   */
  protected boolean nextLine(Text value) throws IOException {
    while (getFilePosition() <= end) {
      value.clear();
      int b = 0;
      
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
  protected boolean nextShape(V s) throws IOException {
    if (!nextLine(tempLine))
      return false;
    s.fromText(tempLine);
    return true;
  }
  
  @Override
  public boolean next(Partition key, ShapeIterator<V> value) throws IOException {
    key.set(cellMbr);
    value.setSpatialRecordReader(this);
    return value.hasNext();
  }
  
  /**
   * An iterator that iterates over all shapes in the input file
   * @author Eldawy
   */
  public static class ShapeIterator<V extends Shape>
      implements Iterator<V>, Iterable<V> {
    protected V shape;
    protected V nextShape;
    private SpatialRecordReader2<V> srr;
    
    public ShapeIterator() {
    }

    public void setSpatialRecordReader(SpatialRecordReader2<V> srr) {
      this.srr = srr;
      try {
        if (shape != null)
          nextShape = (V) shape.clone();
        if (nextShape != null && !srr.nextShape(nextShape))
            nextShape = null;
      } catch (IOException e) {
      }
    }
    
    public void setShape(V shape) {
      this.shape = shape;
      this.nextShape = (V) shape.clone();
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
    public V next() {
      try {
        if (nextShape == null)
          return null;
        // Swap Shape and nextShape and read next
        V temp = shape;
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
    public Iterator<V> iterator() {
      return this;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Unsupported method ShapeIterator#remove");
    }
    
  }
  
}
