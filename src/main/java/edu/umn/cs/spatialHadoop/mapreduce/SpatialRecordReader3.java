/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapreduce;

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
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;

/**
 * @author Ahmed Eldawy
 *
 */
public class SpatialRecordReader3<V extends Shape> extends
    RecordReader<Partition, Iterable<V>> {
  
  private static final Log LOG = LogFactory.getLog(SpatialRecordReader3.class);

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
  protected Partition cellMBR;
  
  /**
   * The input stream that reads directly from the input file.
   * If the file is not compressed, this stream is the same as #in.
   * Otherwise, this is the raw (compressed) input stream. This stream is used
   * only to calculate the progress of the input file.
   */
  private FSDataInputStream directIn;
  /** Input stream that reads data from input file */
  private InputStream in;
  /**Determine current position to report progress*/
  private Seekable progressPosition;

  /**Used to read text lines from the input*/
  private LineReader lineReader;

  /**The shape used to parse input lines*/
  private V stockShape;

  private Text tempLine;

  /**Input query range if specified in the job configuration*/
  private Shape inputQueryRange;
  /**The MBR of the input query. Used to apply duplicate avoidance technique*/
  private Rectangle inputQueryMBR;

  private CompressionCodecFactory compressionCodecFactory;

  private ShapeIterator<V> value;
  
  /**
   * Number of bytes read from the input so far. This is used to determine when
   * to stop when reading from the input directly. We canno simply rely on the
   * position of the input file because LineReader might buffer some data in
   * memory without actually processing it.
   */
  private long bytesRead;

  /**
   * Context of the map task. Used to increment counters.
   */
  private Counter inputRecordsCounter;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context != null ? context.getConfiguration() : new Configuration();
    if (context != null && context instanceof MapContext)
      inputRecordsCounter = ((MapContext)context).getCounter(Task.Counter.MAP_INPUT_RECORDS);
    initialize(split, conf);
  }

  public void initialize(InputSplit split, Configuration conf)
      throws IOException, InterruptedException {
    FileSplit fsplit = (FileSplit) split;
    if (compressionCodecFactory == null)
      compressionCodecFactory = new CompressionCodecFactory(conf);

    LOG.info("Open a SpatialRecordReader to split: "+split);
    this.path = fsplit.getPath();
    this.start = fsplit.getStart();
    this.end = this.start + split.getLength();
    this.fs = this.path.getFileSystem(conf);
    this.directIn = fs.open(this.path);
    codec = compressionCodecFactory.getCodec(this.path);
    
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
        progressPosition = cIn;
      } else {
        // Non-splittable input, need to start from the beginning
        CompressionInputStream cIn = codec.createInputStream(directIn, decompressor);
        in = cIn;
        progressPosition = cIn;
      }
    } else {
      // Non-compressed file, seek to the desired position and use this stream
      // to get the progress and position
      directIn.seek(start);
      in = directIn;
      progressPosition = directIn;
    }
    this.stockShape = (V) OperationsParams.getShape(conf, "shape");
    this.tempLine = new Text();
    
    this.lineReader = new LineReader(in);
    bytesRead = 0;
    
    if (this.start != 0) {
      // Skip until first end-of-line reached
      bytesRead += lineReader.readLine(tempLine);
    }
    if (conf.get(SpatialInputFormat3.InputQueryRange) != null) {
      // Retrieve the input query range to apply on all records
      this.inputQueryRange = OperationsParams.getShape(conf,
          SpatialInputFormat3.InputQueryRange);
      this.inputQueryMBR = this.inputQueryRange.getMBR();
    }
    
    // Check if there is an associated global index to read cell boundaries
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, path.getParent());
    if (gindex == null) {
      cellMBR = new Partition();
      cellMBR.filename = path.getName();
      cellMBR.invalidate();
    } else {
      // Set from the associated partition in the global index
      for (Partition p : gindex) {
        if (p.filename.equals(this.path.getName()))
          cellMBR = p;
      }
    }
    
    this.value = new ShapeIterator<V>();
    value.setShape(stockShape);
  }
  
  public long getPos() throws IOException {
    if (codec != null) {
      // Input is compressed. Report the progress as indicated by the
      // decompressor
      return progressPosition.getPos();
    } else {
      // Input is not compressed. Report the progress as indicated by number
      // of bytes read from the input
      return start + bytesRead;
    }
  }
  
  /**
   * Reads the next line from input and return true if a line was read.
   * If no more lines are available in this split, a false is returned.
   * @param value The text object to fill with the next line
   * @return <code>true</code> if a line was read; <code>false</code> otherwise.
   * @throws IOException If an error occurs while reading from disk.
   */
  protected boolean nextLine(Text value) throws IOException {
    while (getPos() <= end) {
      value.clear();

      int lineLength;
      // Read the first line from stream
      if ((lineLength = lineReader.readLine(value)) <= 0) {
        // Indicates an end of stream
        return false;
      }
      
      // Append the part read from stream to the part extracted from buffer
      bytesRead += lineLength;
      
      if (value.getLength() > 1) {
        // Read a non-empty line. Note that end-of-line character is included
        return true;
      }
    }
    // Reached end of file
    return false;
  }
  
  protected boolean isMatched(Shape shape) {
    // Match with the query
    if (inputQueryRange != null && (shape == null || !shape.isIntersected(inputQueryRange)))
      return false;
    // Check if we need to apply a duplicate avoidance step or not
    if (!cellMBR.isValid() || inputQueryMBR == null)
      return true;
    // Apply reference point duplicate avoidance technique
    Rectangle shapeMBR = shape.getMBR();
    double reference_x = Math.max(inputQueryMBR.x1, shapeMBR.x1);
    double reference_y = Math.max(inputQueryMBR.y1, shapeMBR.y1);
    return cellMBR.contains(reference_x, reference_y);
  }
  
  /**
   * Reads next shape from input and returns true. If no more shapes are left
   * in the split, a false is returned. This function first reads a line
   * by calling the method {@link #nextLine(Text)} then parses the returned
   * line by calling {@link Shape#fromText(Text)} on that line. If no stock
   * shape is set, a {@link NullPointerException} is thrown.
   * @param s A mutable shape object to update with the next value
   * @return <code>true</code> if an object was read; <code>false</code> if end-of-file was reached.
   * @throws IOException If an error happens while reading from disk
   */
  protected boolean nextShape(V s) throws IOException {
    do {
      if (!nextLine(tempLine))
        return false;
      s.fromText(tempLine);
    } while (!isMatched(s));
    return true;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    value.setSpatialRecordReader(this);
    return value.hasNext();
  }

  @Override
  public Partition getCurrentKey() throws IOException, InterruptedException {
    return cellMBR;
  }

  @Override
  public Iterable<V> getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
        (directIn.getPos() - start) / (float)(end - start));
    }
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
  
  /**
   * An iterator that iterates over all shapes in the input file
   * @author Eldawy
   */
  public static class ShapeIterator<V extends Shape>
      implements Iterator<V>, Iterable<V> {
    protected V shape;
    protected V nextShape;
    private SpatialRecordReader3<V> srr;
    
    public ShapeIterator() {
    }

    public void setSpatialRecordReader(SpatialRecordReader3<V> srr) {
      this.srr = srr;
      try {
        if (shape != null)
          nextShape = (V) shape.clone();
        if (nextShape != null && !srr.nextShape(nextShape))
            nextShape = null;
      } catch (IOException e) {
        throw new RuntimeException("Error reading from file", e);
      }
    }
    
    public void setShape(V shape) {
      this.shape = shape;
      this.nextShape = (V) shape.clone();
      try {
        if (srr != null && !srr.nextShape(nextShape))
            nextShape = null;
      } catch (IOException e) {
        throw new RuntimeException("Error eading from file", e);
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
        if (srr.inputRecordsCounter != null)
          srr.inputRecordsCounter.increment(1);
        return shape;
      } catch (IOException e) {
        throw new RuntimeException("Error reading from file", e);
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
