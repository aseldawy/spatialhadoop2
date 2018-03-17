/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.LocalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reads a file that contains R-trees.
 * @author Ahmed Eldawy
 *
 */
public class LocalIndexRecordReader<V extends Shape> extends
    RecordReader<Partition, Iterable<? extends V>> {

  private static final Log LOG = LogFactory.getLog(LocalIndexRecordReader.class);

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

  /**The input stream that reads directly from the input file.*/
  private FSDataInputStream in;

  /**The shape used to parse input lines*/
  private V stockShape;

  /**The nd offsets of the index to be returned next*/
  private long indexEnd;

  /**Value to be returned*/
  private Iterable<? extends V> value;

  /**Optional query range*/
  private Shape inputQueryRange;

  /**The MBR of the input query. Used to apply duplicate avoidance technique*/
  private Rectangle inputQueryMBR;

  /**The local index class used to open the files*/
  private Class<? extends LocalIndex> localIndexClass;

  /**The underlying configuration*/
  private Configuration conf;

  public LocalIndexRecordReader(Class<? extends LocalIndex> localIndexClass) {
    this.localIndexClass = localIndexClass;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context != null? context.getConfiguration() : new Configuration();
    initialize(split, conf);
  }

  public void initialize(InputSplit split, Configuration conf)
      throws IOException, InterruptedException {
    this.conf = conf;
    LOG.info("Open a SpatialRecordReader to split: "+split);
    FileSplit fsplit = (FileSplit) split;
    this.path = fsplit.getPath();
    this.start = fsplit.getStart();
    this.end = this.start + split.getLength();
    this.fs = this.path.getFileSystem(conf);
    this.in = fs.open(this.path);

    // Non-compressed file, seek to the desired position and use this stream
    // to get the progress and position
    in.seek(start);

    this.stockShape = (V) OperationsParams.getShape(conf, "shape");

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
      cellMBR.invalidate();
    } else {
      // Set from the associated partition in the global index
      for (Partition p : gindex) {
        if (p.filename.equals(this.path.getName()))
          cellMBR = p;
      }
    }
    indexEnd = end;
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Each key-value pair is an MBR of a partition and the objects in the corresponding R-tree
    if (indexEnd <= 0)
      return false;
    // Seek to the next local index
    in.seek(indexEnd - 4);
    int indexSize = in.readInt();
    long indexStart = indexEnd - indexSize;

    try {
      LocalIndex<V> localIndex = localIndexClass.newInstance();
      localIndex.setup(conf);
      in.seek(indexStart);
      localIndex.read(in, indexStart, indexEnd, stockShape);
      this.indexEnd = indexStart; // Prepare to read the next local index

      if (inputQueryRange != null) {
        // Apply a query query
        value = localIndex.search(inputQueryMBR.x1, inputQueryMBR.y1, inputQueryMBR.x2, inputQueryMBR.y2);
        if (cellMBR.isValid()) {
          // Need a duplicate avoidance step
          value = new DuplicateAvoidanceIterator<V>(cellMBR, inputQueryMBR, value.iterator());
        }
        return value.iterator().hasNext();
      } else {
        // Return all elements
        // TODO consider returning a reference to the local index itself
        value = localIndex.scanAll();
        return value.iterator().hasNext();
      }
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating the local index", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating the local index", e);
    }
  }
  
  public long getPos() throws IOException {
    return in.getPos();
  }

  @Override
  public Partition getCurrentKey() throws IOException, InterruptedException {
    return cellMBR;
  }
  
  public static class DuplicateAvoidanceIterator<V extends Shape> implements Iterable<V>, Iterator<V> {
    /**MBR of the containing cell to run the reference point technique*/
    private Rectangle cellMBR;
    /**MBR of the query range*/
    private Rectangle inputQueryMBR;
    /**All underlying values*/
    private Iterator<? extends V> values;
    /**The value that will be returned next*/
    private V nextValue;

    public DuplicateAvoidanceIterator(Rectangle cellMBR,
        Rectangle inputQueryMBR, Iterator<? extends V> values) {
      this.cellMBR = cellMBR;
      this.inputQueryMBR = inputQueryMBR;
      this.values = values;
      prefetchNextValue();
    }
    
    public boolean isMatched(Shape shape) {
      // Apply reference point duplicate avoidance technique
      Rectangle shapeMBR = shape.getMBR();
      double reference_x = Math.max(inputQueryMBR.x1, shapeMBR.x1);
      double reference_y = Math.max(inputQueryMBR.y1, shapeMBR.y1);
      return cellMBR.contains(reference_x, reference_y);
    }
    
    @Override
    public Iterator<V> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return nextValue != null;
    }

    @Override
    public V next() {
      V currentValue = (V) nextValue.clone();
      prefetchNextValue();
      return currentValue;
    }

    private void prefetchNextValue() {
      if (!values.hasNext()) {
        nextValue = null;
        return;
      }
      do {
        nextValue = values.next();
      } while (values.hasNext() && !isMatched(nextValue));
      if (nextValue == null || !isMatched(nextValue))
        nextValue = null;
    }
    
    @Override
    public void remove() {
      throw new RuntimeException("Non-implemented method");
    }
    
  }

  @Override
  public Iterable<? extends V> getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
        (getPos() - start) / (float)(end - start));
    }
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
      in = null;
    }
  }

}
