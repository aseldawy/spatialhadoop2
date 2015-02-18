/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * A partitioner that partitioner data using the STR bulk loading algorithm.
 * @author Ahmed Eldawy
 *
 */
public class STRPartitioner extends Partitioner {
  private static final Log LOG = LogFactory.getLog(STRPartitioner.class);

  /**MBR of the input file*/
  private Rectangle mbr;
  /**Number of rows and columns*/
  private int columns, rows;
  /**Locations of vertical strips*/
  private double[] xSplits;
  /**Locations of horizontal strips for each vertical strip*/
  private double[] ySplits;
  
  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public STRPartitioner() {
  }

  /**
   * Constructs a new grid partitioner which is used for indexing
   * @param inPath
   * @param job
   * @throws IOException 
   */
  public static STRPartitioner createIndexingPartitioner(Path inPath,
      Path outPath, JobConf job) throws IOException {
    long t1 = System.currentTimeMillis();
    String gridSize = job.get("grid");
    Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    int columns, rows;
    if (gridSize != null) {
      // Use user-specified grid size
      String[] parts = gridSize.split(",");
      columns = Integer.parseInt(parts[0]);
      rows = Integer.parseInt(parts[1]);
    } else {
      // Auto-detect grid size
      long inSize = FileUtil.getPathSize(inPath.getFileSystem(job), inPath);
      FileSystem outFS = outPath.getFileSystem(job);
      long outBlockSize = outFS.getDefaultBlockSize(outPath);
      GridInfo gridInfo = new GridInfo(inMBR.x1, inMBR.y1, inMBR.x2, inMBR.y2);
      gridInfo.calculateCellDimensions(inSize, outBlockSize);
      columns = gridInfo.columns;
      rows = gridInfo.rows;
    }
    LOG.info("Using an STR partitioner of size "+columns+"x"+rows);
    
    // Draw a random sample of the input file
    final Vector<Point> vsample = new Vector<Point>();
    
    float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);

    LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
    ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
      @Override
      public void collect(Point value) {
        vsample.add(value.clone());
      }
    };
    OperationsParams params2 = new OperationsParams(job);
    params2.setFloat("ratio", sample_ratio);
    params2.setLong("size", sample_size);
    params2.setClass("outshape", Point.class, Shape.class);
    Sampler.sample(new Path[] {inPath}, resultCollector, params2);
    Point[] sample = vsample.toArray(new Point[vsample.size()]);
    vsample.clear();
    long t2 = System.currentTimeMillis();
    System.out.println("Total time for sampling in millis: "+(t2-t1));
    LOG.info("Finished reading a sample of "+sample.length+" records");
    
    // Apply the STR algorithm in two rounds
    // 1- First round, sort points by X and split into the given columns
    Arrays.sort(sample, new Comparator<Point>() {
      @Override
      public int compare(Point a, Point b) {
        return a.x < b.x? -1 : (a.x > b.x? 1 : 0);
      }});
    
    STRPartitioner p = new STRPartitioner();
    p.columns = columns;
    p.rows = rows;
    p.xSplits = new double[columns];
    p.ySplits = new double[rows * columns];
    p.mbr = new Rectangle(inMBR);
    int prev_quantile = 0;
    for (int column = 0; column < columns; column++) {
      int col_quantile = (column + 1) * sample.length / columns;
      // Determine the x split for this column. Last column has a special handling
      p.xSplits[column] = col_quantile == sample.length ? inMBR.x2 : sample[col_quantile-1].x;
      // 2- Partition this column vertically in the same way
      Arrays.sort(sample, prev_quantile, col_quantile, new Comparator<Point>() {
        @Override
        public int compare(Point a, Point b) {
          return a.y < b.y? -1 : (a.y > b.y? 1 : 0);
        }
      });
      // Compute y-splits for this column
      for (int row = 0; row < rows; row++) {
        int row_quantile = (prev_quantile * (rows - (row+1)) +
            col_quantile * (row+1)) / rows;
        // Determine y split for this row. Last row has a special handling
        p.ySplits[column * rows + row] = row_quantile == col_quantile ? inMBR.y2 : sample[row_quantile].y;
      }
      
      prev_quantile = col_quantile;
    }
    
    return p;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(columns);
    out.writeInt(rows);
    ByteBuffer bbuffer = ByteBuffer.allocate((xSplits.length + ySplits.length) * 8);
    for (double xSplit : xSplits)
      bbuffer.putDouble(xSplit);
    for (double ySplit : ySplits)
      bbuffer.putDouble(ySplit);
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Did not calculate buffer size correctly");
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (mbr == null)
      mbr = new Rectangle();
    mbr.readFields(in);
    columns = in.readInt();
    rows = in.readInt();
    xSplits = new double[columns];
    ySplits = new double[columns * rows];
    
    int bufferLength = (xSplits.length + ySplits.length) * 8;
    byte[] buffer = new byte[bufferLength];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < xSplits.length; i++)
      xSplits[i] = bbuffer.getDouble();
    for (int i = 0; i < ySplits.length; i++)
      ySplits[i] = bbuffer.getDouble();
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Error reading STR partitioner");
  }
  
  @Override
  public int getPartitionCount() {
    return ySplits.length;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return;
    // Replicate to all overlapping partitions
    // Find first and last matching columns
    int col1 = Arrays.binarySearch(xSplits, shapeMBR.x1);
    if (col1 < 0)
      col1 = -col1 - 1; // Adjust the position if value not found
    int col2 = Arrays.binarySearch(xSplits, shapeMBR.x2);
    if (col2 < 0)
      col2 = -col2 - 1; // Adjust the position if value not found

    for (int col = col1; col <= col2; col++) {
      // For each column, find all matching rows
      int cell1 = Arrays.binarySearch(ySplits, col * rows, (col+1) * rows, shapeMBR.y1);
      if (cell1 < 0)
        cell1 = -cell1 - 1;
      int cell2 = Arrays.binarySearch(ySplits, col * rows, (col+1) * rows, shapeMBR.y2);
      if (cell2 < 0)
        cell2 = -cell2 - 1;

      for (int cell = cell1; cell <= cell2; cell++)
        matcher.collect(cell);
    }
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    if (shape == null)
      return -1;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return -1;
    // Assign to only one partition
    Point center = shapeMBR.getCenterPoint();
    int col = Arrays.binarySearch(xSplits, center.x);
    if (col < 0)
      col = -col - 1;
    int cell = Arrays.binarySearch(ySplits, col * rows, (col+1)*rows, center.y);
    if (cell < 0)
      cell = -cell - 1;
    return cell;
  }
  
  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(index);
  }

  @Override
  public CellInfo getPartition(int id) {
    int col = id / rows;
    int row = id % rows;
    double y2 = ySplits[id];
    double y1 = row == 0? mbr.y1 : ySplits[id-1];
    double x2 = xSplits[col];
    double x1 = col == 0? mbr.x1 : xSplits[col-1];
    return new CellInfo(id, x1, y1, x2, y2);
  }
}
