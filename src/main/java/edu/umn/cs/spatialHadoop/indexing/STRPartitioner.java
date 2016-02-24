/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A partitioner that partitioner data using the STR bulk loading algorithm.
 * @author Ahmed Eldawy
 *
 */
public class STRPartitioner extends Partitioner {
  /**MBR of the input file*/
  private final Rectangle mbr = new Rectangle();
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
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {
    // Apply the STR algorithm in two rounds
    // 1- First round, sort points by X and split into the given columns
    Arrays.sort(points, new Comparator<Point>() {
      @Override
      public int compare(Point a, Point b) {
        return a.x < b.x? -1 : (a.x > b.x? 1 : 0);
      }});
    // Calculate partitioning numbers based on a grid
    int numSplits = (int) Math.ceil((double)points.length / capacity);
    GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
    gridInfo.calculateCellDimensions(numSplits);
    this.columns = gridInfo.columns;
    this.rows = gridInfo.rows;
    this.xSplits = new double[columns];
    this.ySplits = new double[rows * columns];
    int prev_quantile = 0;
    this.mbr.set(mbr);
    for (int column = 0; column < columns; column++) {
      int col_quantile = (column + 1) * points.length / columns;
      // Determine the x split for this column. Last column has a special handling
      this.xSplits[column] = col_quantile == points.length ? mbr.x2 : points[col_quantile-1].x;
      // 2- Partition this column vertically in the same way
      Arrays.sort(points, prev_quantile, col_quantile, new Comparator<Point>() {
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
        this.ySplits[column * rows + row] = row_quantile == col_quantile ? mbr.y2 : points[row_quantile].y;
      }
      
      prev_quantile = col_quantile;
    }
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
