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
import java.lang.IllegalArgumentException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A partitioner that partitioner data using a uniform grid.
 * If a shape overlaps multiple grids, it replicates it to all overlapping
 * partitions.
 * @author Ahmed Eldawy
 *
 */
@Partitioner.GlobalIndexerMetadata(disjoint = true)
public class GridPartitioner extends Partitioner {
  private static final Log LOG = LogFactory.getLog(GridPartitioner.class);

  /**Origin of the grid*/
  protected double x, y;
  
  /**Number of tiles*/
  protected int numTiles;

  /**Width and height of a single tile*/
  protected double tileWidth, tileHeight;

  /**Total number of columns and rows within the input range*/
  transient protected int numColumns, numRows;

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public GridPartitioner() {
  }

  public GridPartitioner(Rectangle mbr, int columns, int rows) {
    this.x = mbr.x1;
    this.y = mbr.y1;
    this.numTiles = rows * columns;
    this.numColumns = columns;
    this.numRows = rows;
    this.tileWidth = mbr.getWidth() / columns;
    this.tileHeight = mbr.getHeight() / rows;
  }

  public GridPartitioner(Rectangle mbr, int numPartitions) {
    x = mbr.x1;
    y = mbr.y1;
    numTiles = numPartitions;
    // Create square tiles
    double areaPerTile = mbr.getWidth() * mbr.getHeight() / numPartitions;
    this.tileWidth = this.tileHeight = Math.sqrt(areaPerTile);
    this.numColumns = (int) Math.round(mbr.getWidth() / tileWidth);
    this.numRows = (int) Math.round(mbr.getHeight() / tileHeight);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(x);
    out.writeDouble(y);
    out.writeDouble(tileWidth);
    out.writeDouble(tileHeight);
    out.writeInt(numTiles);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.x = in.readDouble();
    this.y = in.readDouble();
    this.tileWidth = in.readDouble();
    this.tileHeight = in.readDouble();
    this.numTiles = in.readInt();
    this.numColumns = (int) Math.round(Math.sqrt(numTiles));
    this.numRows = (int) Math.ceil(numTiles / numColumns);
  }
  
  @Override
  public int getPartitionCount() {
    return numTiles;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return;
    int col1, col2, row1, row2;
    col1 = (int)Math.floor((shapeMBR.x1 - x) / tileWidth);
    col2 = (int)Math.ceil((shapeMBR.x2 - x) / tileWidth);
    row1 = (int)Math.floor((shapeMBR.y1 - y) / tileHeight);
    row2 = (int)Math.ceil((shapeMBR.y2 - y) / tileHeight);
    
    if (col1 < 0) col1 = 0;
    if (row1 < 0) row1 = 0;
    for (int col = col1; col < col2; col++)
      for (int row = row1; row < row2; row++)
        matcher.collect(getCellNumber(col, row));
  }
  
  private int getCellNumber(int col, int row) {
    return row * numColumns + col;
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    if (shape == null)
      return -1;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return -1;
    Point centerPoint = shapeMBR.getCenterPoint();
    int col = (int)Math.floor((centerPoint.x - x) / tileWidth);
    int row = (int)Math.floor((centerPoint.y - y) / tileHeight);
    return getCellNumber(col, row);
  }

  @Override
  public CellInfo getPartition(int partitionID) {
    // Retrieve column and row of the given partition
    int col = partitionID % numColumns;
    int row = partitionID / numColumns;
    return new CellInfo(partitionID, x + col * tileWidth, y + row * tileHeight,
        x + (col + 1) * tileWidth, y + (row + 1) * tileHeight);
  }

  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(index);
  }
}
