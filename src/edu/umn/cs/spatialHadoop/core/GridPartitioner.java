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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.OperationsParams;

/**
 * A partitioner that partitioner data using a uniform grid.
 * If a shape overlaps multiple grids, it replicates it to all overlapping
 * partitions.
 * @author Ahmed Eldawy
 *
 */
public class GridPartitioner extends Partitioner {
  /**The information of the underlying grid*/
  private GridInfo gridInfo;
  
  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public GridPartitioner() {
    // Initialize grid info so that readFields work correctly
    this.gridInfo = new GridInfo();
  }
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int numPartitions) {
    this.gridInfo.set(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
    this.gridInfo.calculateCellDimensions(numPartitions);
  }
  
  /**
   * Initializes a grid partitioner for a given file
   * @param inFile
   * @param params
   */
  public GridPartitioner(Path[] inFiles, JobConf job) {
    Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    this.gridInfo = new GridInfo(inMBR.x1, inMBR.y1, inMBR.x2, inMBR.y2);
    int numOfPartitions = job.getInt("m", job.getNumReduceTasks() * job.getNumReduceTasks() * 1000);
    this.gridInfo.calculateCellDimensions(numOfPartitions);
  }

  public GridPartitioner(Path[] inFiles, JobConf job, int width, int height) {
    Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    this.gridInfo = new GridInfo(inMBR.x1, inMBR.y1, inMBR.x2, inMBR.y2);
    this.gridInfo.columns = width;
    this.gridInfo.rows = height;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.gridInfo.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.gridInfo.readFields(in);
  }
  
  @Override
  public int getPartitionCount() {
    return gridInfo.rows * gridInfo.columns;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return;
    java.awt.Rectangle overlappingCells = this.gridInfo.getOverlappingCells(shapeMBR);
    for (int x = overlappingCells.x; x < overlappingCells.x + overlappingCells.width; x++) {
      for (int y = overlappingCells.y; y < overlappingCells.y + overlappingCells.height; y++) {
        matcher.collect(this.gridInfo.getCellId(x, y));
      }
    }
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    if (shape == null)
      return -1;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return -1;
    Point centerPoint = shapeMBR.getCenterPoint();
    return this.gridInfo.getOverlappingCell(centerPoint.x, centerPoint.y);
  }

  @Override
  public CellInfo getPartition(int partitionID) {
    return gridInfo.getCell(partitionID);
  }

  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(index+1);
  }
}
