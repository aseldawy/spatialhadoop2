/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.*;

import java.awt.geom.Rectangle2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A partitioner that supports an R-tree-based partitioning using either the
 * original Guttman R-tree (1984) or the improved R*-tree (1990)
 * @author Ahmed Eldawy
 *
 */
public class RTreePartitioner extends Partitioner {

  /**The coordinates of the partitions*/
  protected double[] x1s, y1s, x2s, y2s;

  /**
   * Computes the expansion that will happen on an a partition when it is
   * enlarged to enclose a given rectangle.
   * @param partitionID
   * @param mbr the MBR of the object to be added to the partition
   * @return
   */
  protected double Partition_expansion(int partitionID, Rectangle mbr) {
    double widthBefore = x2s[partitionID] - x1s[partitionID];
    double heightBefore = y2s[partitionID] - y1s[partitionID];
    double widthAfter = widthBefore, heightAfter = heightBefore;
    if (mbr.x1 < x1s[partitionID])
      widthAfter += x1s[partitionID] - mbr.x1;
    if (mbr.y1 < y1s[partitionID])
      widthAfter += y1s[partitionID] - mbr.y1;
    if (mbr.x2 > x2s[partitionID])
      heightAfter += x2s[partitionID] - mbr.x2;
    if (mbr.y2 > y2s[partitionID])
      heightAfter += y2s[partitionID] - mbr.y2;
    return widthAfter * heightAfter - widthBefore * heightBefore;
  }

  /**
   * Computes the area of a partition.
   * @param partitionID
   * @return
   */
  protected double Partition_area(int partitionID) {
    return (x2s[partitionID] - x1s[partitionID]) *
        (y2s[partitionID] - y1s[partitionID]);
  }

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public RTreePartitioner() {
  }
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {
    double[] xs = new double[points.length];
    double[] ys = new double[points.length];
    for (int i = 0; i < points.length; i++) {
      xs[i] = points[i].x;
      ys[i] = points[i].y;
    }
    RTreeGuttman rtree = RStarTree.constructFromPoints(xs, ys, capacity/2, capacity);
    Rectangle2D.Double[] nodes = rtree.getAllLeaves();
    x1s = new double[nodes.length];
    y1s = new double[nodes.length];
    x2s = new double[nodes.length];
    y2s = new double[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      x1s[i] = nodes[i].getMinX();
      y1s[i] = nodes[i].getMinY();
      x2s[i] = nodes[i].getMaxX();
      y2s[i] = nodes[i].getMaxY();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(x1s.length);
    for (int i = 0; i < x1s.length; i++) {
      out.writeDouble(x1s[i]);
      out.writeDouble(y1s[i]);
      out.writeDouble(x2s[i]);
      out.writeDouble(y2s[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numPartitions = in.readInt();
    if (getPartitionCount() != numPartitions) {
      x1s = new double[numPartitions];
      y1s = new double[numPartitions];
      x2s = new double[numPartitions];
      y2s = new double[numPartitions];
    }
    for (int i = 0; i < numPartitions; i++) {
      x1s[i] = in.readDouble();
      y1s[i] = in.readDouble();
      x2s[i] = in.readDouble();
      y2s[i] = in.readDouble();
    }
  }
  
  @Override
  public int getPartitionCount() {
    return x1s == null? 0 : x1s.length;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    throw new RuntimeException("Disjoint replicated partitioning not supported for R-tree!");
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    // ChooseLeaf. Select a leaf node in which to place a new entry E
    // Select a node N whose rectangle needs least enlargement to include E
    // Resolve ties by choosing the entry with the rectangle of smallest area
    Rectangle shapeMBR = shape.getMBR();
    double minExpansion = Double.POSITIVE_INFINITY;
    int chosenPartition = -1;
    for (int i = 0; i < getPartitionCount(); i++) {
      double expansion = Partition_expansion(i, shapeMBR);
      if (expansion < minExpansion) {
        minExpansion = expansion;
        chosenPartition = i;
      } else if (expansion == minExpansion) {
        // Resolve ties by choosing the entry with the rectangle of smallest area
        if (Partition_area(i) < Partition_area(chosenPartition))
          chosenPartition = i;
      }
    }
    return chosenPartition;
  }
  
  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(index);
  }

  @Override
  public CellInfo getPartition(int id) {
    return new CellInfo(id, x1s[id], y1s[id], x2s[id], y2s[id]);
  }
}
