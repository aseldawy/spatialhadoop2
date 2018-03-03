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
import edu.umn.cs.spatialHadoop.util.IntArray;

import java.awt.geom.Rectangle2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A partitioner that supports an R*-tree-based partitioning using the R*-tree
 * index published in (1990)
 * @see RStarTree
 * @see RStarTree#partitionPoints(double[], double[], int, int, boolean, RStarTree.AuxiliarySearchStructure)
 * @author Ahmed Eldawy
 *
 */
public class RStarTreePartitioner extends Partitioner {

  /**MBR of the points used to partition the space*/
  protected Rectangle mbrPoints;

  /**The coordinates of the partitions*/
  protected double[] x1s, y1s, x2s, y2s;

  /**A temporary array used to compute intersections*/
  private IntArray overlappingPartitions = new IntArray();

  /**An auxiliary search structure to find matching partitions quickly*/
  private RStarTree.AuxiliarySearchStructure aux;

  /**
   * Computes the expansion that will happen on an a partition when it is
   * enlarged to enclose a given rectangle.
   * @param partitionID
   * @param mbr the MBR of the object to be added to the partition
   * @return
   */
  protected double Partition_expansion(int partitionID, Rectangle mbr) {
    // If the given rectangle is completely enclosed in the enalrged MBR of the
    // given partition, return 0
    if (mbr.x1 >= x1s[partitionID] && mbr.x2 <= x2s[partitionID] &&
        mbr.y1 >= y1s[partitionID] && mbr.y2 <= y2s[partitionID])
      return 0;
    // Compute the non-infinity MBR of the partition
    double px1 = Math.max(x1s[partitionID], mbrPoints.x1);
    double px2 = Math.min(x2s[partitionID], mbrPoints.x2);
    double py1 = Math.max(y1s[partitionID], mbrPoints.y1);
    double py2 = Math.min(y2s[partitionID], mbrPoints.y2);
    double areaBefore = (px2 - px1) * (py2 - py1);
    // Expand the non-infinity MBR of the partition to include the given MBR
    px1 = Math.min(px1, mbr.x1);
    py1 = Math.min(py1, mbr.y1);
    px2 = Math.max(px2, mbr.x2);
    py2 = Math.max(py2, mbr.y2);
    return (px2-px1) * (py2-py1) - areaBefore;
  }

  /**
   * Tests if a partition overlaps a given rectangle
   * @param partitionID
   * @param mbr
   * @return
   */
  protected boolean Partition_overlap(int partitionID, Rectangle mbr) {
    return !(mbr.x2 <= x1s[partitionID] || x2s[partitionID] < mbr.x1 ||
      mbr.y2 <= y1s[partitionID] || y2s[partitionID] < mbr.y1);
  }

  /**
   * Computes the area of a partition.
   * @param partitionID
   * @return
   */
  protected double Partition_area(int partitionID) {
    double px1 = Math.max(x1s[partitionID], mbrPoints.x1);
    double px2 = Math.min(x2s[partitionID], mbrPoints.x2);
    double py1 = Math.max(y1s[partitionID], mbrPoints.y1);
    double py2 = Math.min(y2s[partitionID], mbrPoints.y2);
    return (px2 - px1) * (py2 - py1);
  }

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public RStarTreePartitioner() {
  }
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {
    double[] xs = new double[points.length];
    double[] ys = new double[points.length];
    mbrPoints = new Rectangle(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    for (int i = 0; i < points.length; i++) {
      xs[i] = points[i].x;
      ys[i] = points[i].y;
      mbrPoints.expand(points[i]);
    }
    aux = new RStarTree.AuxiliarySearchStructure();
    Rectangle[] partitions = RStarTree.partitionPoints(xs, ys, capacity * 8 / 10, capacity, true, aux);
    x1s = new double[partitions.length];
    y1s = new double[partitions.length];
    x2s = new double[partitions.length];
    y2s = new double[partitions.length];
    for (int i = 0; i < partitions.length; i++) {
      x1s[i] = partitions[i].x1;
      y1s[i] = partitions[i].y1;
      x2s[i] = partitions[i].x2;
      y2s[i] = partitions[i].y2;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    mbrPoints.write(out);
    out.writeInt(x1s.length);
    for (int i = 0; i < x1s.length; i++) {
      out.writeDouble(x1s[i]);
      out.writeDouble(y1s[i]);
      out.writeDouble(x2s[i]);
      out.writeDouble(y2s[i]);
    }
    aux.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (mbrPoints == null) mbrPoints = new Rectangle();
    mbrPoints.readFields(in);
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
    if (aux == null)
      aux = new RStarTree.AuxiliarySearchStructure();
    aux.readFields(in);
  }
  
  @Override
  public int getPartitionCount() {
    return x1s == null? 0 : x1s.length;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    Rectangle shapeMBR = shape.getMBR();
    aux.search(shapeMBR.x1, shapeMBR.y1, shapeMBR.x2, shapeMBR.y2, overlappingPartitions);
    for (int overlappingPartition : overlappingPartitions)
      matcher.collect(overlappingPartition);
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    // ChooseLeaf. Select a leaf node in which to place a new entry E
    // Select a node N whose rectangle needs least enlargement to include E
    // Resolve ties by choosing the entry with the rectangle of smallest area
    // For efficiency, we only consider the partitions that overlap the input
    // shape. This is not entirely accurate however.
    Rectangle shapeMBR = shape.getMBR();
    double minExpansion = Double.POSITIVE_INFINITY;
    int chosenPartition = -1;
    aux.search(shapeMBR.x1, shapeMBR.y1, shapeMBR.x2, shapeMBR.y2, overlappingPartitions);
    if (overlappingPartitions.size() == 1)
      return overlappingPartitions.get(0);
    for (int overlappingPartition : overlappingPartitions) {
      double expansion = Partition_expansion(overlappingPartition, shapeMBR);
      if (expansion < minExpansion) {
        minExpansion = expansion;
        chosenPartition = overlappingPartition;
      } else if (expansion == minExpansion) {
        // Resolve ties by choosing the entry with the rectangle of smallest area
        if (Partition_area(overlappingPartition) < Partition_area(chosenPartition))
          chosenPartition = overlappingPartition;
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
