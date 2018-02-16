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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A partitioner that uses the original Antonin Guttman R-tree index technique.
 * @author Ahmed Eldawy
 *
 */
public class RTreePartitioner extends Partitioner {

  /**
   * A helper class that extends Rectangle with expansion calculation functions.
   */
  private static class RTreePartition extends Rectangle {
    public double area() {
      return getWidth() * getHeight();
    }

    public double expansion(Rectangle mbr) {
      double newWidth = this.getWidth();
      double newHeight = this.getHeight();
      if (mbr.x1 < this.x1)
        newWidth += (this.x1 - mbr.x1);
      else if (mbr.x2 > this.x2)
        newWidth += (mbr.x2 - this.x2);
      if (mbr.y1 < this.y1)
        newHeight += (this.y1 - mbr.y1);
      else if (mbr.y2 > this.y2)
        newHeight += (this.y2 - mbr.y2);

      return newWidth * newHeight - getWidth() * getHeight();
    }
  }

  /**The list of all partitions created on the sample points*/
  private RTreePartition[] partitions;

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
    RTreeGuttman rtree = new RTreeGuttman(xs, ys, capacity/2, capacity);
    Rectangle[] nodes = rtree.getAllLeaves();
    partitions = new RTreePartition[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      partitions[i] = new RTreePartition();
      partitions[i].set(nodes[i]);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitions.length);
    for (Rectangle partition : partitions) {
      partition.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numPartitions = in.readInt();
    if (partitions == null || partitions.length != numPartitions)
      partitions = new RTreePartition[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      if (partitions[i] == null)
        partitions[i] = new RTreePartition();
      partitions[i].readFields(in);
    }
  }
  
  @Override
  public int getPartitionCount() {
    return partitions.length;
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
    for (int i = 0; i < partitions.length; i++) {
      double expansion = partitions[i].expansion(shapeMBR);
      if (expansion < minExpansion) {
        minExpansion = expansion;
        chosenPartition = i;
      } else if (expansion == minExpansion) {
        // Resolve ties by choosing the entry with the rectangle of smallest area
        if (partitions[i].area() < partitions[chosenPartition].area())
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
    return new CellInfo(id, partitions[id]);
  }
}
