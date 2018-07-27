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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A partitioner that partitions a file according to an existing set of cells.
 * @author Ahmed Eldawy
 *
 */
@Partitioner.GlobalIndexerMetadata(disjoint = false, extension = "cells")
public class CellPartitioner extends Partitioner {

  /**An R-tree that indexes the set of existing partition for efficient search*/
  protected RTreeGuttman partitions;

  /**The list of cells that this partitioner can choose from*/
  protected CellInfo[] cells;

  /**A reusable array that holds the list of overlapping cells*/
  protected IntArray overlappingCells = new IntArray();

  /**The degree of the R-Tree index that we use to speed up node lookup.*/
  protected static final int RTreeDegree = 32;

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public CellPartitioner() {
  }

  public CellPartitioner(CellInfo[] cells) {
    this.cells = cells.clone();
    this.cells = new CellInfo[cells.length];
    // We have to explicitly create each object as CellInfo to ensure that
    // write/readFields will work correctly
    for (int i = 0; i < cells.length; i++)
      this.cells[i] = new CellInfo(cells[i]);
    initialize(cells);
  }

  @Override
  public void construct(Rectangle mbr, Point[] points, int capacity) {
    throw new RuntimeException("You can only use the constructor CellPartition(CellInfo[])");
  }

  /**
   * Initialize the R-tree from the list of cells.
   * @param cells
   */
  private void initialize(CellInfo[] cells) {
    double[] x1s = new double[cells.length];
    double[] y1s = new double[cells.length];
    double[] x2s = new double[cells.length];
    double[] y2s = new double[cells.length];

    for (int i = 0; i < cells.length; i++) {
      x1s[i] = cells[i].x1;
      y1s[i] = cells[i].y1;
      x2s[i] = cells[i].x2;
      y2s[i] = cells[i].y2;
    }

    // The RR*-tree paper recommends setting m = 0.2 M
    partitions = new RRStarTree(RTreeDegree / 5, RTreeDegree);
    partitions.initializeHollowRTree(x1s, y1s, x2s, y2s);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(cells.length);
    for (CellInfo cell : cells)
      cell.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numCells = in.readInt();
    if (cells == null || cells.length != numCells) {
      // Initialize the array of cells only if needed
      cells = new CellInfo[numCells];
      for (int i = 0; i < numCells; i++)
        cells[i] = new CellInfo();
    }
    for (int i = 0; i < numCells; i++)
      cells[i].readFields(in);

    // Re-initialize the R-tree
    initialize(cells);
  }
  
  @Override
  public int getPartitionCount() {
    return cells == null ? 0 : cells.length;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    throw new RuntimeException("Disjoint partitioning is not supported!");
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    Rectangle shapeMBR = shape.getMBR();
    partitions.search(shapeMBR.x1, shapeMBR.y2, shapeMBR.x2, shapeMBR.y2, overlappingCells);
    int chosenCellIndex;
    if (overlappingCells.size() == 1) {
      // Only one overlapping node, return it
      chosenCellIndex = overlappingCells.peek();
    } else  if (overlappingCells.size() > 0) {
      // More than one overlapping cells, choose the best between them
      chosenCellIndex = -1;
      double minVol = Double.POSITIVE_INFINITY;
      double minPerim = Double.POSITIVE_INFINITY;
      for (int overlappingCellIndex : overlappingCells) {
        CellInfo overlappingCell = cells[overlappingCellIndex];
        double vol = overlappingCell.area();
        if (vol < minVol) {
          minVol = vol;
          minPerim = overlappingCell.getWidth() + overlappingCell.getHeight();
          chosenCellIndex = overlappingCellIndex;
        } else if (vol == minVol) {
          // This also covers the case of vol == minVol == 0
          double cellPerimeter = overlappingCell.getWidth() + overlappingCell.getHeight();
          if (cellPerimeter < minPerim) {
            minPerim = cellPerimeter;
            chosenCellIndex = overlappingCellIndex;
          }
        }
      }
    } else {
      // No overlapping cells, follow the (fake) insert choice
      chosenCellIndex = partitions.noInsert(shapeMBR.x1, shapeMBR.y1, shapeMBR.x2, shapeMBR.y2);
    }
    return cells[chosenCellIndex].cellId;
  }
  
  @Override
  public CellInfo getPartitionAt(int index) {
    return cells[index];
  }

  @Override
  public CellInfo getPartition(int id) {
    for (CellInfo cell : cells)
      if (id == cell.cellId)
        return cell;
    return null;
  }
}
