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

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public CellPartitioner() {
  }

  public CellPartitioner(CellInfo[] cells) {
    this.cells = cells.clone();
    initialize(cells);
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

    partitions = new RRStarTree(4, 8);
    partitions.initializeHollowRTree(x1s, y1s, x2s, y2s);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(cells.length);
    for (CellInfo cell : cells)
      cell.write(out);
    initialize(cells);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numCells = in.readInt();
    if (cells == null || cells.length != numCells) {
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
    int i = partitions.noInsert(shapeMBR.x1, shapeMBR.y1, shapeMBR.x2, shapeMBR.y2);
    return cells[i].cellId;
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
