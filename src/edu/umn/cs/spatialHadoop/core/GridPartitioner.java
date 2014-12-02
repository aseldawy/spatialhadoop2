/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
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
  private static final Log LOG = LogFactory.getLog(GridPartitioner.class);
  private GridInfo gridInfo;
  
  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public GridPartitioner() {
    // Initialize grid info so that readFields work correctly
    this.gridInfo = new GridInfo();
  }
  
  /**
   * Initializes a grid partitioner for a given file
   * @param inFile
   * @param params
   */
  public GridPartitioner(Path inFile, JobConf job) {
    try {
      FileSystem inFS = inFile.getFileSystem(job);
      long inFileLen = inFS.getFileStatus(inFile).getLen();
      Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
      Path outFile = FileOutputFormat.getOutputPath(job);
      FileSystem outFS = outFile.getFileSystem(job);
      long outBlockSize = outFS.getDefaultBlockSize(outFile);
      this.gridInfo = new GridInfo(inMBR.x1, inMBR.y1, inMBR.x2, inMBR.y2);
      this.gridInfo.calculateCellDimensions(inFileLen, outBlockSize);
    } catch (IOException e) {
      LOG.warn("Error partitioning file into grid", e);
    }
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
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    java.awt.Rectangle overlappingCells = this.gridInfo.getOverlappingCells(shape.getMBR());
    for (int x = overlappingCells.x; x < overlappingCells.x + overlappingCells.width; x++) {
      for (int y = overlappingCells.y; y < overlappingCells.y + overlappingCells.height; y++) {
        matcher.collect(this.gridInfo.getCellId(x, y));
      }
    }
  }

  @Override
  public CellInfo getPartition(int partitionID) {
    return gridInfo.getCell(partitionID);
  }

}
