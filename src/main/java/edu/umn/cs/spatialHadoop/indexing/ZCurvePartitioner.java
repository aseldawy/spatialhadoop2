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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * Partition the space based on Z-curve.
 * @author Ahmed Eldawy
 *
 */
public class ZCurvePartitioner extends Partitioner {
  private static final Log LOG = LogFactory.getLog(ZCurvePartitioner.class);

  /**MBR of the input file*/
  protected final Rectangle mbr = new Rectangle();
  /**Upper bound of all partitions*/
  protected long[] zSplits;

  protected static final int Resolution = Integer.MAX_VALUE;
  
  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public ZCurvePartitioner() {
  }
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {
    this.mbr.set(mbr);
    long[] zValues = new long[points.length];
    for (int i = 0; i < points.length; i++)
      zValues[i] = computeZ(mbr, points[i].x, points[i].y);
    createFromZValues(zValues, capacity);
  }

  /**
   * Create a ZCurvePartitioner from a list of points
   * @param zValues
   * @param capacity
   */
  protected void createFromZValues(final long[] zValues, int capacity) {
    Arrays.sort(zValues);
    int numSplits = (int) Math.ceil((double)zValues.length / capacity);
    this.zSplits = new long[numSplits];
    long maxZ = computeZ(mbr, mbr.x2, mbr.y2);
    for (int i = 0; i < numSplits; i++) {
      int quantile = (int) ((long)(i + 1) * zValues.length / numSplits);
      this.zSplits[i] = quantile == zValues.length ? maxZ : zValues[quantile];
    }
  }

  /**
   * Computes the Z-order of a point relative to a containing rectangle
   * @param mbr
   * @param x
   * @param y
   * @return
   */
  public static long computeZ(Rectangle mbr, double x, double y) {
    int ix = (int) ((x - mbr.x1) * Resolution / mbr.getWidth());
    int iy = (int) ((y - mbr.y1) * Resolution / mbr.getHeight());
    return computeZOrder(ix, iy);
  }
  
  /**
   * Reverse the computation of the Z-value and returns x and y dimensions
   * @param mbr
   * @param z
   * @param outPoint
   */
  public static void uncomputeZ(Rectangle mbr, long z, Point outPoint) {
    long ixy = unComputeZOrder(z);
    int ix = (int) (ixy >> 32);
    int iy = (int) (ixy & 0xffffffffL);
    outPoint.x = (double)(ix) * mbr.getWidth() / Resolution + mbr.x1;
    outPoint.y = (double)(iy) * mbr.getHeight() / Resolution + mbr.y1;
  }
  
  /**
   * Computes the Z-order (Morton order) of a two-dimensional point.
   * @param x - integer value of the x-axis (cannot exceed Integer.MAX_VALUE)
   * @param y - integer value of the y-axis (cannot exceed Integer.MAX_VALUE)
   * @return
   */
  public static long computeZOrder(long x, long y) {
    long morton = 0;
  
    for (long bitPosition = 0; bitPosition < 32; bitPosition++) {
      long mask = 1L << bitPosition;
      morton |= (x & mask) << (bitPosition + 1);
      morton |= (y & mask) << bitPosition;
    }
    return morton;
  }
  
  public static java.awt.Point unComputeZOrder(long morton, java.awt.Point point) {
    long ixy = unComputeZOrder(morton);
    point.x = (int) (ixy >>> 32);
    point.y = (int) (ixy & 0xffffffff);
    return point;
  }
  
  public static long unComputeZOrder(long morton) {
    long x = 0, y = 0;
    for (long bitPosition = 0; bitPosition < 32; bitPosition++) {
      long mask = 1L << (bitPosition << 1);
      y |= (morton & mask) >> bitPosition;
      x |= (morton & (mask << 1)) >> (bitPosition + 1);
    }
    return (x << 32) | y;
  }
  
  /**
   * Compute the minimal bounding rectangle MBR of a range on the Z-curve.
   * Notice that getting the MBR of the two end points does not always work.
   * 
   * @param zMin
   * @param zMax
   * @return
   */
  public static java.awt.Rectangle getMBRInteger(long zMin, long zMax) {
    zMax -= 1; // Because the range is exclusive
    long changedBits = zMin ^ zMax;
    // The mask contains 1's for all bits that are less or equal significant
    // to any changed bit
    long mask = changedBits;
    long oldMask;
    do {
      oldMask = mask;
      mask |= (mask >> 1);
    } while (mask != oldMask);
    // Both zMin and zMax can be used in the following equations because we
    // explicitly set all different bits
    java.awt.Point minXY = unComputeZOrder(zMin & (~mask), new java.awt.Point());
    java.awt.Point maxXY = unComputeZOrder(zMin | mask, new java.awt.Point());
    java.awt.Rectangle mbr = new java.awt.Rectangle(minXY.x, minXY.y, maxXY.x - minXY.x, maxXY.y - minXY.y);
    return mbr;
  }
  
  public static Rectangle getMBR(Rectangle mbr, long zMin, long zMax) {
    java.awt.Rectangle mbrInteger = getMBRInteger(zMin, zMax);
    Rectangle trueMBR = new Rectangle();
    trueMBR.x1 = (double)(mbrInteger.x) * mbr.getWidth() / Resolution + mbr.x1;
    trueMBR.y1 = (double)(mbrInteger.y) * mbr.getHeight() / Resolution + mbr.y1;
    trueMBR.x2 = (double)(mbrInteger.getMaxX()) * mbr.getWidth() / Resolution + mbr.x1;
    trueMBR.y2 = (double)(mbrInteger.getMaxY()) * mbr.getHeight() / Resolution + mbr.y1;
    return trueMBR;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(zSplits.length);
    ByteBuffer bbuffer = ByteBuffer.allocate(zSplits.length * 8);
    for (long zSplit : zSplits)
      bbuffer.putLong(zSplit);
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Did not calculate buffer size correctly");
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    mbr.readFields(in);
    int partitionCount = in.readInt();
    zSplits = new long[partitionCount];
    int bufferLength = 8 * partitionCount;
    byte[] buffer = new byte[bufferLength];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < partitionCount; i++) {
      zSplits[i] = bbuffer.getLong();
    }
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Error reading STR partitioner");
  }
  
  @Override
  public int getPartitionCount() {
    return zSplits.length;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    // TODO match with all overlapping partitions instead of only one
    int partition = overlapPartition(shape);
    if (partition >= 0)
      matcher.collect(partition);
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    if (shape == null)
      return -1;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return -1;
    // Assign to only one partition that contains the center point
    Point center = shapeMBR.getCenterPoint();
    long zValue = computeZ(mbr, center.x, center.y);
    int partition = Arrays.binarySearch(zSplits, zValue);
    if (partition < 0)
      partition = -partition - 1;
    return partition;
  }

  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(index);
  }
  
  @Override
  public CellInfo getPartition(int id) {
    CellInfo cell = new CellInfo();
    cell.cellId = id;
    long zMax = zSplits[id];
    long zMin = id == 0? 0 : zSplits[id-1];
    
    Rectangle cellMBR = getMBR(mbr, zMin, zMax);
    cell.set(cellMBR);
    return cell;
  }
}
