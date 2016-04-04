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
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;

/**
 * @author Ahmed Eldawy
 *
 */
public class HilbertCurvePartitioner extends Partitioner {
  /**Splits along the Hilbert curve*/
  protected int[] splits;
  
  /**The MBR of the original input file*/
  protected final Rectangle mbr = new Rectangle();

  protected static final int Resolution = Short.MAX_VALUE;
  
  public HilbertCurvePartitioner() {
  }
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {
    this.mbr.set(mbr);
    int[] hValues = new int[points.length];
    for (int i = 0; i < points.length; i++)
      hValues[i] = computeHValue(mbr, points[i].x, points[i].y);
    createFromHValues(hValues, capacity);
  }

  /**
   * Create a HilbertCurvePartitioner from a list of points
   * @param hValues
   * @param capacity
   */
  protected void createFromHValues(final int[] hValues, int capacity) {
    Arrays.sort(hValues);
    int numSplits = (int) Math.ceil((double)hValues.length / capacity);
    this.splits = new int[numSplits];
    int maxH = 0x7fffffff;
    for (int i = 0; i < splits.length; i++) {
      int quantile = (int) ((long)(i + 1) * hValues.length / numSplits);
      this.splits[i] = quantile == hValues.length ? maxH : hValues[quantile];
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(splits.length);
    ByteBuffer bbuffer = ByteBuffer.allocate(splits.length * 4);
    for (int split : splits)
      bbuffer.putInt(split);
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    mbr.readFields(in);
    splits = new int[in.readInt()];
    byte[] buffer = new byte[splits.length * 4];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < splits.length; i++)
      splits[i] = bbuffer.getInt();
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    throw new RuntimeException("Non-implemented method");
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
    int hValue = computeHValue(mbr, center.x, center.y);
    int partition = Arrays.binarySearch(splits, hValue);
    if (partition < 0)
      partition = -partition - 1;
    return partition;
  }

  /**
   * Compute Hilbert curve value for a point (x, y) in a square of size
   * (n*n)
   * @param n - Size of the square
   * @param x - x dimension (short)
   * @param y - y dimension (short)
   * @return
   */
  public static int computeHValue(int n, int x, int y) {
    int h = 0;
    for (int s = n/2; s > 0; s/=2) {
      int rx = (x & s) > 0 ? 1 : 0;
      int ry = (y & s) > 0 ? 1 : 0;
      h += s * s * ((3 * rx) ^ ry);

      // Rotate
      if (ry == 0) {
        if (rx == 1) {
          x = n-1 - x;
          y = n-1 - y;
        }

        //Swap x and y
        int t = x; x = y; y = t;
      }
    }
    return h;
  }
  public static int computeHValue(Rectangle inMBR, final double x, final double y) {
    int ix = (int) ((x - inMBR.x1) * Resolution / inMBR.getWidth());
    int iy = (int) ((y - inMBR.y1) * Resolution / inMBR.getHeight());
    return computeHValue(Resolution+1, ix, iy);
  }

  @Override
  public CellInfo getPartition(int partitionID) {
    throw new RuntimeException("Non-implemented method");
  }

  @Override
  public CellInfo getPartitionAt(int index) {
    throw new RuntimeException("Non-implemented method");
  }

  @Override
  public int getPartitionCount() {
    return splits.length;
  }

  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    
    Path inPath = params.getInputPath();
    long length = inPath.getFileSystem(params).getFileStatus(inPath).getLen();
    ShapeIterRecordReader reader = new ShapeIterRecordReader(params,
        new FileSplit(inPath, 0, length, new String[0]));
    Rectangle key = reader.createKey();
    ShapeIterator shapes = reader.createValue();
    final Vector<Point> points = new Vector<Point>();
    while (reader.next(key, shapes)) {
      for (Shape s : shapes) {
        points.add(s.getMBR().getCenterPoint());
      }
    }
    Rectangle inMBR = (Rectangle)OperationsParams.getShape(params, "mbr");
    HilbertCurvePartitioner hcp = new HilbertCurvePartitioner();
    hcp.createFromPoints(inMBR, points.toArray(new Point[points.size()]), 10);
    
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = hcp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  }
}
