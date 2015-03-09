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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;

/**
 * Partition the space based on a Quad tree
 * @author Ahmed Eldawy
 *
 */
public class QuadTreePartitioner extends Partitioner {
  /**The minimal bounding rectangle of the underlying file*/
  protected Rectangle mbr;
  /**ID of all leaf nodes in partition tree*/
  protected int[] leafNodeIDs;

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public QuadTreePartitioner() {
    mbr = new Rectangle();
  }
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int numPartitions) {
    this.mbr.set(mbr);
    long[] zValues = new long[points.length];
    for (int i = 0; i < points.length; i++)
      zValues[i] = ZCurvePartitioner.computeZ(mbr, points[i].x, points[i].y);
    createFromZValues(zValues, numPartitions);
  }
  
  /**
   * Create a ZCurvePartitioner from a list of points
   * @param vsample
   * @param inMBR
   * @param partitions
   * @return
   */
  protected void createFromZValues(final long[] zValues, int partitions) {
    int nodeCapacity = zValues.length / partitions;
    Arrays.sort(zValues);
    class QuadTreeNode {
      int fromIndex, toIndex;
      long minZ/*, maxZ*/;
      int nodeID; // A unique ID of the node
      int depth; // Depth in the tree starting with ONE at the root
      
      public QuadTreeNode(int fromIndex, int toIndex, long minZ, long maxZ,
          int nodeID, int depth) {
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
        this.minZ = minZ;
        //this.maxZ = maxZ;
        this.nodeID = nodeID;
        this.depth = depth;
      }
    }
    
    long minZ = ZCurvePartitioner.computeZ(mbr, mbr.x1, mbr.y1); // Always zero
    long maxZ = ZCurvePartitioner.computeZ(mbr, mbr.x2, mbr.y2);
    QuadTreeNode root = new QuadTreeNode(0, zValues.length, minZ, maxZ, 1, 1);
    Queue<QuadTreeNode> nodesToSplit = new ArrayDeque<QuadTreeNode>();
    nodesToSplit.add(root);

    Vector<Integer> leafNodeIDs = new Vector<Integer>();
    
    while (!nodesToSplit.isEmpty()) {
      QuadTreeNode nodeToSplit = nodesToSplit.remove();
      if (nodeToSplit.toIndex - nodeToSplit.fromIndex <= nodeCapacity) {
        // No need to split
        leafNodeIDs.add(nodeToSplit.nodeID);
      } else {
        // The position of the lowest of the two bits that change for these
        // children in the Z-order
        // For the root, we change the two highest bits in the zOrder
        int changedBits =
            KdTreePartitioner.getNumberOfSignificantBits(ZCurvePartitioner.Resolution) * 2 -
            nodeToSplit.depth * 2;
        // Need to split into four children
        long childMinZ = nodeToSplit.minZ;
        int childFromIndex = nodeToSplit.fromIndex;
        for (int iChild = 0; iChild < 4; iChild++) {
          long childMaxZ = nodeToSplit.minZ + ((iChild + 1L) << changedBits);
          int childToIndex = Arrays.binarySearch(zValues,
              nodeToSplit.fromIndex, nodeToSplit.toIndex, childMaxZ);
          if (childToIndex < 0)
            childToIndex = -(childToIndex + 1);
          QuadTreeNode childNode = new QuadTreeNode(childFromIndex,
              childToIndex, childMinZ, childMaxZ,
              nodeToSplit.nodeID * 4 + iChild, nodeToSplit.depth + 1);
          nodesToSplit.add(childNode);
          childMinZ = childMaxZ;
          childFromIndex = childToIndex;
        }
      }
    }
    
    this.leafNodeIDs = new int[leafNodeIDs.size()];
    for (int i = 0; i < leafNodeIDs.size(); i++)
      this.leafNodeIDs[i] = leafNodeIDs.get(i);
    Arrays.sort(this.leafNodeIDs);
  }


  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(leafNodeIDs.length);
    ByteBuffer bbuffer = ByteBuffer.allocate(leafNodeIDs.length * 4);
    for (int leafNodeID : leafNodeIDs)
      bbuffer.putInt(leafNodeID);
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Did not calculate buffer size correctly");
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (mbr == null)
      mbr = new Rectangle();
    mbr.readFields(in);
    int numberOfLeafNodes = in.readInt();
    leafNodeIDs = new int[numberOfLeafNodes];
    byte[] buffer = new byte[leafNodeIDs.length * 4];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < leafNodeIDs.length; i++)
      leafNodeIDs[i] = bbuffer.getInt();
  }

  @Override
  public int overlapPartition(Shape shape) {
    if (shape == null || shape.getMBR() == null)
      return -1;

    Point queryPoint = shape.getMBR().getCenterPoint();
    int nodeToSearch = 1; // Start from the root
    Rectangle nodeMBR = mbr.clone();
    while (Arrays.binarySearch(leafNodeIDs, nodeToSearch) < 0) {
      if (nodeToSearch > leafNodeIDs[leafNodeIDs.length - 1]) {
        System.err.println("not found");
        return -1;
      }
      Point nodeCenter = nodeMBR.getCenterPoint();
      if (queryPoint.x < nodeCenter.x && queryPoint.y < nodeCenter.y) {
        nodeToSearch = nodeToSearch * 4;
        nodeMBR.x2 = nodeCenter.x;
        nodeMBR.y2 = nodeCenter.y;
      } else if (queryPoint.x < nodeCenter.x && queryPoint.y >= nodeCenter.y) {
        nodeToSearch = nodeToSearch * 4 + 1;
        nodeMBR.x2 = nodeCenter.x;
        nodeMBR.y1 = nodeCenter.y;
      } else if (queryPoint.x >= nodeCenter.x && queryPoint.y < nodeCenter.y) {
        nodeToSearch = nodeToSearch * 4 + 2;
        nodeMBR.x1 = nodeCenter.x;
        nodeMBR.y2 = nodeCenter.y;
      } else {
        nodeToSearch = nodeToSearch * 4 + 3;
        nodeMBR.x1 = nodeCenter.x;
        nodeMBR.y1 = nodeCenter.y;
      }
    }
    return nodeToSearch;
  }
  
  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null || shape.getMBR() == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    Queue<CellInfo> nodesToSearch = new ArrayDeque<CellInfo>();
    nodesToSearch.add(new CellInfo(1, mbr));
    
    while (!nodesToSearch.isEmpty()) {
      // Go down as necessary
      CellInfo nodeToSearch = nodesToSearch.remove();
      if (shapeMBR.isIntersected(nodeToSearch)) {
        if (Arrays.binarySearch(leafNodeIDs, nodeToSearch.cellId) >= 0) {
          // Reached a leaf node that overlaps the given shape
          matcher.collect(nodeToSearch.cellId);
        } else {
          // Overlapping with a non-leaf node, go deeper to four children
          Point centerPoint = nodeToSearch.getCenterPoint();
          nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4,
              nodeToSearch.x1, nodeToSearch.y1, centerPoint.x, centerPoint.y));
          nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4 + 1,
              nodeToSearch.x1, centerPoint.y, centerPoint.x, nodeToSearch.y2));
          nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4 + 2,
              centerPoint.x, nodeToSearch.y1, nodeToSearch.x2, centerPoint.y));
          nodesToSearch.add(new CellInfo(nodeToSearch.cellId * 4 + 3,
              centerPoint.x, centerPoint.y, nodeToSearch.x2, nodeToSearch.y2));
        }
      }
    }
  }

  @Override
  public int getPartitionCount() {
    return leafNodeIDs.length;
  }

  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(leafNodeIDs[index]);
  }
  
  @Override
  public CellInfo getPartition(int partitionID) {
    CellInfo cellInfo = new CellInfo(partitionID, mbr);
    
    int partitionDepth =
        (KdTreePartitioner.getNumberOfSignificantBits(partitionID) + 1) / 2;
    
    for (int depth = 1; depth < partitionDepth; depth++) {
      int childNumber = (partitionID >> (2 * (partitionDepth - depth - 1))) & 3;
      Point center = cellInfo.getCenterPoint();
      switch (childNumber) {
      case 0: cellInfo.x2 = center.x; cellInfo.y2 = center.y; break;
      case 1: cellInfo.x2 = center.x; cellInfo.y1 = center.y; break;
      case 2: cellInfo.x1 = center.x; cellInfo.y2 = center.y; break;
      case 3: cellInfo.x1 = center.x; cellInfo.y1 = center.y; break;
      }
    }
    
    return cellInfo;
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
    
    QuadTreePartitioner qtp = new QuadTreePartitioner();
    qtp.createFromPoints(inMBR, points.toArray(new Point[points.size()]), 8);
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = qtp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  
    System.out.println("Partition count "+qtp.getPartitionCount());
    for (int i = 0; i < qtp.getPartitionCount(); i++) {
      System.out.println(qtp.getPartitionAt(i).toWKT());
    }
  }
}
