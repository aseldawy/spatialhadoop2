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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Queue;
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
 * A partitioner that partitioner data using a K-d tree-based partitioner.
 * @author Ahmed Eldawy
 *
 */
public class KdTreePartitioner extends Partitioner {
  /**MBR of the input file*/
  private final Rectangle mbr = new Rectangle();
  
  /**
   * Location of all splits stored in a complete binary tree which is encoded in
   * a single array in a heap-like structure.
   */
  private double[] splits;
  
  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public KdTreePartitioner() {
  }
  
  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {

    // Enumerate all partition IDs to be able to count leaf nodes in any split
    // TODO do the same functionality without enumerating all IDs
    int numSplits = (int) Math.ceil((double)points.length / capacity);
    String[] ids = new String[numSplits];
    for (int id = numSplits; id < 2 * numSplits; id++)
      ids[id - numSplits] = Integer.toBinaryString(id);
    
    // Keep splitting the space into halves until we reach the desired number of
    // partitions
    @SuppressWarnings("unchecked")
    Comparator<Point>[] comparators = new Comparator[] {
      new Comparator<Point>() {
        @Override
        public int compare(Point a, Point b) {
          return a.x < b.x? -1 : (a.x > b.x? 1 : 0);
        }},
      new Comparator<Point>() {
        @Override
        public int compare(Point a, Point b) {
          return a.y < b.y? -1 : (a.y > b.y? 1 : 0);
        }}
    };
    
    class SplitTask {
      int fromIndex;
      int toIndex;
      int direction;
      int partitionID;

      /**Constructor using all fields*/
      public SplitTask(int fromIndex, int toIndex, int direction,
          int partitionID) {
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
        this.direction = direction;
        this.partitionID = partitionID;
      }
      
    }
    Queue<SplitTask> splitTasks = new ArrayDeque<SplitTask>();
    splitTasks.add(new SplitTask(0, points.length, 0, 1));
    
    this.mbr.set(mbr);
    this.splits = new double[numSplits];
    
    while (!splitTasks.isEmpty()) {
      SplitTask splitTask = splitTasks.remove();
      if (splitTask.partitionID < numSplits) {
        String child1 = Integer.toBinaryString(splitTask.partitionID * 2);
        String child2 = Integer.toBinaryString(splitTask.partitionID * 2 + 1);
        int size_child1 = 0, size_child2 = 0;
        for (int i = 0; i < ids.length; i++) {
          if (ids[i].startsWith(child1))
            size_child1++;
          else if (ids[i].startsWith(child2))
            size_child2++;
        }
        
        // Calculate the index which partitions the subrange into sizes
        // proportional to size_child1 and size_child2
        int splitIndex = (int) (((long)size_child1 * splitTask.toIndex + (long)size_child2 * splitTask.fromIndex)
            / (size_child1 + size_child2));
        partialQuickSort(points, splitTask.fromIndex, splitTask.toIndex,
            splitIndex, comparators[splitTask.direction]);
        Point splitValue = points[splitIndex];
        this.splits[splitTask.partitionID] = splitTask.direction == 0 ?
            splitValue.x : splitValue.y;
        splitTasks.add(new SplitTask(splitTask.fromIndex, splitIndex,
            1 - splitTask.direction, splitTask.partitionID * 2));
        splitTasks.add(new SplitTask(splitIndex, splitTask.toIndex,
            1 - splitTask.direction, splitTask.partitionID * 2 + 1));
      }
    }
  }

  /**
   * Reorders the given subrange of the array so that the element at the
   * desiredIndex is at its correct position. Upon return of this function,
   * the element at the desired index is greater than or equal all previous
   * values in the subrange and less than or equal to all following items in
   * the subrange.
   * @param a - the array to sort
   * @param fromIndex - the index of the first element in the subrange
   * @param toIndex - the index after the last element in the subrange
   * @param desiredIndex - the index which needs to be adjusted
   * @param c - the comparator used to compare array elements
   */
  public static <T> void partialQuickSort(T[] a, int fromIndex, int toIndex,
      int desiredIndex, Comparator<T> c) {
    Arrays.sort(a, fromIndex, toIndex, c);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(splits.length);
    ByteBuffer bbuffer = ByteBuffer.allocate(splits.length * 8);
    for (double split : splits)
      bbuffer.putDouble(split);
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Did not calculate buffer size correctly");
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    mbr.readFields(in);
    int partitions = in.readInt();
    splits = new double[partitions];
    
    int bufferLength = splits.length * 8;
    byte[] buffer = new byte[bufferLength];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < splits.length; i++)
      splits[i] = bbuffer.getDouble();
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Error reading STR partitioner");
  }
  
  @Override
  public int getPartitionCount() {
    return splits.length;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null || shape.getMBR() == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    /**Information about a split to test*/
    class SplitToTest {
      /**The ID of the split in the array of splits in the KDTreePartitioner*/
      int splitID;
      /**Direction of the split. 0 is vertical (|) and 1 is horizontal (-)*/
      int direction;
      
      public SplitToTest(int splitID, int direction) {
        this.splitID = splitID;
        this.direction = direction;
      }
    }
    // A queue of all splits to test
    Queue<SplitToTest> splitsToTest = new ArrayDeque<SplitToTest>();
    // Start from the first (root) split
    splitsToTest.add(new SplitToTest(1, 0));
    
    while (!splitsToTest.isEmpty()) {
      SplitToTest splitToTest = splitsToTest.remove();
      if (splitToTest.splitID >= splits.length) {
        // Matched a partition. return it
        matcher.collect(splitToTest.splitID);
      } else {
        // Need to test that split
        if (splitToTest.direction == 0) {
          // The corresponding split is vertical (along the x-axis). Like |
          if (shapeMBR.x1 < splits[splitToTest.splitID])
            splitsToTest.add(new SplitToTest(splitToTest.splitID * 2, 1 ^ splitToTest.direction)); // Go left
          if (shapeMBR.x2 > splits[splitToTest.splitID])
            splitsToTest.add(new SplitToTest(splitToTest.splitID * 2 + 1, 1 ^ splitToTest.direction)); // Go right
        } else {
          // The corresponding split is horizontal (along the y-axis). Like -
          if (shapeMBR.y1 < splits[splitToTest.splitID])
            splitsToTest.add(new SplitToTest(splitToTest.splitID * 2, 1 ^ splitToTest.direction));
          if (shapeMBR.y2 > splits[splitToTest.splitID])
            splitsToTest.add(new SplitToTest(splitToTest.splitID * 2 + 1, 1 ^ splitToTest.direction));
        }
      }
    }
  }

  /**
   * @param shape
   * @return
   */
  public int overlapPartition(Shape shape) {
    if (shape == null || shape.getMBR() == null)
      return -1;
    Point pt = shape.getMBR().getCenterPoint();
    int splitID = 1; // Start from the root
    int direction = 0;
    while (splitID < splits.length) {
      if (direction == 0) {
        // The corresponding split is vertical (along the x-axis). Like |
        if (pt.x < splits[splitID])
          splitID = splitID * 2; // Go left
        else
          splitID = splitID * 2 + 1; // Go right
      } else {
        // The corresponding split is horizontal (along the y-axis). Like -
        if (pt.y < splits[splitID])
          splitID = splitID * 2;
        else
          splitID = splitID * 2 + 1;
      }
      direction ^= 1;
    }
    return splitID;
  }
  
  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(index + splits.length);
  }

  @Override
  public CellInfo getPartition(int id) {
    CellInfo cellInfo = new CellInfo(id, mbr);
    boolean minXFound = false, minYFound = false,
        maxXFound = false, maxYFound = false;
    // Direction 0 means x-axis and 1 means y-axis
    int direction = getNumberOfSignificantBits(id) & 1;
    while (id > 1) {
      // 0 means maximum and 1 means minimum
      int minOrMax = id & 1;
      id >>>= 1;
      if (minOrMax == 0 && direction == 0 && !maxXFound) {
        cellInfo.x2 = splits[id]; maxXFound = true;
      } else if (minOrMax == 0 && direction == 1 && !maxYFound) {
        cellInfo.y2 = splits[id]; maxYFound = true;
      } else if (minOrMax == 1 && direction == 0 && !minXFound) {
        cellInfo.x1 = splits[id]; minXFound = true;
      } else if (minOrMax == 1 && direction == 1 && !minYFound) {
        cellInfo.y1 = splits[id]; minYFound = true;
      }
      direction ^= 1;
    }
    return cellInfo;
  }
  
  /**
   * Get number of significant bits. In other words, get the highest position of
   * a bit that is set to 1 and add 1 to that position. For the value zero,
   * number of significant bits is zero.
   * 
   * @param x
   * @return
   */
  public static int getNumberOfSignificantBits(int x) {
    int numOfSignificantBits = 0;
    if ((x & 0xffff0000) != 0) {
      // There's some bit on the upper word that is not zero
      numOfSignificantBits += 16;
      x >>>= 16;
    }
    if ((x & 0xff00) != 0) {
      // There's some non-zero bit in the upper byte
      numOfSignificantBits += 8;
      x >>>= 8;
    }
    if ((x & 0xf0) != 0) {
      numOfSignificantBits += 4;
      x >>>= 4;
    }
    if ((x & 0xC) != 0) {
      numOfSignificantBits += 2;
      x >>>= 2;
    }
    if ((x & 0x2) != 0) {
      numOfSignificantBits += 1;
      x >>>= 1;
    }
    if ((x & 0x1) != 0) {
      numOfSignificantBits += 1;
      //id >>>= 1; // id will always be zero
    }
    return numOfSignificantBits;
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
    
    KdTreePartitioner kdp = new KdTreePartitioner();
    kdp.createFromPoints(inMBR, points.toArray(new Point[points.size()]), 7);
    System.out.println("x,y,partition");
    int[] sizes = new int[kdp.getPartitionCount()*2];
    for (Point p : points) {
      int partition = kdp.overlapPartition(p);
      //System.out.println(p.x+","+p.y+","+partition);
      sizes[partition]++;
    }
    for (int i = 0; i < sizes.length; i++)
      System.out.print(sizes[i]+",");
  }
}
