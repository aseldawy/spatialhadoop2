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
import java.util.Comparator;
import java.util.Queue;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * A partitioner that partitioner data using a K-d tree-based partitioner.
 * @author Ahmed Eldawy
 *
 */
public class KdTreePartitioner extends Partitioner {
  private static final Log LOG = LogFactory.getLog(KdTreePartitioner.class);

  /**MBR of the input file*/
  private Rectangle mbr;
  
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

  /**
   * Constructs a new grid partitioner which is used for indexing
   * @param inPath
   * @param job
   * @throws IOException 
   */
  public static KdTreePartitioner createIndexingPartitioner(Path inPath,
      Path outPath, JobConf job, boolean replicate) throws IOException {
    Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    
    // Determine number of partitions
    long inSize = FileUtil.getPathSize(inPath.getFileSystem(job), inPath);
    FileSystem outFS = outPath.getFileSystem(job);
    long outBlockSize = outFS.getDefaultBlockSize(outPath);
    int partitions = (int) (inSize / outBlockSize);
    LOG.info("K-d tree partitiong into "+partitions+" partitions");
    
    // Draw a random sample of the input file
    final Vector<Point> vsample = new Vector<Point>();
    
    float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);

    LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
    ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
      @Override
      public void collect(Point value) {
        vsample.add(value.clone());
      }
    };
    OperationsParams params2 = new OperationsParams(job);
    params2.setFloat("ratio", sample_ratio);
    params2.setLong("size", sample_size);
    params2.setClass("outshape", Point.class, Shape.class);
    Sampler.sample(new Path[] {inPath}, resultCollector, params2);
    // Convert to array to be able to sort subranges
    Point[] points = vsample.toArray(new Point[vsample.size()]);
    vsample.clear();
    LOG.info("Finished reading a sample of "+points.length+" records");

    KdTreePartitioner kdp = createFromPoints(inMBR, partitions, points);
    
    return kdp;
  }

  public static KdTreePartitioner createFromPoints(Rectangle inMBR,
      int partitions, Point[] points) {
    // Enumerate all partition IDs to be able to count leaf nodes in any split
    // TODO do the same functionality without enumerating all IDs
    String[] ids = new String[partitions];
    for (int id = partitions; id < 2 * partitions; id++)
      ids[id - partitions] = Integer.toBinaryString(id);
    
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
    
    KdTreePartitioner kdp = new KdTreePartitioner();
    kdp.mbr = new Rectangle(inMBR);
    kdp.splits = new double[partitions];
    
    while (!splitTasks.isEmpty()) {
      SplitTask splitTask = splitTasks.remove();
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
      int splitIndex = (size_child1 * splitTask.toIndex + size_child2 * splitTask.fromIndex)
          / (size_child1 + size_child2);
      partialQuickSort(points, splitTask.fromIndex, splitTask.toIndex,
          splitIndex, comparators[splitTask.direction]);
      Point splitValue = points[splitIndex];
      kdp.splits[splitTask.partitionID] = splitTask.direction == 0 ?
          splitValue.x : splitValue.y;
      if (splitTask.partitionID * 2 < partitions) {
        splitTasks.add(new SplitTask(splitTask.fromIndex, splitIndex,
            1 - splitTask.direction, splitTask.partitionID * 2));
      }
      if (splitTask.partitionID * 2 + 1 < partitions) {
        splitTasks.add(new SplitTask(splitIndex, splitTask.toIndex,
            1 - splitTask.direction, splitTask.partitionID * 2 + 1));
      }
    }
    return kdp;
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
    if (mbr == null)
      mbr = new Rectangle();
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
    // TODO match with all overlapping partitions
    int partitionID = overlapPartition(shape);
    if (partitionID >= 0)
      matcher.collect(partitionID);
  }

  /**
   * @param shape
   * @return
   */
  public int overlapPartition(Shape shape) {
    if (shape == null || shape.getMBR() == null)
      return -1;
    Point pt = shape.getMBR().getCenterPoint();
    int partitionID = 1; // Start from the root
    int direction = 0;
    while (partitionID < splits.length) {
      if (direction == 0) {
        // The corresponding split is vertical (along the x-axis). Like |
        if (pt.x < splits[partitionID])
          partitionID = partitionID * 2; // Go left
        else
          partitionID = partitionID * 2 + 1; // Go right
      } else {
        // The corresponding split is horizontal (along the y-axis). Like -
        if (pt.y < splits[partitionID])
          partitionID = partitionID * 2;
        else
          partitionID = partitionID * 2 + 1;
      }
      direction ^= 1;
    }
    return partitionID;
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
    
    KdTreePartitioner kdp = createFromPoints(inMBR, 5, points.toArray(new Point[points.size()]));
  }
}
