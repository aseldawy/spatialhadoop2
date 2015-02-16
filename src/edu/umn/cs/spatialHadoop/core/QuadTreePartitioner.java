/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
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
 * Partition the space based on a Quad tree
 * @author Ahmed Eldawy
 *
 */
public class QuadTreePartitioner extends ZCurvePartitioner {
  private static final Log LOG = LogFactory.getLog(QuadTreePartitioner.class);

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public QuadTreePartitioner() {
  }

  /**
   * Constructs a new grid partitioner which is used for indexing
   * @param inPath
   * @param job
   * @throws IOException 
   */
  public static QuadTreePartitioner createIndexingPartitioner(Path inPath,
      Path outPath, JobConf job) throws IOException {
    final Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    // Determine number of partitions
    long inSize = FileUtil.getPathSize(inPath.getFileSystem(job), inPath);
    FileSystem outFS = outPath.getFileSystem(job);
    long outBlockSize = outFS.getDefaultBlockSize(outPath);
    int partitions = (int) (inSize / outBlockSize);
    LOG.info("Quad tree to partition the space into "+partitions+" partitions");
    
    // Sample of the input file and each point is mapped to a Z-value
    final Vector<Long> zValues = new Vector<Long>();
    
    float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);
    
    LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
    ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
      @Override
      public void collect(Point p) {
        zValues.add(computeZ(inMBR, p.x, p.y));
      }
    };
    OperationsParams params2 = new OperationsParams(job);
    params2.setFloat("ratio", sample_ratio);
    params2.setLong("size", sample_size);
    params2.setClass("outshape", Point.class, Shape.class);
    Sampler.sample(new Path[] {inPath}, resultCollector, params2);
    LOG.info("Finished reading a sample of "+zValues.size()+" records");

    QuadTreePartitioner p = createFromZValues(zValues.toArray(new Long[zValues.size()]), inMBR, partitions);
    
    return p;
  }
  
  public static QuadTreePartitioner createFromPoints(final Vector<Point> points,
      final Rectangle inMBR, int partitions) {
    Vector<Long> zValues = new Vector<Long>(points.size());
    for (Point p : points) {
      zValues.add(computeZ(inMBR, p.x, p.y));
    }
    QuadTreePartitioner p = createFromZValues(
        zValues.toArray(new Long[zValues.size()]), inMBR, partitions);
    return p;
  }

  /**
   * Create a ZCurvePartitioner from a list of points
   * @param vsample
   * @param inMBR
   * @param partitions
   * @return
   */
  public static QuadTreePartitioner createFromZValues(final Long[] zValues,
      final Rectangle inMBR, int partitions) {
    int nodeCapacity = zValues.length / partitions;
    Arrays.sort(zValues);
    class QuadTreeNode {
      int fromIndex, toIndex;
      long minZ, maxZ;
      int nodeID; // A unique ID of the node
      int depth; // Depth in the tree starting with ONE at the root
      
      public QuadTreeNode(int fromIndex, int toIndex, long minZ, long maxZ,
          int nodeID, int depth) {
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
        this.minZ = minZ;
        this.maxZ = maxZ;
        this.nodeID = nodeID;
        this.depth = depth;
      }
    }
    
    long minZ = computeZ(inMBR, inMBR.x1, inMBR.y1); // Always zero
    long maxZ = computeZ(inMBR, inMBR.x2, inMBR.y2);
    QuadTreeNode root = new QuadTreeNode(0, zValues.length, minZ, maxZ, 1, 1);
    Queue<QuadTreeNode> nodesToSplit = new ArrayDeque<QuadTreeNode>();
    nodesToSplit.add(root);
    
    Vector<Long> zSplits = new Vector<Long>();
    
    while (!nodesToSplit.isEmpty()) {
      QuadTreeNode nodeToSplit = nodesToSplit.remove();
      if (nodeToSplit.toIndex - nodeToSplit.fromIndex <= nodeCapacity) {
        // No need to split
        zSplits.add(nodeToSplit.maxZ);
      } else {
        // The position of the lowest of the two bits that change for these
        // children in the Z-order
        // For the root, we change the two highest bits in the zOrder
        int changedBits =
            KdTreePartitioner.getNumberOfSignificantBits(Resolution) * 2 -
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
    
    QuadTreePartitioner p = new QuadTreePartitioner();
    p.mbr = new Rectangle(inMBR);
    p.zSplits = new long[zSplits.size()];
    for (int i = 0; i < zSplits.size(); i++) {
      p.zSplits[i] = zSplits.get(i);
    }
    Arrays.sort(p.zSplits);
    return p;
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
    
    QuadTreePartitioner qtp = createFromPoints(points, inMBR, 8);
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = qtp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
    
    for (int i = 0; i < qtp.getPartitionCount(); i++) {
      System.out.println(qtp.getPartition(i).toWKT());
    }
  }

}
