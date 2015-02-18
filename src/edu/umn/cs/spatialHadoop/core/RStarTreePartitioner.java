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
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import com.github.davidmoten.rtree.geometry.Geometries;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * A partitioner that partitioner data by bulk loading them into an R* tree
 * @author Ahmed Eldawy
 *
 */
public class RStarTreePartitioner extends Partitioner {
  private static final Log LOG = LogFactory.getLog(RStarTreePartitioner.class);

  /**MBR of the input file*/
  protected Rectangle mbr;
  /**A list of all cells*/
  protected CellInfo[] cells;
  
  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public RStarTreePartitioner() {
  }

  /**
   * Constructs a new grid partitioner which is used for indexing
   * @param inPath
   * @param job
   * @throws IOException 
   */
  public static RStarTreePartitioner createIndexingPartitioner(Path inPath,
      Path outPath, JobConf job) throws IOException {
    Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    
    // Determine number of partitions
    long inSize = FileUtil.getPathSize(inPath.getFileSystem(job), inPath);
    FileSystem outFS = outPath.getFileSystem(job);
    long outBlockSize = outFS.getDefaultBlockSize(outPath);
    int partitionCount = Math.min(1, (int) (inSize / outBlockSize));
    LOG.info("R* tree partitiong into "+partitionCount+" partitions");
    
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
    LOG.info("Finished reading a sample of "+vsample.size()+" records");
    
    RStarTreePartitioner rstp = createFromPoints(inMBR, vsample, partitionCount);
    return rstp;
  }

  public static RStarTreePartitioner createFromPoints(Rectangle inMBR,
      final Vector<Point> vsample, int partitionCount) {
    int maxChildrenPerPartition = vsample.size() / partitionCount;
    // Load them into an R*-tree
    com.github.davidmoten.rtree.RTree<Integer, com.github.davidmoten.rtree.geometry.Point> rtree =
        com.github.davidmoten.rtree.RTree.star().maxChildren(maxChildrenPerPartition).minChildren(maxChildrenPerPartition/2).create();
    for (int i = 0; i < vsample.size(); i++) {
      rtree = rtree.add(i, Geometries.point(vsample.get(i).x, vsample.get(i).y));
    }
    // Since there's no way to retrieve the tree structure directly, we convert
    // it to text and extract leaf nodes from the text
    int treeDepth = rtree.calculateDepth();
    String treeString = rtree.asString();
    Pattern nodePattern = Pattern.compile("(\\s*)mbr=Rectangle \\[x1=(.*), y1=(.*), x2=(.*), y2=(.*)\\]");
    Matcher matcher = nodePattern.matcher(treeString);
    Vector<CellInfo> leafNodes = new Vector<CellInfo>();
    while (matcher.find()) {
      int nodeDepth = matcher.group(1).length() / 2 + 1;
      if (nodeDepth == treeDepth) {
        // Matched a leaf node
        CellInfo cell = new CellInfo();
        cell.cellId = leafNodes.size();
        cell.x1 = Double.parseDouble(matcher.group(2));
        cell.y1 = Double.parseDouble(matcher.group(3));
        cell.x2 = Double.parseDouble(matcher.group(4));
        cell.y2 = Double.parseDouble(matcher.group(5));
        leafNodes.add(cell);
      }
    }
    
    RStarTreePartitioner rstp = new RStarTreePartitioner();
    rstp.mbr = inMBR;
    rstp.cells = leafNodes.toArray(new CellInfo[leafNodes.size()]);
    return rstp;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(cells.length);
    for (CellInfo cell : cells)
      cell.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    if (mbr == null)
      mbr = new Rectangle();
    mbr.readFields(in);
    int cellCount = in.readInt();
    cells = new CellInfo[cellCount];
    for (int i = 0; i < cells.length; i++) {
      cells[i] = new CellInfo();
      cells[i].readFields(in);
    }
  }
  
  @Override
  public int getPartitionCount() {
    return cells.length;
  }
  
  @Override
  public CellInfo getPartitionAt(int index) {
    return cells[index];
  }
  
  @Override
  public CellInfo getPartition(int partitionID) {
    // Partition ID is the same as cell index in the array
    return getPartitionAt(partitionID);
  }
  
  @Override
  public int overlapPartition(Shape shape) {
    if (shape == null || shape.getMBR() == null)
      return -1;
    Rectangle shapeMBR = shape.getMBR();
    for (CellInfo cell : cells) {
      if (cell.isIntersected(shapeMBR))
        return cell.cellId;
    }
    // Does not overlap any cell
    return -1;
  }
  
  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null || shape.getMBR() == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    for (CellInfo cell : cells) {
      if (cell.isIntersected(shapeMBR))
        matcher.collect(cell.cellId);
    }
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
    
    RStarTreePartitioner rstp = createFromPoints(inMBR, points, 10);
    
    System.out.println("ID\tGeomtry");
    for (int i = 0; i < rstp.getPartitionCount(); i++) {
      System.out.println(rstp.getPartitionAt(i).toWKT());
    }

  }

}
