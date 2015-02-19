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
import java.util.Arrays;
import java.util.Collections;
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
 * @author Ahmed Eldawy
 *
 */
public class HilbertCurvePartitioner extends Partitioner {
  private static final Log LOG = LogFactory.getLog(HilbertCurvePartitioner.class);

  /**Splits along the Hilbert curve*/
  protected int[] splits;
  
  /**The MBR of the original input file*/
  protected Rectangle mbr;

  protected static final int Resolution = Short.MAX_VALUE;
  
  public HilbertCurvePartitioner() {
  }
  
  /**
   * Constructs a new grid partitioner which is used for indexing
   * @param inPath
   * @param job
   * @throws IOException 
   */
  public static HilbertCurvePartitioner createIndexingPartitioner(Path inPath,
      Path outPath, JobConf job) throws IOException {
    long t1 = System.currentTimeMillis();
    final Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    // Determine number of partitions
    long inSize = FileUtil.getPathSize(inPath.getFileSystem(job), inPath);
    FileSystem outFS = outPath.getFileSystem(job);
    long outBlockSize = outFS.getDefaultBlockSize(outPath);
    int partitions = Math.max(1, (int) (inSize / outBlockSize));
    LOG.info("Z-cruve to partition the space into "+partitions+" partitions");
    
    // Sample of the input file and each point is mapped to a Z-value
    final Vector<Integer> hValues = new Vector<Integer>();
    
    float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);
    
    LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
    ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
      @Override
      public void collect(Point p) {
        hValues.add(computeHValue(inMBR, p.x, p.y));
      }
    };
    OperationsParams params2 = new OperationsParams(job);
    params2.setFloat("ratio", sample_ratio);
    params2.setLong("size", sample_size);
    params2.setClass("outshape", Point.class, Shape.class);
    Sampler.sample(new Path[] {inPath}, resultCollector, params2);
    LOG.info("Finished reading a sample of "+hValues.size()+" records");
    long t2 = System.currentTimeMillis();
    System.out.println("Total time for sampling in millis: "+(t2-t1));
    
    HilbertCurvePartitioner p = createFromHValues(hValues, inMBR, partitions);
    return p;
  }
  
  public static HilbertCurvePartitioner createFromPoints(final Vector<Point> points,
      final Rectangle inMBR, int partitions) {
    Vector<Integer> hValues = new Vector<Integer>(points.size());
    for (Point p : points)
      hValues.add(computeHValue(inMBR, p.x, p.y));
    HilbertCurvePartitioner p = createFromHValues(hValues, inMBR, partitions);
    return p;
  }

  
  /**
   * Create a ZCurvePartitioner from a list of points
   * @param vsample
   * @param inMBR
   * @param partitions
   * @return
   */
  public static HilbertCurvePartitioner createFromHValues(final Vector<Integer> hValues,
      final Rectangle inMBR, int partitions) {
    Collections.sort(hValues);
    
    HilbertCurvePartitioner p = new HilbertCurvePartitioner();
    p.mbr = new Rectangle(inMBR);
    p.splits = new int[partitions];
    int maxH = 0x7fffffff;
    for (int i = 0; i < partitions; i++) {
      int quantile = (i + 1) * hValues.size() / partitions;
      p.splits[i] = quantile == hValues.size() ? maxH : hValues.get(quantile);
    }
    return p;
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
    if (mbr == null)
      mbr = new Rectangle();
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
    
    HilbertCurvePartitioner hcp = createFromPoints(points, inMBR, 10);
    
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = hcp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  }
}
