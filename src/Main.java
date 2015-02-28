import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.KdTreePartitioner;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Partitioner;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.QuadTreePartitioner;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.STRPartitioner;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;


/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which 
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/

/**
 * @author Eldawy
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
      OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
      
      Path inPath = params.getInputPath();
      long length = inPath.getFileSystem(params).getFileStatus(inPath).getLen();
      ShapeIterRecordReader reader = new ShapeIterRecordReader(params,
          new FileSplit(inPath, 0, length, new String[0]));
      Rectangle key = reader.createKey();
      ShapeIterator shapes = reader.createValue();
      
      while (reader.next(key, shapes)) {
        for (Shape s : shapes) {
          System.out.println(((Partition)s).toWKT());
        }
      }
      
      reader.close();
      
      System.exit(0);
      
      
      
      final Vector<Point> points = new Vector<Point>();
      Rectangle inMBR = (Rectangle)OperationsParams.getShape(params, "mbr");
      while (reader.next(key, shapes)) {
        for (Shape s : shapes) {
          if (s.getMBR() != null) {
            Point centerPoint = s.getMBR().getCenterPoint();
            boolean bad_point = false;
            for (Point p : points) {
              if (p.distanceTo(centerPoint) < 1.0) {
                bad_point = true;
                break;
              }
            }
            if (!bad_point)
              points.add(centerPoint);
          }
          if (points.size() > 256)
            break;
        }
        if (points.size() > 256)
          break;
      }
      for (Point p : points) {
        System.out.println(p);
      }
      System.out.println("All points "+points.size());

      Partitioner kdp = new KdTreePartitioner();
//      Partitioner kdp = HilbertCurvePartitioner.createFromPoints(points, inMBR, 9);
//      Partitioner kdp = ZCurvePartitioner.createFromPoints(points, inMBR, 9);
//      Partitioner kdp = QuadTreePartitioner.createFromPoints(points, inMBR, 9);
//      Partitioner kdp = STRPartitioner.createFromPoints(inMBR, 3, 3, points.toArray(new Point[points.size()]));
      kdp.createFromPoints(inMBR, points.toArray(new Point[points.size()]), 9);

      if (kdp instanceof KdTreePartitioner || kdp instanceof QuadTreePartitioner
          || kdp instanceof STRPartitioner) {
        for (int i = 0; i < kdp.getPartitionCount(); i++) {
          System.out.println(kdp.getPartitionAt(i));
        }
      } else {
        // Get tight MBR around partitions
        Map<Integer, Rectangle> mbrs = new HashMap<Integer, Rectangle>();
        for (Point p : points) {
          int partitionID = kdp.overlapPartition(p);
          Rectangle mbr = mbrs.get(partitionID);
          if (mbr == null) {
            mbr = p.getMBR();
            mbrs.put(partitionID, mbr);
          } else {
            mbr.expand(p);
          }
        }
        for (Rectangle mbr : mbrs.values())
          System.out.println(mbr);
      }
    }
}