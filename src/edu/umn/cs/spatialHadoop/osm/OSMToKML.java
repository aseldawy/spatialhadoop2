/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayRecordReader;

/**
 * Converts a text file containing shapes to a KML file
 * @author Ahmed Eldawy
 *
 */
public class OSMToKML {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    final OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    if (!params.checkInputOutput()) {
      System.err.println("Please specify input and output");
      System.exit(1);
    }
    params.setClass("shape", OSMPolygon.class, Shape.class);
    Path inputPath = params.getInputPath();
    FileSystem inFs = inputPath.getFileSystem(params);
    ShapeArrayRecordReader in = new ShapeArrayRecordReader(params,
        new FileSplit(inputPath, 0, inFs.getFileStatus(inputPath).getLen(),
            new String[0]));
    Path outPath = params.getOutputPath();
    FileSystem outFs = outPath.getFileSystem(params);
    PrintWriter out = new PrintWriter(outFs.create(outPath));
    out.println("<?xml version='1.0' encoding='UTF-8'?>");
    out.println("<kml xmlns='http://www.opengis.net/kml/2.2'>");
    out.println("<Document>");
    Rectangle key = in.createKey();
    ArrayWritable values = in.createValue();
    while (in.next(key, values)) {
      for (Shape shape : (Shape[]) values.get()) {
        if (shape instanceof OSMPolygon) {
          out.println(((OSMPolygon) shape).toKMLElement());
        }
      }
      out.println();
    }
    out.println("</Document>");
    out.println("</kml>");
    in.close();
    out.close();
  }

}
