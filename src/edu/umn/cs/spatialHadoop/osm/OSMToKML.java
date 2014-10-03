/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

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

  
  public static void geometryToKML(StringBuilder str, Geometry geom) {
    if (geom instanceof Point) {
      Point pt = (Point) geom;
      str.append("<Point>");
      str.append("<coordinates>");
      str.append(pt.getX()+","+pt.getY());
      str.append("</coordinates>");
      str.append("</Point>");
    } else if (geom instanceof LinearRing) {
      LinearRing linearRing = (LinearRing) geom;
      str.append("<LinearRing>");
      str.append("<coordinates>");
      for (Coordinate coord : linearRing.getCoordinates())
        str.append(coord.x+","+coord.y+" ");
      str.append("</coordinates>");
      str.append("</LinearRing>");
    } else if (geom instanceof LineString) {
      LineString linestring = (LineString) geom;
      str.append("<LineString>");
      str.append("<coordinates>");
      for (Coordinate coord : linestring.getCoordinates())
        str.append(coord.x+","+coord.y+" ");
      str.append("</coordinates>");
      str.append("</LineString>");
    } else if (geom instanceof Polygon) {
      Polygon polygon = (Polygon) geom;
      str.append("<Polygon>");

      // Write exterior boundary
      str.append("<outerBoundaryIs>");
      geometryToKML(str, polygon.getExteriorRing());
      str.append("</outerBoundaryIs>");

      // Write all interior boundaries
      for (int n = 0; n < polygon.getNumInteriorRing(); n++) {
        str.append("<innerBoundaryIs>");
        geometryToKML(str, polygon.getInteriorRingN(n));
        str.append("</innerBoundaryIs>");
      }
      str.append("</Polygon>");
    } else if (geom instanceof GeometryCollection) {
      GeometryCollection geomCollection = (GeometryCollection) geom;
      str.append("<GeometryCollection>");
      for (int n = 0; n < geomCollection.getNumGeometries(); n++) {
        geometryToKML(str, geomCollection.getGeometryN(n));
      }
      str.append("</GeometryCollection>");
    } else {
      throw new RuntimeException("Cannot generate KML from '"+geom.getClass()+"'");
    }
  }
  
  /**
   * Returns a KML representation of this object.
   * @return
   */
  public static String OSMtoKMLElement(OSMPolygon osm) {
    StringBuilder str = new StringBuilder();
    str.append("<Placemark>");
    if (osm.tags != null) {
      if (osm.tags.containsKey("name")) {
        str.append("<name>");
        str.append("<![CDATA[");
        str.append(osm.tags.get("name"));
        str.append("]]>");
        str.append("</name>");
      }
      str.append("<description>");
      str.append("<![CDATA[");
      str.append(osm.tags.toString());
      str.append("]]>");
      str.append("</description>");
      
      geometryToKML(str, osm.geom);
    }
    str.append("</Placemark>");
    return str.toString();
  }
  
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
    PrintWriter out;
    ZipOutputStream zipOut = null;
    if (outPath.getName().toLowerCase().endsWith(".kmz")) {
      // Create a KMZ file
      FSDataOutputStream kmzOut = outFs.create(outPath);
      zipOut = new ZipOutputStream(kmzOut);
      zipOut.putNextEntry(new ZipEntry("osm.kml"));
      out = new PrintWriter(zipOut);
    } else {
      out = new PrintWriter(outFs.create(outPath));
    }
    out.println("<?xml version='1.0' encoding='UTF-8'?>");
    out.println("<kml xmlns='http://www.opengis.net/kml/2.2'>");
    out.println("<Document>");
    Rectangle key = in.createKey();
    ArrayWritable values = in.createValue();
    while (in.next(key, values)) {
      for (Shape shape : (Shape[]) values.get()) {
        if (shape instanceof OSMPolygon) {
          out.println(OSMtoKMLElement((OSMPolygon) shape));
        }
      }
      out.println();
    }
    out.println("</Document>");
    out.println("</kml>");
    in.close();
    if (zipOut != null) {
      // KMZ file
      out.flush();
      zipOut.closeEntry();
      zipOut.close();
    } else {
      // KML file
      out.close();
    }
  }

}
