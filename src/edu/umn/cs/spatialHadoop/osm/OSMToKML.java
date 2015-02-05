/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
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

  /**
   * Returns the style name that should be used to draw the given object.
   * @param polygon
   * @return
   */
  public static String getStyle(OSMPolygon polygon) {
    if (HasTag.hasTag(polygon.tags, "amenity,building", "yes"))
      return "building";
    if (HasTag.hasTag(polygon.tags, "natural,waterway", "bay,wetland,water,coastline,riverbank,dock,boatyard"))
      return "lake";
    if (HasTag.hasTag(polygon.tags, "leisure,boundary,landuse,natural",
        "wood,tree_row,grassland,park,golf_course,national_park,garden,nature_reserve,forest,grass,tree,orchard,farmland,protected_area"))
      return "park";
    if (HasTag.hasTag(polygon.tags, "landuse", "cemetery"))
      return "cemetery";
    if (HasTag.hasTag(polygon.tags, "leisure,landuse",
        "sports_centre,stadium,track,pitch,golf_course,water_park,swimming_pool,recreation_ground,piste"))
      return "sport";
    if (HasTag.hasTag(polygon.tags, "admin_level", "8"))
      return "postal_code";
    return null;
  }
  
  public static void writeAllStyles(PrintWriter out) {
    out.println("<Style id='lake'>");
    out.println("<LineStyle>");
    out.println("<color>ffff0000</color>");
    out.println("<width>3.0</width>");
    out.println("</LineStyle>");
    out.println("<PolyStyle>");
    out.println("<color>3dff0000</color>");
    out.println("</PolyStyle>");
    out.println("</Style>");

    out.println("<Style id='park'>");
    out.println("<LineStyle>");
    out.println("<color>ff00ff00</color>");
    out.println("<width>2.0</width>");
    out.println("</LineStyle>");
    out.println("<PolyStyle>");
    out.println("<color>3d00ff00</color>");
    out.println("</PolyStyle>");
    out.println("</Style>");
    
    out.println("<Style id='cemetery'>");
    out.println("<LineStyle>");
    out.println("<color>ff000000</color>");
    out.println("<width>2.0</width>");
    out.println("</LineStyle>");
    out.println("<PolyStyle>");
    out.println("<color>1d000000</color>");
    out.println("</PolyStyle>");
    out.println("</Style>");
    
    out.println("<Style id='postal_code'>");
    out.println("<LineStyle>");
    out.println("<color>ffffffff</color>");
    out.println("<width>2.0</width>");
    out.println("</LineStyle>");
    out.println("<PolyStyle>");
    out.println("<color>00000000</color>");
    out.println("</PolyStyle>");
    out.println("</Style>");
    
    out.println("<Style id='sport'>");
    out.println("<LineStyle>");
    out.println("<color>ff0ec9ff</color>");
    out.println("<width>2.0</width>");
    out.println("</LineStyle>");
    out.println("<PolyStyle>");
    out.println("<color>3d0ec9ff</color>");
    out.println("</PolyStyle>");
    out.println("</Style>");
    
    out.println("<Style id='building'>");
    out.println("<LineStyle>");
    out.println("<color>ff000000</color>");
    out.println("<width>2.0</width>");
    out.println("</LineStyle>");
    out.println("<PolyStyle>");
    out.println("<color>7fffffff</color>");
    out.println("</PolyStyle>");
    out.println("</Style>");
  }
  
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
    } else if (geom != null) {
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
    str.append("<styleUrl>#"+getStyle(osm)+"</styleUrl>");
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
    writeAllStyles(out);
    Rectangle key = in.createKey();
    ArrayWritable values = in.createValue();
    while (in.next(key, values)) {
      System.out.println("Read "+values.get().length);
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
