/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.awt.Color;
import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPoint;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A shape class that represents an OGC compliant geometry. The geometry is
 * enclosed inside the class and all calls are delegated to it. The class also
 * holds extra information for each records that could represent other columns
 * for each records. The text representation is assumed to be some kind of CSV.
 * The shape is always the first column in that CSV. The text representation of
 * the shape could be either a WTK (Well Known Text) or a binary representation.
 * The WKT can be generated with PostGIS using the function ST_AsText(geom). An
 * example may look like this:<br>
 * <code>
 * POLYGON ((-89 43,-89 50,-97 50,-97 43,-89 43))
 * </code> The binary representation can be generated from PostGIS by selecting
 * the geom column using a normal select statement. When a shape is parsed, we
 * detect the format and use the appropriate parser. When writing to text, we
 * always use the binary representation as it is faster and more compact. For
 * binary serialization/deserialization, we use the PostGIS writer and parser.
 * 
 * @author Ahmed Eldawy
 * 
 */
public class OGCESRIShape implements Shape {
  
  private static final Log LOG = LogFactory.getLog(OGCESRIShape.class);
  
  /**
   * The underlying geometry
   */
  public OGCGeometry geom;
  
  public OGCESRIShape() {
    this(null);
  }
  
  public OGCESRIShape(OGCGeometry geom) {
    this.geom = geom;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = geom.asBinary().array();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    byte[] bytes = new byte[size];
    in.readFully(bytes);
    geom = OGCGeometry.fromBinary(ByteBuffer.wrap(bytes));
  }

  
  /**
   * Convert a string containing a hex string to a byte array of binary.
   * For example, the string "AABB" is converted to the byte array {0xAA, 0XBB}
   * @param hex
   * @return
   */
  public static byte[] hexToBytes(String hex) {
    byte[] bytes = new byte[(hex.length() + 1) / 2];
    for (int i = 0; i < hex.length(); i++) {
      byte x = (byte) hex.charAt(i);
      if (x >= '0' && x <= '9')
        x -= '0';
      else if (x >= 'a' && x <= 'f')
        x = (byte) ((x - 'a') + 0xa);
      else if (x >= 'A' && x <= 'F')
        x = (byte) ((x - 'A') + 0xA);
      else
        throw new RuntimeException("Invalid hex char "+x);
      if (i % 2 == 0)
        x <<= 4;
      bytes[i / 2] |= x;
    }
    return bytes;
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeGeometry(text, geom, '\0');
    return text;
  }

  @Override
  public void fromText(Text text) {
    try {
      geom = TextSerializerHelper.consumeGeometryESRI(text, '\0');
    } catch (RuntimeException e) {
      LOG.error("Error parsing: "+text);
      throw e;
    }
  }

  @Override
  public Rectangle getMBR() {
    if (geom.isEmpty())
      return null;
    com.esri.core.geometry.Polygon mbr = (com.esri.core.geometry.Polygon) geom.envelope().getEsriGeometry();
    int pointCount = mbr.getPointCount();
    double xmin = mbr.getPoint(0).getX();
    double ymin = mbr.getPoint(0).getY();
    double xmax = xmin, ymax = ymin;
    for (int i = 1; i < pointCount; i++) {
      com.esri.core.geometry.Point point = mbr.getPoint(i);
      if (point.getX() < xmin)
        xmin = point.getX();
      if (point.getX() > xmax)
        xmax = point.getX();
      if (point.getY() < ymin)
        ymin = point.getY();
      if (point.getY() > ymax)
        ymax = point.getY();
    }
    
    return new Rectangle(xmin, ymin, xmax, ymax);
  }

  @Override
  public double distanceTo(double x, double y) {
    OGCPoint point = new OGCPoint(new com.esri.core.geometry.Point(x, y), this.geom.getEsriSpatialReference());
    return this.geom.distance(point);
  }

  @Override
  public boolean isIntersected(Shape s) {
    Rectangle s_mbr = s.getMBR();
    Rectangle this_mbr = this.getMBR();
    if (s_mbr == null || this_mbr == null)
      return false;
    // Filter step
    if (!s_mbr.isIntersected(this_mbr))
      return false;
    try {
      if (s instanceof OGCESRIShape)
        return geom.intersects(((OGCESRIShape)s).geom);
    } catch (NullPointerException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    ArrayList<OGCGeometry> points = new ArrayList<OGCGeometry>();
    points.add(new OGCPoint(new com.esri.core.geometry.Point(s_mbr.x1, s_mbr.y1), geom.getEsriSpatialReference()));
    points.add(new OGCPoint(new com.esri.core.geometry.Point(s_mbr.x2, s_mbr.y2), geom.getEsriSpatialReference()));
    
    OGCGeometryCollection all_points = new OGCConcreteGeometryCollection(points, geom.getEsriSpatialReference());
    
    return geom.intersects(all_points.envelope());
  }

  @Override
  public Shape clone() {
    OGCESRIShape copy = new OGCESRIShape(this.geom);
    return copy;
  }
  
  @Override
  public String toString() {
    return geom.asText();
  }
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    OGCGeometry geom = this.geom;
    Color shape_color = g.getColor();
    if (geom instanceof OGCGeometryCollection) {
      OGCGeometryCollection geom_coll = (OGCGeometryCollection) geom;
      for (int i = 0; i < geom_coll.numGeometries(); i++) {
        OGCGeometry sub_geom = geom_coll.geometryN(i);
        // Recursive call to draw each geometry
        new OGCESRIShape(sub_geom).draw(g, fileMBR, imageWidth, imageHeight, scale);
      }
    } else if (geom.getEsriGeometry() instanceof MultiPath) {
      MultiPath path = (MultiPath) geom.getEsriGeometry();
      double sub_geom_alpha = path.calculateLength2D() * scale;
      int color_alpha = sub_geom_alpha > 1.0 ? 255 : (int) Math.round(sub_geom_alpha * 255);

      if (color_alpha == 0)
        return;

      int[] xpoints = new int[path.getPointCount()];
      int[] ypoints = new int[path.getPointCount()];
      
      for (int i = 0; i < path.getPointCount(); i++) {
        double px = path.getPoint(i).getX();
        double py = path.getPoint(i).getY();
        
        // Transform a point in the polygon to image coordinates
        xpoints[i] = (int) Math.round((px - fileMBR.x1) * imageWidth / fileMBR.getWidth());
        ypoints[i] = (int) Math.round((py - fileMBR.y1) * imageHeight / fileMBR.getHeight());
      }
      
      // Draw the polygon
      g.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
      if (path instanceof Polygon)
        g.drawPolygon(xpoints, ypoints, path.getPointCount());
      else if (path instanceof Polyline)
        g.drawPolyline(xpoints, ypoints, path.getPointCount());
    }
  }
  
  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    drawESRIGeom(g, geom, xscale, yscale);
  }
  
  public void drawESRIGeom(Graphics g, OGCGeometry geom, double xscale, double yscale) {
    if (geom instanceof OGCLineString) {
      OGCLineString linestring = (OGCLineString) geom;
      int[] xs = new int[linestring.numPoints()];
      int[] ys = new int[linestring.numPoints()];
      int n = 0;
      for (int i = 0; i < n; i++) {
        OGCPoint point = linestring.pointN(i);
        xs[n] = (int) Math.round(point.X() * xscale);
        ys[n] = (int) Math.round(point.Y() * yscale);
        // Increment number of point if this point is different than previous ones
        if (n == 0 || xs[n] != xs[n-1] || ys[n] != ys[n-1])
          n++;
      }
      g.drawPolyline(xs, ys, n);
    } else {
      throw new RuntimeException("Cannot draw a shape of type "+geom.getClass());
    }
  }
}
