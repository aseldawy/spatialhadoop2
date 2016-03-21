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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

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
public class OGCJTSShape implements Shape {
  
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(OGCJTSShape.class);
  
  private final WKTReader wktReader = new WKTReader();
  private final WKBWriter wkbWriter = new WKBWriter();
  private final WKBReader wkbReader = new WKBReader();
  
  /**
   * The underlying geometry
   */
  public Geometry geom;
  
  public OGCJTSShape() {
    this(null);
  }
  
  public OGCJTSShape(Geometry geom) {
    this.geom = geom;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] wkb = wkbWriter.write(geom);
    out.writeInt(wkb.length);
    out.write(wkb);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      byte[] wkb = new byte[in.readInt()];
      in.readFully(wkb);
      geom = wkbReader.read(wkb);
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeGeometry(text, geom, '\0');
    return text;
  }
  
  public Geometry parseText(String str) throws ParseException {
    Geometry geom = null;
    try {
      // Parse string as well known text (WKT)
      geom = wktReader.read(str);
    } catch (ParseException e) {
      try {
        // Error parsing from WKT, try hex string instead
        byte[] binary = WKBReader.hexToBytes(str);
        geom = wkbReader.read(binary);
      } catch (RuntimeException e1) {
        // Cannot parse text. Just return null
      }
    }
    return geom;
  }


  @Override
  public void fromText(Text text) {
    this.geom = TextSerializerHelper.consumeGeometryJTS(text, '\0');
  }

  @Override
  public Rectangle getMBR() {
    if (geom == null)
      return null;
    Coordinate[] coords = geom.getEnvelope().getCoordinates();
    if (coords.length == 0)
      return null;
    double xmin, ymin, xmax, ymax;
    if (coords.length == 1) {
      // A point
      xmin = coords[0].x;
      ymin = coords[0].y;
      xmax = xmin + Math.ulp(xmin);
      ymax = ymin + Math.ulp(ymin);
    } else if (coords.length == 2) {
      // An orthogonal line
      if (coords[0].x == coords[1].x) {
        // A vertical line
        xmin = coords[0].x;
        xmax = xmin + Math.ulp(xmin);
        ymin = Math.min(coords[0].y, coords[1].y);
        ymax = Math.max(coords[0].y, coords[1].y);
      } else {
        // A horizontal line
        xmin = Math.min(coords[0].x, coords[1].x);
        xmax = Math.max(coords[0].x, coords[1].x);
        ymin = coords[0].y;
        ymax = ymin + Math.ulp(ymin);
      }
    } else if (coords.length == 4 || coords.length == 5) {
      // A rectangle
      xmin = Math.min(coords[0].x, coords[2].x);
      ymin = Math.min(coords[0].y, coords[2].y);
      xmax = Math.max(coords[0].x, coords[2].x);
      ymax = Math.max(coords[0].y, coords[2].y);
    } else {
      throw new RuntimeException("Cannot get MBR of "+geom);
    }
    
    return new Rectangle(xmin, ymin, xmax, ymax);
  }

  @Override
  public double distanceTo(double x, double y) {
    return this.geom.distance(geom.getFactory().createPoint(new Coordinate(x, y)));
  }

  @Override
  public boolean isIntersected(Shape s) {
    if (this.geom == null)
      return false;
    if (s instanceof OGCJTSShape) {
      return geom.intersects(((OGCJTSShape)s).geom);
    }
    Rectangle mbr = s.getMBR();
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(mbr.x1, mbr.y1);
    coordinates[1] = new Coordinate(mbr.x1, mbr.y2);
    coordinates[2] = new Coordinate(mbr.x2, mbr.y2);
    coordinates[3] = new Coordinate(mbr.x2, mbr.y1);
    coordinates[4] = coordinates[0];
    Polygon mbrPoly = geom.getFactory().createPolygon(geom.getFactory().createLinearRing(coordinates), null);
    
    return geom.intersects(mbrPoly);
  }

  @Override
  public Shape clone() {
    OGCJTSShape copy = new OGCJTSShape(this.geom);
    return copy;
  }
  
  @Override
  public String toString() {
    return geom == null? "(empty)" : geom.toString();
  }
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    Geometry geom = this.geom;
    Color shape_color = g.getColor();
    
    drawJTSShape(g, geom, fileMBR, imageWidth, imageHeight, scale, shape_color);
  }
  
  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    drawJTSGeom(g, geom, xscale, yscale, false);
  }

  /**
   * Draw the given JTS Geometry to the graphics using specific scales in x and y
   * @param g Graphics to draw to
   * @param geom The geometry to draw
   * @param xscale The scale of the x-axis in terms in pixels/units
   * @param yscale The scale of the y-axis in terms of pixels/units
   * @param fill Whether to fill the shape or just draw an outline
   */
  public static void drawJTSGeom(Graphics g, Geometry geom, double xscale,
      double yscale, boolean fill) {
    if (geom instanceof GeometryCollection) {
      GeometryCollection geom_coll = (GeometryCollection) geom;
      for (int i = 0; i < geom_coll.getNumGeometries(); i++) {
        Geometry sub_geom = geom_coll.getGeometryN(i);
        // Recursive call to draw each geometry
        drawJTSGeom(g, sub_geom, xscale, yscale, fill);
      }
    } else if (geom instanceof com.vividsolutions.jts.geom.Polygon) {
      com.vividsolutions.jts.geom.Polygon poly = (com.vividsolutions.jts.geom.Polygon) geom;

      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        LineString ring = poly.getInteriorRingN(i);
        drawJTSGeom(g, ring, xscale, yscale, fill);
      }
      
      drawJTSGeom(g, poly.getExteriorRing(), xscale, yscale, fill);
    } else if (geom instanceof LineString) {
      LineString line = (LineString) geom;
      double geom_alpha = line.getLength() * (xscale + yscale) / 2.0;
      int color_alpha = geom_alpha > 1.0 ? 255 : (int) Math.round(geom_alpha * 255);
      if (color_alpha == 0)
        return;
      
      int[] xpoints = new int[line.getNumPoints()];
      int[] ypoints = new int[line.getNumPoints()];
      int n = 0;

      for (int i = 0; i < xpoints.length; i++) {
        double px = line.getPointN(i).getX();
        double py = line.getPointN(i).getY();
        
        // Transform a point in the polygon to image coordinates
        xpoints[n] = (int) Math.round(px * xscale);
        ypoints[n] = (int) Math.round(py * yscale);
        // Include this point only if first point or different than previous point
        if (n == 0 || xpoints[n] != xpoints[n-1] || ypoints[n] != ypoints[n-1])
          n++;
      }
      
      // Draw the polygon
      //graphics.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
      if (n == 1)
        g.fillRect(xpoints[0], ypoints[0], 1, 1);
      else if (!fill)
        g.drawPolyline(xpoints, ypoints, n);
      else
        g.fillPolygon(xpoints, ypoints, n);
    }
  }

  /**
   * Plots a Geometry from the library JTS into the given image.
   * @param graphics
   * @param geom
   * @param fileMbr
   * @param imageWidth
   * @param imageHeight
   * @param scale
   * @param shape_color
   * @deprecated - use {@link #drawJTSGeom(Graphics, Geometry, double, double)}
   */
  @Deprecated
  private static void drawJTSShape(Graphics graphics, Geometry geom,
      Rectangle fileMbr, int imageWidth, int imageHeight, double scale,
      Color shape_color) {
    if (geom instanceof GeometryCollection) {
      GeometryCollection geom_coll = (GeometryCollection) geom;
      for (int i = 0; i < geom_coll.getNumGeometries(); i++) {
        Geometry sub_geom = geom_coll.getGeometryN(i);
        // Recursive call to draw each geometry
        drawJTSShape(graphics, sub_geom, fileMbr, imageWidth, imageHeight, scale, shape_color);
      }
    } else if (geom instanceof com.vividsolutions.jts.geom.Polygon) {
      com.vividsolutions.jts.geom.Polygon poly = (com.vividsolutions.jts.geom.Polygon) geom;

      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        LineString ring = poly.getInteriorRingN(i);
        drawJTSShape(graphics, ring, fileMbr, imageWidth, imageHeight, scale, shape_color);
      }
      
      drawJTSShape(graphics, poly.getExteriorRing(), fileMbr, imageWidth, imageHeight, scale, shape_color);
    } else if (geom instanceof LineString) {
      LineString line = (LineString) geom;
      double geom_alpha = line.getLength() * scale;
      int color_alpha = geom_alpha > 1.0 ? 255 : (int) Math.round(geom_alpha * 255);
      if (color_alpha == 0)
        return;
      
      int[] xpoints = new int[line.getNumPoints()];
      int[] ypoints = new int[line.getNumPoints()];

      for (int i = 0; i < xpoints.length; i++) {
        double px = line.getPointN(i).getX();
        double py = line.getPointN(i).getY();
        
        // Transform a point in the polygon to image coordinates
        xpoints[i] = (int) Math.round((px - fileMbr.x1) * imageWidth / fileMbr.getWidth());
        ypoints[i] = (int) Math.round((py - fileMbr.y1) * imageHeight / fileMbr.getHeight());
      }
      
      // Draw the polygon
      //graphics.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
      graphics.drawPolyline(xpoints, ypoints, xpoints.length);
    }
  }
  
}
