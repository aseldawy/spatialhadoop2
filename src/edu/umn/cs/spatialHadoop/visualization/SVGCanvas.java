/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Point;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.FloatArray;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * A canvas that contains a set of vector data.
 * Although this layer contains vector data, it keeps into consideration
 * that this data is drawn at a specific raster resolution. It automatically
 * simplifies objects drawn on it to match the configured resolution.
 * In addition, it automatically removes very small objects that are much
 * smaller than a pixel and removes some records if they are hidden behind
 * other records.
 * @author Ahmed Eldawy
 *
 */
public class SVGCanvas extends Canvas {

  /**The scale of the image on the x-axis in terms of pixels per input units*/
  protected double xscale;

  /**The scale of the image on the y-axis in terms of pixels per input units*/
  protected double yscale;

  /**All coordinates used in the file are stored here*/
  protected FloatArray xs, ys;

  /**The point start index and number of points in each polygon*/
  protected IntArray polygonsStart, polygonsSize;

  /**Translation of the origin*/
  protected float ox, oy;

  /**Default constructor is necessary to be able to deserialize it*/
  public SVGCanvas() {
  	xs = new FloatArray();
  	ys = new FloatArray();
  	polygonsStart = new IntArray();
  	polygonsSize = new IntArray();
  }

  /**
   * Creates a canvas of the given size for a given (portion of) input
   * data.
   * @param inputMBR - the MBR of the input area to plot
   * @param width - width the of the image to generate in pixels
   * @param height - height of the image to generate in pixels
   */
  public SVGCanvas(Rectangle inputMBR, int width, int height) {
    super(inputMBR, width, height);
    this.xscale = width / getInputMBR().getWidth();
    this.yscale = height / getInputMBR().getHeight();
    this.width = width;
    this.height = height;
    this.ox = (float) (-getInputMBR().x1 * xscale);
    this.oy = (float) (-getInputMBR().y1 * yscale);
    
    this.xs = new FloatArray();
    this.ys = new FloatArray();
    this.polygonsStart = new IntArray();
    this.polygonsSize = new IntArray();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    xs.write(out);
    ys.write(out);
    polygonsStart.write(out);
    polygonsSize.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    xs.readFields(in);
    ys.readFields(in);
    polygonsStart.readFields(in);
    polygonsSize.readFields(in);
  }
  
  /**
   * Draws a JTS geometry
   * @param geom
   */
  public void drawShape(Geometry geom) {
    if (geom instanceof GeometryCollection) {
      GeometryCollection geom_coll = (GeometryCollection) geom;
      for (int i = 0; i < geom_coll.getNumGeometries(); i++) {
        Geometry sub_geom = geom_coll.getGeometryN(i);
        // Recursive call to draw each geometry
        drawShape(sub_geom);
      }
    } else if (geom instanceof com.vividsolutions.jts.geom.Polygon) {
      com.vividsolutions.jts.geom.Polygon poly = (com.vividsolutions.jts.geom.Polygon) geom;

      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        LineString ring = poly.getInteriorRingN(i);
        drawShape(ring);
      }

      drawShape(poly.getExteriorRing());
    } else if (geom instanceof LineString) {
      LineString line = (LineString) geom;
      double geom_alpha = line.getLength() * (xscale + yscale) / 2.0;
      int color_alpha = geom_alpha > 1.0 ? 255 : (int) Math.round(geom_alpha * 255);
      if (color_alpha == 0)
        return;

      float[] xpoints = new float[line.getNumPoints()];
      float[] ypoints = new float[line.getNumPoints()];

      for (int i = 0; i < xpoints.length; i++) {
        float px = (float) line.getPointN(i).getX();
        float py = (float) line.getPointN(i).getY();

        // Transform a point in the polygon to image coordinates
        xpoints[i] = (float) (px * xscale);
        ypoints[i] = (float) (py * yscale);
      }

      // Draw the polygon
      //graphics.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
      polygonsStart.append(xs.size());
      polygonsSize.append(xpoints.length);
      xs.append(xpoints, 0, xpoints.length, ox);
      ys.append(ypoints, 0, ypoints.length, oy);
    }
  }

  public void mergeWith(SVGCanvas intermediateLayer) {
    Point offset = projectToImageSpace(intermediateLayer.getInputMBR().x1,
        intermediateLayer.getInputMBR().y1);
    this.xs.append(intermediateLayer.xs, offset.x);
    this.ys.append(intermediateLayer.ys, offset.y);
    this.polygonsStart.append(intermediateLayer.polygonsStart, this.polygonsStart.size());
    this.polygonsSize.append(intermediateLayer.polygonsSize);
  }

  public void writeToFile(PrintStream p) {
    // Write as SVG XML format

    // Write header
    p.println("<?xml version='1.0' standalone='no'?>");
    p.println("<!DOCTYPE svg PUBLIC '-//W3C/DTD SVG 1.1//EN' "
        + "'http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd'>");
    p.printf("<svg width='%d' height='%d' version='1.1' "
        + "xmlns='http://www.w3.org/2000/svg'>\n", width, height);

    // Retrieve all xs and ys as java arrays for efficiency
    float[] xs = this.xs.underlyingArray();
    float[] ys = this.ys.underlyingArray();
    
    // Draw all polygons
    if (polygonsStart.size() > 0) {
      p.printf("<g style='stroke:rgb(0,0,0);'>\n");
      for (int i = 0; i < polygonsStart.size(); i++) {
        int polygonStart = polygonsStart.get(i);
        int polygonSize = polygonsSize.get(i);
        p.print("<polygon points='");
        for (int j = polygonStart; j < polygonStart + polygonSize; j++) {
          p.printf("%f,%f ", xs[j], ys[j]);
        }
        p.println("'/>");
      }
      p.printf("</g>\n");
    }
    p.println("</svg>");
  }

}
