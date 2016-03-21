/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
import java.awt.Composite;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GraphicsConfiguration;
import java.awt.Image;
import java.awt.Paint;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.RenderingHints.Key;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.font.FontRenderContext;
import java.awt.font.GlyphVector;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ImageObserver;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.RenderableImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.text.AttributedCharacterIterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * A simple graphics class that draws directly on an a BufferedImage and does
 * not require an active X11 display.
 * @author eldawy
 *
 */
public class SVGGraphics extends Graphics2D implements Writable {

  /**Size of the underlying SVG file in pixels*/
  protected int width, height;
  /**Translation amount*/
  protected int tx, ty;
  /**All coordinates used in the file are stored here*/
  protected IntArray xs, ys;
  
  // I use lots of primitive arrays as opposed to vectors for memory efficiency
  /**The start index of each line*/
  protected IntArray linesStart;
  
  /**The start index and size (number of points) of each polyline*/
  protected IntArray polylinesStart, polylinesSize;
  
  protected IntArray polygonsStart, polygonsSize;
  
  /**The start position of each rectangle*/
  protected IntArray rectanglesStart;
  
  public SVGGraphics(int width, int height) {
    this.width = width;
    this.height = height;
    xs = new IntArray();
    ys = new IntArray();
    linesStart = new IntArray();
    polylinesStart = new IntArray();
    polylinesSize = new IntArray();
    polygonsStart = new IntArray();
    polygonsSize = new IntArray();
    rectanglesStart = new IntArray();
  }

  /**
   * Default constructor to be used with {@link Writable#readFields(java.io.DataInput)}
   */
  public SVGGraphics() {
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(width);
    out.writeInt(height);
    xs.write(out);
    ys.write(out);
    linesStart.write(out);
    polylinesStart.write(out);
    polylinesSize.write(out);
    polygonsStart.write(out);
    polygonsSize.write(out);
    rectanglesStart.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    width = in.readInt();
    height = in.readInt();
    xs.readFields(in);
    ys.readFields(in);
    linesStart.readFields(in);
    polylinesStart.readFields(in);
    polylinesSize.readFields(in);
    polygonsStart.readFields(in);
    polygonsSize.readFields(in);
    rectanglesStart.readFields(in);
  }

  @Override
  public void draw(Shape s) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, AffineTransform xform, ImageObserver obs) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawImage(BufferedImage img, BufferedImageOp op, int x, int y) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawRenderedImage(RenderedImage img, AffineTransform xform) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawRenderableImage(RenderableImage img, AffineTransform xform) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawString(String str, int x, int y) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawString(String str, float x, float y) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawString(AttributedCharacterIterator iterator, int x, int y) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawString(AttributedCharacterIterator iterator, float x, float y) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawGlyphVector(GlyphVector g, float x, float y) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void fill(Shape s) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean hit(Rectangle rect, Shape s, boolean onStroke) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GraphicsConfiguration getDeviceConfiguration() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setComposite(Composite comp) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setPaint(Paint paint) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setStroke(Stroke s) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setRenderingHint(Key hintKey, Object hintValue) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Object getRenderingHint(Key hintKey) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setRenderingHints(Map<?, ?> hints) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void addRenderingHints(Map<?, ?> hints) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public RenderingHints getRenderingHints() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void translate(int tx, int ty) {
    this.tx += tx;
    this.ty += ty;
  }

  @Override
  public void translate(double tx, double ty) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void rotate(double theta) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void rotate(double theta, double x, double y) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void scale(double sx, double sy) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void shear(double shx, double shy) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void transform(AffineTransform Tx) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setTransform(AffineTransform Tx) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public AffineTransform getTransform() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Paint getPaint() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Composite getComposite() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setBackground(Color color) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Color getBackground() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Stroke getStroke() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void clip(Shape s) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public FontRenderContext getFontRenderContext() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Graphics create() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Color getColor() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setColor(Color c) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setPaintMode() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setXORMode(Color c1) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Font getFont() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setFont(Font font) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public FontMetrics getFontMetrics(Font f) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Rectangle getClipBounds() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void clipRect(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setClip(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Shape getClip() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void setClip(Shape clip) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void copyArea(int x, int y, int width, int height, int dx, int dy) {
    throw new RuntimeException("Not implemented");
  }
  
  protected void setPixel(int x, int y, int rgb) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawLine(int x1, int y1, int x2, int y2) {
    linesStart.append(xs.size());
    xs.append(x1 + tx);
    xs.append(x2 + tx);
    ys.append(y1 + ty);
    ys.append(y2 + ty);
  }

  @Override
  public void fillRect(int x, int y, int width, int height) {
    rectanglesStart.append(xs.size());
    xs.append(x + tx);
    ys.append(y + ty);
    xs.append(width);
    ys.append(height);
  }

  @Override
  public void clearRect(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
  }
  
  @Override
  public void drawRoundRect(int x, int y, int width, int height, int arcWidth,
      int arcHeight) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void fillRoundRect(int x, int y, int width, int height, int arcWidth,
      int arcHeight) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawOval(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void fillOval(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawArc(int x, int y, int width, int height, int startAngle,
      int arcAngle) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void fillArc(int x, int y, int width, int height, int startAngle,
      int arcAngle) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void drawPolyline(int[] xPoints, int[] yPoints, int nPoints) {
    polylinesStart.append(xs.size());
    polylinesSize.append(nPoints);
    xs.append(xPoints, 0, nPoints, tx);
    ys.append(yPoints, 0, nPoints, ty);
  }

  @Override
  public void drawPolygon(int[] xPoints, int[] yPoints, int nPoints) {
    polygonsStart.append(xs.size());
    polygonsSize.append(nPoints);
    xs.append(xPoints, 0, nPoints, tx);
    ys.append(yPoints, 0, nPoints, ty);
  }

  @Override
  public void fillPolygon(int[] xPoints, int[] yPoints, int nPoints) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int x, int y, ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int x, int y, int width, int height,
      ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int x, int y, Color bgcolor,
      ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int x, int y, int width, int height,
      Color bgcolor, ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int dx1, int dy1, int dx2, int dy2,
      int sx1, int sy1, int sx2, int sy2, ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int dx1, int dy1, int dx2, int dy2,
      int sx1, int sy1, int sx2, int sy2, Color bgcolor, ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void dispose() {
  }
  
  /**
   * Merge all objects in the given svgGraphics with this one
   * @param other
   */
  public void mergeWith(SVGGraphics other, int tx, int ty) {
    // 1- Merge points
    int pointsShift = this.xs.size();
    this.xs.append(other.xs, tx);
    this.ys.append(other.ys, ty);
    
    // 2- Merge lines
    this.linesStart.append(other.linesStart, pointsShift);

    // 3- Merge polylines
    this.polylinesStart.append(other.polylinesStart, pointsShift);
    this.polylinesSize.append(other.polylinesSize);
    
    // 4- Merge polygons
    this.polygonsStart.append(other.polygonsStart, pointsShift);
    this.polygonsSize.append(other.polygonsSize);
    
    // 5- Merge rectangles
    this.rectanglesStart.append(other.rectanglesStart, pointsShift);
  }

  
  public void writeAsSVG(PrintStream p) {
    // Write header
    p.println("<?xml version='1.0' standalone='no'?>");
    p.println("<!DOCTYPE svg PUBLIC '-//W3C/DTD SVG 1.1//EN' "
        + "'http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd'>");
    p.printf("<svg width='%d' height='%d' version='1.1' "
        + "xmlns='http://www.w3.org/2000/svg'>\n", width, height);

    // Retrieve all xs and ys as java arrays for efficiency
    int[] xs = this.xs.underlyingArray();
    int[] ys = this.ys.underlyingArray();
    // 1- Draw all lines
    if (!linesStart.isEmpty()) {
      p.printf("<g style='stroke:rgb(0,0,0);'>\n");
      for (int i = 0; i < linesStart.size(); i++) {
        int lineStart = linesStart.get(i);
        p.printf("<line x1='%d' y1='%d' x2='%d' y2='%d'/>\n",
            xs[lineStart], ys[lineStart], xs[lineStart+1], ys[lineStart+1]);
      }
      p.printf("</g>\n");
    }
    
    // 2- Draw all polygons
    if (!polygonsStart.isEmpty()) {
      p.printf("<g style='stroke:rgb(0,0,0);'>\n");
      for (int i = 0; i < polygonsStart.size(); i++) {
        int polygonStart = polygonsStart.get(i);
        int polygonSize = polygonsSize.get(i);
        p.print("<polygon points='");
        for (int j = polygonStart; j < polygonStart + polygonSize; j++) {
          p.printf("%d,%d ", xs[j], ys[j]);
        }
        p.println("'/>");
      }
      p.printf("</g>\n");
    }
    
    // 3- Draw all polylines
    if (!polylinesStart.isEmpty()) {
      p.printf("<g style='stroke:rgb(0,0,0); fill:none;'>\n");
      for (int i = 0; i < polylinesStart.size(); i++) {
        int polylineStart = polylinesStart.get(i);
        int polylineSize = polylinesSize.get(i);
        p.print("<polyline points='");
        for (int j = polylineStart; j < polylineStart + polylineSize; j++) {
          p.printf("%d,%d ", xs[j], ys[j]);
        }
        p.println("'/>");
      }
      p.printf("</g>\n");
      
    }
    // 4- Draw all rectangles
    if (!rectanglesStart.isEmpty()) {
      p.printf("<g style='stroke:rgb(0,0,0); fill:none;'>\n");
      for (int i = 0; i < rectanglesStart.size(); i++) {
        int rectStart = rectanglesStart.get(i);
        p.printf("<rect x='%d' y='%d' width='%d' height='%d'/>\n",
            xs[rectStart], ys[rectStart], xs[rectStart+1], ys[rectStart+1]);
      }
      p.printf("</g>\n");
    }
    
    p.println("</svg>");
  }
    
}
