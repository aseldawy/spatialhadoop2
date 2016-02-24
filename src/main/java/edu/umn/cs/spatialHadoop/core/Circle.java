/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.awt.Graphics;
import java.awt.geom.Arc2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A class that represents a circle
 * @author eldawy
 *
 */
public class Circle extends Arc2D.Double implements Shape {

  /**
   * 
   */
  private static final long serialVersionUID = 9221371239739890276L;

  /**
   * Default constructor to allow construction then deserialization
   */
  public Circle() {
  }
  
  /**
   * Initializes a circle with zero radius
   * @param x
   * @param y
   */
  public Circle(double x, double y) {
    this(x, y, 0);
  }
  
  /**
   * Initializes a circle with a specific center and radius
   * @param x
   * @param y
   * @param r
   */
  public Circle(double x, double y, double r) {
    set(x, y, r);
  }
  
  /**
   * Updates the circle to the given center and radius
   * @param x
   * @param y
   * @param r
   */
  public void set(double x, double y, double r) {
    super.setArc(x - r, y - r, r + r, r + r, 0, 360, Arc2D.CHORD);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(getCenterX());
    out.writeDouble(getCenterY());
    out.writeDouble(getRadius());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    double x = in.readDouble();
    double y = in.readDouble();
    double r = in.readDouble();
    set(x, y, r);
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeDouble(getCenterX(), text, ',');
    TextSerializerHelper.serializeDouble(getCenterY(), text, ',');
    TextSerializerHelper.serializeDouble(getRadius(), text, '\0');
    return text;
  }

  @Override
  public void fromText(Text text) {
    double x = TextSerializerHelper.consumeDouble(text, ',');
    double y = TextSerializerHelper.consumeDouble(text, ',');
    double r = TextSerializerHelper.consumeDouble(text, '\0');
    set(x, y, r);
  }
  
  private double getRadius() {
    return getWidth() / 2.0;
  }
  
  @Override
  public String toString() {
    return "Circle: @("+getCenterX()+","+getCenterY()+")- radius "+getRadius();
  }

  @Override
  public Rectangle getMBR() {
    return new Rectangle(getMinX(), getMinY(),
        getMaxX(), getMaxY());
  }

  @Override
  public double distanceTo(double x, double y) {
    double dx = getCenterX() - x;
    double dy = getCenterY() - y;
    return Math.sqrt(dx * dx + dy * dy);
  }

  @Override
  public boolean isIntersected(Shape s) {
    Rectangle mbr = s.getMBR();
    return super.intersects(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
  }
  
  public Circle clone() {
    return new Circle(getCenterX(), getCenterY(), getWidth() / 2);
  }
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    throw new RuntimeException("Not implemented yet");
  }
  
  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    throw new RuntimeException("Not implemented yet");
  }
}
