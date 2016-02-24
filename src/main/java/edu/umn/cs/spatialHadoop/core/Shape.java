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

import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.io.TextSerializable;

/**
 * A general 2D shape.
 * @author aseldawy
 *
 */
public interface Shape extends Writable, Cloneable, TextSerializable {
  /**
   * Returns minimum bounding rectangle for this shape.
   * @return
   */
  public Rectangle getMBR();
  
  /**
   * Gets the distance of this shape to the given point.
   * @param x
   * @param y
   * @return
   */
  public double distanceTo(double x, double y);
  
  /**
   * Returns true if this shape is intersected with the given shape
   * @param s
   * @return
   */
  public boolean isIntersected(final Shape s);
  
  /**
   * Returns a clone of this shape
   * @return
   * @throws CloneNotSupportedException
   */
  public Shape clone();
  
  /**
   * Draws a shape to the given graphics.
   * @param g - the graphics or canvas to draw to
   * @param fileMBR - the MBR of the file in which the shape is contained
   * @param imageWidth - width of the image to draw
   * @param imageHeight - height of the image to draw
   * @param scale - the scale used to convert shape coordinates to image coordinates
   * @deprecated - see 
   */
  @Deprecated
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth, int imageHeight, double scale);
  
  /**
   * Draws the shape to the given graphics and scale.
   * @param g - the graphics to draw the shape to.
   * @param xscale - scale of the image x-axis in terms of pixels per points.
   * @param yscale - scale of the image y-axis in terms of pixels per points.
   */
  public void draw(Graphics g, double xscale, double yscale);
}
