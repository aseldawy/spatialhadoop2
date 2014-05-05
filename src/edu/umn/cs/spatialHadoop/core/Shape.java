/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
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
   */
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth, int imageHeight, double scale);
}
