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

import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.core.Rectangle;

/**An abstract interface for any canvas*/
public abstract class Canvas implements Writable {
  /**The MBR of the this layer in input coordinates*/
  protected Rectangle inputMBR;
  
  /**Width of this layer in pixels*/
  protected int width;
  
  /**Height of this layer in pixels*/
  protected int height;
  
  public Canvas() {}
  
  public Canvas(Rectangle inputMBR, int width, int height) {
    super();
    this.inputMBR = inputMBR;
    this.width = width;
    this.height = height;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    inputMBR.getMBR().write(out);
    out.writeInt(width);
    out.writeInt(height);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    if (inputMBR == null)
      inputMBR = new Rectangle();
    inputMBR.readFields(in);
    width = in.readInt();
    height = in.readInt();
  }

  public Rectangle getInputMBR() {
    return inputMBR;
  }

  public int getWidth() {
    return width;
  }
  
  public int getHeight() {
    return height;
  }
  
  /**
   * Project a point from input space to image space.
   * @param x
   * @param y
   * @return
   */
  public Point projectToImageSpace(double x, double y) {
    // Calculate the offset of the intermediate layer in the final canvas based on its MBR
    Rectangle finalMBR = this.getInputMBR();
    int imageX = (int) Math.floor((x - finalMBR.x1) * this.getWidth() / finalMBR.getWidth());
    int imageY = (int) Math.floor((y - finalMBR.y1) * this.getHeight() / finalMBR.getHeight());
    return new Point(imageX, imageY);
  }
}