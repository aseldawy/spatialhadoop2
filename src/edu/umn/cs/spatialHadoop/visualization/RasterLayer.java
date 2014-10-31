/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**An abstract interface for any raster layer*/
public abstract class RasterLayer implements Writable {
  /**The x coordinate of that layer in the final image*/
  protected int xOffset;
  
  /**The y coordinate of that layer in the final image*/
  protected int yOffset;
  
  /**Updates the current raster layer by merging it with
   * another raster layer*/
  public abstract void mergeWith(RasterLayer another);
  
  /**Convert the current raster layer to an image*/
  public abstract BufferedImage asImage();
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(xOffset);
    out.writeInt(yOffset);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.xOffset = in.readInt();
    this.yOffset = in.readInt();
  }
}