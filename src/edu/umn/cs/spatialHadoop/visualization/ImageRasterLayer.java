/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.imageio.ImageIO;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * A raster layer that contains an image
 * @author Ahmed Eldawy
 *
 */
public class ImageRasterLayer extends RasterLayer {

  /**
   * The underlying image
   */
  protected BufferedImage image;

  /**
   * The graphics associated with the image if the image is in draw mode.
   * If the image is not in draw mode, graphics will be null.
   */
  protected Graphics2D graphics;

  /**The MBR of the input area associated with this image*/
  protected Rectangle mbr;
  
  /**The scale of the image on the x-axis in terms of pixels per input units*/
  protected double xscale;

  /**The scale of the image on the y-axis in terms of pixels per input units*/
  protected double yscale;

  /**Default color to use with underlying graphics*/
  private Color color;

  public ImageRasterLayer() {}
  
  public ImageRasterLayer(int xOffset, int yOffset, int width, int height) {
    this.xOffset = xOffset;
    this.yOffset = yOffset;
    image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
  }
  
  /**
   * Sets the minimal bounding rectangle of the input area associated with this image
   * @param MBR
   */
  public void setMBR(Rectangle mbr) {
    this.mbr = mbr.clone();
  }
  
  public void setColor(Color color) {
    this.color = color;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageIO.write(this.image, "png", baos);
    baos.close();
    byte[] bytes = baos.toByteArray();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int length = in.readInt();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    this.image = ImageIO.read(new ByteArrayInputStream(bytes));
  }

  @Override
  public void mergeWith(RasterLayer another) {
    BufferedImage anotherImage = ((ImageRasterLayer)another).image;
    getOrCreateGrahics(false).drawImage(anotherImage, another.xOffset, another.yOffset, null);
  }

  @Override
  public BufferedImage asImage() {
    if (graphics != null) {
      graphics.dispose();
      graphics = null;
    }
    return image;
  }
  
  protected Graphics2D getOrCreateGrahics(boolean translate) {
    if (graphics == null) {
      // Create graphics for the first time
      graphics = image.createGraphics();
      if (translate) {
        // Calculate the scale of the image in terms of pixels per unit
        xscale = image.getWidth() / mbr.getWidth();
        yscale = image.getHeight() / mbr.getHeight();
        // Translate the graphics to adjust its origin with the input origin
        graphics.translate(-mbr.x1 * xscale, -mbr.y1 * yscale);
        graphics.setColor(color);
      }
    }
    return graphics;
  }
  
  public void drawShape(Shape shape) {
    Graphics2D g = getOrCreateGrahics(true);
    shape.draw(g, xscale, yscale);
  }

}
