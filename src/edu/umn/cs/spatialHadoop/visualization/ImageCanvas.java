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
import java.awt.Graphics2D;
import java.awt.Point;
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
 * A canvas that contains an in-memory image
 * @author Ahmed Eldawy
 *
 */
public class ImageCanvas extends Canvas {

  /**
   * The underlying image
   */
  protected BufferedImage image;

  /**
   * The graphics associated with the image if the image is in draw mode.
   * If the image is not in draw mode, graphics will be null.
   */
  protected Graphics2D graphics;

  /**The scale of the image on the x-axis in terms of pixels per input units*/
  protected double xscale;

  /**The scale of the image on the y-axis in terms of pixels per input units*/
  protected double yscale;

  /**Default color to use with underlying graphics*/
  private Color color;

  /**Default constructor is necessary to be able to deserialize it*/
  public ImageCanvas() {
    System.setProperty("java.awt.headless", "true");
  }

  /**
   * Creates a canvas of the given size for a given (portion of) input
   * data.
   * @param inputMBR - the MBR of the input area to plot
   * @param width - width the of the image to generate in pixels
   * @param height - height of the image to generate in pixels
   */
  public ImageCanvas(Rectangle inputMBR, int width, int height) {
    super(inputMBR, width, height);
    System.setProperty("java.awt.headless", "true");
    image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    // Calculate the scale of the image in terms of pixels per unit
    xscale = image.getWidth() / getInputMBR().getWidth();
    yscale = image.getHeight() / getInputMBR().getHeight();
  }
  
  public void setColor(Color color) {
    this.color = color;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageIO.write(getImage(), "png", baos);
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
    // Calculate the scale of the image in terms of pixels per unit
    xscale = image.getWidth() / getInputMBR().getWidth();
    yscale = image.getHeight() / getInputMBR().getHeight();
  }

  public void mergeWith(ImageCanvas another) {
    Point offset = projectToImageSpace(another.getInputMBR().x1, another.getInputMBR().y1);
    getOrCreateGrahics(false).drawImage(another.getImage(), offset.x, offset.y, null);
  }

  public BufferedImage getImage() {
    if (graphics != null) {
      graphics.dispose();
      graphics = null;
    }
    return image;
  }
  
  protected Graphics2D getOrCreateGrahics(boolean translate) {
    if (graphics == null) {
      // Create graphics for the first time
      try {
        graphics = image.createGraphics();
      } catch (Throwable e) {
        graphics = new SimpleGraphics(image);
      }
      if (translate) {
        // Translate the graphics to adjust its origin with the input origin
        graphics.translate((int)(-getInputMBR().x1 * xscale), (int)(-getInputMBR().y1 * yscale));
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
