/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.imageio.ImageIO;

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
  
  public ImageRasterLayer() {
  }
  
  public ImageRasterLayer(int width, int height) {
    image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
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

  /* (non-Javadoc)
   * @see edu.umn.cs.spatialHadoop.visualization.RasterLayer#mergeWith(edu.umn.cs.spatialHadoop.visualization.RasterLayer)
   */
  @Override
  public void mergeWith(RasterLayer another) {
    BufferedImage anotherImage = ((ImageRasterLayer)another).image;
    Graphics2D g = this.image.createGraphics();
    g.drawImage(anotherImage, another.x, another.y, null);
  }

  /* (non-Javadoc)
   * @see edu.umn.cs.spatialHadoop.visualization.RasterLayer#asImage()
   */
  @Override
  public BufferedImage asImage() {
    return image;
  }

}
