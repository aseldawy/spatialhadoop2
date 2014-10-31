/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A frequency map that can be used to visualize data as heat maps
 * @author Ahmed Eldawy
 *
 */
public class FrequencyMapRasterLayer extends RasterLayer {
  private static final Log LOG = LogFactory.getLog(FrequencyMapRasterLayer.class);
  
  enum KernelType {Flat, Gaussian};
  
  /**The kernel to use for stamping points*/
  protected float[][] kernel;
  
  /**Frequencies*/
  protected float[][] frequencies;

  /**Radius to smooth nearboy points*/
  private int radius;
  
  /**
   * Initialize an empty frequency map to be used to deserialize 
   */
  public FrequencyMapRasterLayer() { }

  /**
   * Initializes a frequency map with the given dimensions
   * @param width
   * @param height
   */
  public FrequencyMapRasterLayer(int width, int height) {
    this.frequencies = new float[width][height];
  }
  
  /**
   * Initialize a frequency map with the given radius and kernel type
   * @param radius
   * @param kernelType
   */
  public FrequencyMapRasterLayer(int radius, KernelType kernelType) {
    this.radius = radius;
    // initialize the kernel according to the radius and kernel type
    kernel = new float[radius * 2][radius * 2];
    switch (kernelType) {
    case Flat:
      for (int dx = -radius; dx < radius; dx++) {
        for (int dy = -radius; dy < radius; dy++) {
          if (dx * dx + dy * dy < radius * radius) {
            kernel[dx + radius][dy + radius] = 1.0f;
          }
        }
      }
      break;
    case Gaussian:
      int stdev = 8;
      // Apply two-dimensional Gaussian function
      // http://en.wikipedia.org/wiki/Gaussian_function#Two-dimensional_Gaussian_function
      for (int dx = -radius; dx < radius; dx++) {
        for (int dy = -radius; dy < radius; dy++) {
          kernel[dx + radius][dy + radius] = (float) Math.pow(Math.E, -(dx * dx + dy * dy)
              / (2 * stdev * stdev));
        }
      }
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzos = new GZIPOutputStream(baos);
    ByteBuffer bbuffer = ByteBuffer.allocate(getHeight() * 4 + 8);
    bbuffer.putInt(getWidth());
    bbuffer.putInt(getHeight());
    gzos.write(bbuffer.array(), 0, bbuffer.position());
    for (int x = 0; x < getWidth(); x++) {
      bbuffer.clear();
      for (int y = 0; y < getHeight(); y++) {
        bbuffer.putFloat(frequencies[x][y]);
      }
      gzos.write(bbuffer.array(), 0, bbuffer.position());
    }
    gzos.close();
    
    byte[] serializedData = baos.toByteArray();
    out.writeInt(serializedData.length);
    out.write(serializedData);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int length = in.readInt();
    byte[] serializedData = new byte[length];
    in.readFully(serializedData);
    ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
    GZIPInputStream gzis = new GZIPInputStream(bais);
    
    byte[] buffer = new byte[8];
    gzis.read(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    int width = bbuffer.getInt();
    int height = bbuffer.getInt();
    // Reallocate memory only if needed
    if (width != this.getWidth() || height != this.getHeight())
      frequencies = new float[width][height];
    buffer = new byte[getHeight() * 4];
    for (int x = 0; x < getWidth(); x++) {
      int size = 0;
      while (size < buffer.length) {
        size += gzis.read(buffer, size, buffer.length - size);
      }
      bbuffer = ByteBuffer.wrap(buffer);
      for (int y = 0; y < getHeight(); y++) {
        frequencies[x][y] = bbuffer.getFloat();
      }
    }

  }
  
  @Override
  public void mergeWith(RasterLayer another) {
    FrequencyMapRasterLayer anotherFM = (FrequencyMapRasterLayer) another;
    int xmin = Math.max(this.xOffset, anotherFM.xOffset);
    int ymin = Math.max(this.yOffset, anotherFM.yOffset);
    int xmax = Math.min(this.xOffset + this.getWidth(), anotherFM.xOffset + anotherFM.getWidth());
    int ymax = Math.min(this.yOffset + this.getHeight(), anotherFM.yOffset + anotherFM.getHeight());
    for (int x = xmin; x < xmax; x++) {
      for (int y = ymin; y < ymax; y++) {
        this.frequencies[x-xOffset][y-yOffset] +=
            anotherFM.frequencies[x - anotherFM.xOffset][y - anotherFM.yOffset];
      }
    }
  }
  
  @Override
  public BufferedImage asImage() {
    float min = Float.MAX_VALUE, max = -Float.MAX_VALUE;
    for (int x = 0; x < this.getWidth(); x++) {
      for (int y = 0; y < this.getHeight(); y++) {
        if (frequencies[x][y] < min)
          min = frequencies[x][y];
        if (frequencies[x][y] > max)
          max = frequencies[x][y];
      }
    }
    LOG.info("Using the value range: "+min+","+max);
    BufferedImage image = new BufferedImage(getWidth(), getHeight(), BufferedImage.TYPE_INT_ARGB);
    for (int x = 0; x < this.getWidth(); x++) {
      for (int y = 0; y < this.getHeight(); y++) {
        Color color = calculateColor(frequencies[x][y], min, max);
        image.setRGB(x, y, color.getRGB());
      }
    }
    return image;
  }

  /**
   * Adds a point to the frequency map
   * @param cx
   * @param cy
   */
  public void addPoint(int cx, int cy) {
    for (int dx = -radius; dx < radius; dx++) {
      for (int dy = -radius; dy < radius; dy++) {
        int imgx = cx + dx - xOffset;
        int imgy = dy + dy - yOffset;
        if (imgx >= 0 && imgx < getWidth() && imgy >= 0 && imgy < getHeight())
          frequencies[imgx][imgy] += kernel[dx + radius][dy + radius];
      }
    }
  }

  public int getWidth() {
    return frequencies == null? 0 : frequencies.length;
  }

  public int getHeight() {
    return frequencies == null? 0 : frequencies[0].length;
  }

  protected static Color color1, color2;
  protected static float hue1, hue2;
  protected static float saturation1, saturation2;
  protected static float brightness1, brightness2;
  
  public static enum GradientType {GT_HUE, GT_COLOR};
  public static GradientType gradientType;

  public static void setColor1(Color color) {
    color1 = color;
    float[] hsbvals = new float[3];
    Color.RGBtoHSB(color.getRed(), color.getGreen(), color.getBlue(), hsbvals);
    hue1 = hsbvals[0];
    saturation1 = hsbvals[1];
    brightness1 = hsbvals[2];
  }
  
  public static void setColor2(Color color) {
    color2 = color;
    float[] hsbvals = new float[3];
    Color.RGBtoHSB(color.getRed(), color.getGreen(), color.getBlue(), hsbvals);
    hue2 = hsbvals[0];
    saturation2 = hsbvals[1];
    brightness2 = hsbvals[2];
  }

  public static Color calculateColor(float value, float minValue, float maxValue) {
    Color color;
    if (value < minValue) {
      color = color1;
    } else if (value > maxValue) {
      color = color2;
    } else {
      // Interpolate between two colors according to gradient type
      float ratio = (value - minValue) / (maxValue - minValue);
      if (gradientType == GradientType.GT_HUE) {
        // Interpolate between two hues
        float hue = hue1 * (1.0f - ratio) + hue2 * ratio;
        float saturation = saturation1 * (1.0f - ratio) + saturation2 * ratio;
        float brightness = brightness1 * (1.0f - ratio) + brightness2 * ratio;
        color = Color.getHSBColor(hue, saturation, brightness);
        int alpha = (int) (color1.getAlpha() * (1.0f - ratio) + color2.getAlpha() * ratio);
        color = new Color(color.getRGB() & 0xffffff | (alpha << 24), true);
      } else if (gradientType == GradientType.GT_COLOR) {
        // Interpolate between colors
        int red = (int) (color1.getRed() * (1.0f - ratio) + color2.getRed() * ratio);
        int green = (int) (color1.getGreen() * (1.0f - ratio) + color2.getGreen() * ratio);
        int blue = (int) (color1.getBlue() * (1.0f - ratio) + color2.getBlue() * ratio);
        int alpha = (int) (color1.getAlpha() * (1.0f - ratio) + color2.getAlpha() * ratio);
        color = new Color(red, green, blue, alpha);
      } else {
        throw new RuntimeException("Unsupported gradient type: "+gradientType);
      }
    }
    return color;
  }

}
