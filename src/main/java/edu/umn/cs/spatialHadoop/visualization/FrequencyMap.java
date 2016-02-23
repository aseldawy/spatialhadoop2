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
import java.awt.Point;
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

import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * A frequency map that can be used to visualize data as heat maps
 * @author Ahmed Eldawy
 *
 */
public class FrequencyMap extends Canvas {
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(FrequencyMap.class);
  
  public static enum SmoothType {Flat, Gaussian};
  
  /**The kernel to use for stamping points*/
  protected float[][] kernel;
  
  /**Frequencies*/
  protected float[][] frequencies;

  /**Radius to smooth nearboy points*/
  private int radius;

  /**The minimum value to be used while drawing the heat map*/
  private float min;

  /**The maximum value to be used while drawing the heat map*/
  private float max;
  
  /**
   * Initialize an empty frequency map to be used to deserialize 
   */
  public FrequencyMap() {
    System.setProperty("java.awt.headless", "true");
  }

  /**
   * Initializes a frequency map with the given dimensions
   * @param width
   * @param height
   */
  public FrequencyMap(Rectangle inputMBR, int width, int height, int radius, SmoothType smoothType) {
    System.setProperty("java.awt.headless", "true");
    this.inputMBR = inputMBR;
    this.width = width;
    this.height = height;
    this.frequencies = new float[width][height];
    this.min = -1; this.max = -2;
    initKernel(radius, smoothType);
  }
  
  /**
   * Initialize a frequency map with the given radius and kernel type
   * @param radius
   * @param smoothType
   */
  protected void initKernel(int radius, SmoothType smoothType) {
    this.radius = radius;
    // initialize the kernel according to the radius and kernel type
    kernel = new float[radius * 2][radius * 2];
    switch (smoothType) {
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
          kernel[dx + radius][dy + radius] = (float) Math.exp(-(dx * dx + dy * dy)
              / (2.0 * stdev * stdev));
        }
      }
    }
  }
  
  /**
   * Sets the range of value to be used while drawing the heat map
   * @param min
   * @param max
   */
  public void setValueRange(float min, float max) {
    this.min = min;
    this.max = max;
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
  
  public void mergeWith(FrequencyMap another) {
    Point offset = projectToImageSpace(another.getInputMBR().x1, another.getInputMBR().y1);
    int xmin = Math.max(0, offset.x);
    int ymin = Math.max(0, offset.y);
    int xmax = Math.min(this.getWidth(), another.getWidth() + offset.x);
    int ymax = Math.min(this.getHeight(), another.getHeight() + offset.y);
    for (int x = xmin; x < xmax; x++) {
      for (int y = ymin; y < ymax; y++) {
        this.frequencies[x][y] +=
            another.frequencies[x - offset.x][y - offset.y];
      }
    }
  }
  
  public BufferedImage asImage() {
    if (min >= max) {
      // Values not set. Autodetect
      min = Float.MAX_VALUE;
      max = -Float.MAX_VALUE;
      for (int x = 0; x < this.getWidth(); x++) {
        for (int y = 0; y < this.getHeight(); y++) {
          if (frequencies[x][y] < min)
            min = frequencies[x][y];
          if (frequencies[x][y] > max)
            max = frequencies[x][y];
        }
      }
    }
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
        int imgx = cx + dx;
        int imgy = cy + dy;
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
  
  /* The following methods are used to compute the gradient */

  protected Color[] colors;
  protected float[] hues;
  protected float[] saturations;
  protected float[] brightnesses;
  
  public enum GradientType {GT_HSB, GT_RGB};
  protected GradientType gradientType;
  
  public void setGradientInfor(Color color1, Color color2, GradientType gradientType) {
    this.colors = new Color[] {color1, color2};
    this.hues = new float[colors.length];
    this.saturations = new float[colors.length];
    this.brightnesses = new float[colors.length];
    
    for (int i = 0; i < colors.length; i++) {
      float[] hsbvals = new float[3];
      Color.RGBtoHSB(colors[i].getRed(), colors[i].getGreen(), colors[i].getBlue(), hsbvals);
      hues[i] = hsbvals[0];
      saturations[i] = hsbvals[1];
      brightnesses[i] = hsbvals[2];
    }
    this.gradientType = gradientType;
  }

  protected Color calculateColor(float value, float minValue, float maxValue) {
    Color color;
    if (value < minValue) {
      color = colors[0];
    } else if (value > maxValue) {
      color = colors[1];
    } else {
      // Interpolate between two colors according to gradient type
      float ratio = (value - minValue) / (maxValue - minValue);
      if (gradientType == GradientType.GT_HSB) {
        // Interpolate between two hues
        float hue = hues[0] * (1.0f - ratio) + hues[1] * ratio;
        float saturation = saturations[0] * (1.0f - ratio) + saturations[1] * ratio;
        float brightness = brightnesses[0] * (1.0f - ratio) + brightnesses[1] * ratio;
        color = Color.getHSBColor(hue, saturation, brightness);
        int alpha = (int) (colors[0].getAlpha() * (1.0f - ratio) + colors[1].getAlpha() * ratio);
        color = new Color(color.getRGB() & 0xffffff | (alpha << 24), true);
      } else if (gradientType == GradientType.GT_RGB) {
        // Interpolate between colors
        int red = (int) (colors[0].getRed() * (1.0f - ratio) + colors[1].getRed() * ratio);
        int green = (int) (colors[0].getGreen() * (1.0f - ratio) + colors[1].getGreen() * ratio);
        int blue = (int) (colors[0].getBlue() * (1.0f - ratio) + colors[1].getBlue() * ratio);
        int alpha = (int) (colors[0].getAlpha() * (1.0f - ratio) + colors[1].getAlpha() * ratio);
        color = new Color(red, green, blue, alpha);
      } else {
        throw new RuntimeException("Unsupported gradient type: "+gradientType);
      }
    }
    return color;
  }

}
