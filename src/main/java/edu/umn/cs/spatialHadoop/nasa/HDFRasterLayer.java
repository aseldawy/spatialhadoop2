/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

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
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.visualization.Canvas;

/**
 * A frequency map that can be used to draw a weighted heat map for NASA data
 * @author Ahmed Eldawy
 *
 */
public class HDFRasterLayer extends Canvas {
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(HDFRasterLayer.class);
  
  /**Sum of temperatures*/
  protected long[][] sum;
  /**Count of temperatures*/
  protected long[][] count;

  /**The minimum value to be used while drawing the heat map*/
  private float min;

  /**The maximum value to be used while drawing the heat map*/
  private float max;
  
  /**Timestamp of this dataset*/
  private long timestamp;
  
  /**
   * Initialize an empty frequency map to be used to deserialize 
   */
  public HDFRasterLayer() { }

  /**
   * Initializes a frequency map with the given dimensions
   * @param width
   * @param height
   */
  public HDFRasterLayer(Rectangle inputMBR, int width, int height) {
    this.inputMBR = inputMBR;
    this.width = width;
    this.height = height;
    this.sum = new long[width][height];
    this.count = new long[width][height];
    this.min = -1; this.max = -2;
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
  
  public long getSum(int x, int y) {
    return sum[x][y];
  }
  
  public long getCount(int x, int y) {
    return count[x][y];
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(timestamp);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzos = new GZIPOutputStream(baos);
    ByteBuffer bbuffer = ByteBuffer.allocate(getHeight() * 2 * 8 + 8);
    bbuffer.putInt(getWidth());
    bbuffer.putInt(getHeight());
    gzos.write(bbuffer.array(), 0, bbuffer.position());
    for (int x = 0; x < getWidth(); x++) {
      bbuffer.clear();
      for (int y = 0; y < getHeight(); y++) {
        bbuffer.putLong(sum[x][y]);
        bbuffer.putLong(count[x][y]);
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
    this.timestamp = in.readLong();
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
    if (width != this.getWidth() || height != this.getHeight()) {
      sum = new long[width][height];
      count = new long[width][height];
    }
    buffer = new byte[getHeight() * 2 * 8];
    for (int x = 0; x < getWidth(); x++) {
      int size = 0;
      while (size < buffer.length) {
        size += gzis.read(buffer, size, buffer.length - size);
      }
      bbuffer = ByteBuffer.wrap(buffer);
      for (int y = 0; y < getHeight(); y++) {
        sum[x][y] = bbuffer.getLong();
        count[x][y] = bbuffer.getLong();
      }
    }
  }
  
  public void mergeWith(HDFRasterLayer another) {
    this.timestamp = Math.max(this.timestamp, another.timestamp);
    Point offset = projectToImageSpace(another.getInputMBR().x1, another.getInputMBR().y1);
    int xmin = Math.max(0, offset.x);
    int ymin = Math.max(0, offset.y);
    int xmax = Math.min(this.getWidth(), another.getWidth() + offset.x);
    int ymax = Math.min(this.getHeight(), another.getHeight() + offset.y);
    for (int x = xmin; x < xmax; x++) {
      for (int y = ymin; y < ymax; y++) {
        this.sum[x][y] += another.sum[x - offset.x][y - offset.y];
        this.count[x][y] += another.count[x - offset.x][y - offset.y];
      }
    }
  }
  
  public BufferedImage asImage() {
    // Calculate the average
    float[][] avg = new float[getWidth()][getHeight()];
    for (int x = 0; x < this.getWidth(); x++) {
      for (int y = 0; y < this.getHeight(); y++) {
        avg[x][y] = (float)sum[x][y] / count[x][y];
      }
    }
    if (min >= max) {
      // Values not set. Autodetect
      min = Float.MAX_VALUE;
      max = -Float.MAX_VALUE;
      for (int x = 0; x < this.getWidth(); x++) {
        for (int y = 0; y < this.getHeight(); y++) {
          if (avg[x][y] < min)
            min = avg[x][y];
          if (avg[x][y] > max)
            max = avg[x][y];
        }
      }
    }
    BufferedImage image = new BufferedImage(getWidth(), getHeight(), BufferedImage.TYPE_INT_ARGB);
    for (int x = 0; x < this.getWidth(); x++) {
      for (int y = 0; y < this.getHeight(); y++) {
        if (count[x][y] > 0) {
          Color color = calculateColor(avg[x][y], min, max);
          image.setRGB(x, y, color.getRGB());
        } else {
          image.setRGB(x, y, 0);
        }
      }
    }
    return image;
  }

  public void addPoint(int x, int y, int weight) {
    if (x >= 0 && x < getWidth() && y >= 0 && y < getHeight()) {
      sum[x][y] += weight;
      count[x][y]++;
    }
  }

  /**
   * Adds a range of points, defined by a rectangle, to the frequency map
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @param weight
   */
  public void addPoints(int x1, int y1, int x2, int y2, int weight) {
    if (x1 < 0)
      x1 = 0;
    if (y1 < 0)
      y1 = 0;
    if (x2 >= getWidth())
      x2 = getWidth() - 1;
    if (y2 >= getHeight())
      y2 = getHeight() - 1;
    for (int x = x1; x < x2; x++) {
      for (int y = y1; y < y2; y++) {
        sum[x][y] += weight;
        count[x][y]++;
      }
    }
  }

  public int getWidth() {
    return sum == null? 0 : sum.length;
  }

  public int getHeight() {
    return sum == null? 0 : sum[0].length;
  }
  
  /* The following methods are used to compute the gradient */

  public void setTimestamp(long ts) {
    this.timestamp = ts;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  protected Color[] colors;
  protected float[] hues;
  protected float[] saturations;
  protected float[] brightnesses;
  
  public enum GradientType {GT_HSB, GT_RGB};
  protected GradientType gradientType;
  
  public void setGradientInfo(Color color1, Color color2, GradientType gradientType) {
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

  /**
   * Recover holes using linear interpolation according to the given water mask
   * @param waterMask
   */
  public void recoverHoles(BitArray waterMask) {
    // Store the status of each value
    // 0 - the corresponding entry originally contained a value
    // 1 - the entry did not contain a value and still does not contain one
    // 2 - the entry did not contain a value and its current value is copied
    // 3 - the entry did not contain a value and its current value is interpolated
    byte[] valueStatus = new byte[getWidth() * getHeight()];
    // Recover in x-direction
    for (int y = 0; y < height; y++) {
      int x2 = 0;
      while (x2 < getWidth()) {
        int x1 = x2;
        // x1 should point to the first missing point
        while (x1 < getWidth() && count[x1][y] > 0)
          x1++;
        x2 = x1;
        // x2 should point to the first non-missing point
        while (x2 < getWidth() && count[x2][y] == 0)
          x2++;
        // Recover all points in the range [x1, x2)
        if (x1 == 0 && x2 == getWidth()) {
          // All the line is empty. Nothing can be done
          // Just mark it for the y-direction round
          for (int x = x1; x < x2; x++)
            valueStatus[y * width + x] = 1;
        } else if (x1 == 0 || x2 == getWidth()) {
          // One value at one end. Replicate it to all missing points
          long recoverCount = x1 == 0? count[x2][y] : count[x1-1][y];
          long recoverSum = x1 == 0? sum[x2][y] : sum[x1-1][y];
          for (int x = x1; x < x2; x++) {
            if (waterMask.get(y * width + x)) {
              sum[x][y] = recoverSum;
              count[x][y] = recoverCount;
            }
            valueStatus[y * width + x] = 2;
          }
        } else {
          long average1 = sum[x1-1][y] / count[x1-1][y];
          long average2 = sum[x2][y] / count[x2][y];
          // Two end point. Interpolate between them
          for (int x = x1; x < x2; x++) {
            if (waterMask.get(y * width + x)) {
              // Adjust the sum and count so that the average is correct
              sum[x][y] = average1 * (x2 - x) + average2 * (x - x1);
              count[x][y] = x2 - x1;
            }
            valueStatus[y * width + x] = 3;
          }
        }
      }
    }
    
    // Recover in y-direction
    for (int x = 0; x < width; x++) {
      int y2 = 0;
      while (y2 < height) {
        int y1 = y2;
        // x1 should point to the first missing point
        while (y1 < height && valueStatus[y1 * width + x] == 0)
          y1++;
        y2 = y1;
        // y2 should point to the first non-missing point
        while (y2 < height && valueStatus[y2 * width + x] != 0)
          y2++;
        // Recover all points in the range [y1, y2)
        if (y1 == 0 && y2 == height) {
          // All the line is empty. Nothing can be done
          // No need to mark them as there is no thrid round
        } else if (y1 == 0 || y2 == height) {
          // One value at one end. Replicate it to all missing points
          long recoverCount = y1 == 0? count[x][y2] : count[x][y1-1];
          long recoverSum = y1 == 0? sum[x][y2] : sum[x][y1-1];
          for (int y = y1; y < y2; y++) {
            if (waterMask.get(y * width + x)) {
              if (valueStatus[y * width + x] == 1) {
                // Value has never been recovered but needs to
                sum[x][y] = recoverSum;
                count[x][y] = recoverCount;
              } else if (valueStatus[y * width + x] == 2) {
                // Value has been previously copied. Take average
                sum[x][y] += recoverSum;
                count[x][y] += recoverCount;
              }
            }
          }
        } else {
          // Two end point. Interpolate between them
          for (int y = y1; y < y2; y++) {
            long average1 = sum[x][y1-1] / count[x][y1-1];
            long average2 = sum[x][y2] / count[x][y2];
            if (waterMask.get(y * width + x)) {
              if (valueStatus[y * width + x] <= 2) {
                // Value has never been recovered or has been copied
                sum[x][y] = average1 * (y2 - y) + average2 * (y - y1);
                count[x][y] = y2 - y1;
              } else {
                // Value has been previously interpolated, take average
                sum[x][y] += average1 * (y2 - y) + average2 * (y - y1);
                count[x][y] += y2 - y1;
              }
            }
          }
        }
      }
    }
  }

}
