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
import java.awt.Composite;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GraphicsConfiguration;
import java.awt.Image;
import java.awt.Paint;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.RenderingHints.Key;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.font.FontRenderContext;
import java.awt.font.GlyphVector;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ImageObserver;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.RenderableImage;
import java.io.File;
import java.io.IOException;
import java.text.AttributedCharacterIterator;
import java.util.Arrays;
import java.util.Map;

import javax.imageio.ImageIO;

/**
 * A simple graphics class that draws directly on an a BufferedImage and does
 * not require an active X11 display.
 * @author eldawy
 *
 */
public class SimpleGraphics extends Graphics2D {
  
  /**
   * The underlying image on which all draw commands go
   */
  private final BufferedImage image;
  private Color background;
  private Color color;
  private int[] pixels;
  /**Amount of translation along the x-axis*/
  private int tx;
  /**Amount of translation along the y-axis*/
  private int ty;

  public SimpleGraphics(BufferedImage image) {
    this.image = image;
    this.pixels = new int[image.getWidth() * image.getHeight()];
    image.getRGB(0, 0, image.getWidth(), image.getHeight(), pixels, 0,
        image.getWidth());
  }

  @Override
  public void draw(Shape s) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public boolean drawImage(Image img, AffineTransform xform, ImageObserver obs) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawImage(BufferedImage img, BufferedImageOp op, int x, int y) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawRenderedImage(RenderedImage img, AffineTransform xform) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawRenderableImage(RenderableImage img, AffineTransform xform) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawString(String str, int x, int y) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawString(String str, float x, float y) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawString(AttributedCharacterIterator iterator, int x, int y) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawString(AttributedCharacterIterator iterator, float x, float y) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawGlyphVector(GlyphVector g, float x, float y) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void fill(Shape s) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public boolean hit(Rectangle rect, Shape s, boolean onStroke) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public GraphicsConfiguration getDeviceConfiguration() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setComposite(Composite comp) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setPaint(Paint paint) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setStroke(Stroke s) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setRenderingHint(Key hintKey, Object hintValue) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Object getRenderingHint(Key hintKey) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setRenderingHints(Map<?, ?> hints) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void addRenderingHints(Map<?, ?> hints) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public RenderingHints getRenderingHints() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void translate(int tx, int ty) {
    this.tx += tx;
    this.ty += ty;
  }

  @Override
  public void translate(double tx, double ty) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void rotate(double theta) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void rotate(double theta, double x, double y) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void scale(double sx, double sy) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void shear(double shx, double shy) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void transform(AffineTransform Tx) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setTransform(AffineTransform Tx) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public AffineTransform getTransform() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Paint getPaint() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Composite getComposite() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setBackground(Color color) {
    this.background = color;
    
  }

  @Override
  public Color getBackground() {
    return background;
  }

  @Override
  public Stroke getStroke() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void clip(Shape s) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public FontRenderContext getFontRenderContext() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Graphics create() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Color getColor() {
    return this.color;    
  }

  @Override
  public void setColor(Color c) {
    this.color = c;
  }

  @Override
  public void setPaintMode() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setXORMode(Color c1) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Font getFont() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setFont(Font font) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public FontMetrics getFontMetrics(Font f) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Rectangle getClipBounds() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void clipRect(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setClip(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public Shape getClip() {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void setClip(Shape clip) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void copyArea(int x, int y, int width, int height, int dx, int dy) {
    throw new RuntimeException("Not implemented");
  }
  
  protected void setPixel(int x, int y, int rgb) {
    x += tx; y += ty;
    if (x >= 0 && y >= 0 && x < image.getWidth() && y < image.getHeight()) {
      int offset = y * image.getWidth() + x;
      int d_alpha = pixels[offset] >>> 24;
      int s_alpha = rgb >>> 24;
      if (d_alpha == 0) {
        pixels[offset] = rgb;
      } else if (s_alpha != 0) {
        int r_alpha = s_alpha + d_alpha * (255 - s_alpha) / 256;
        int r_pixel = 0;
        for (int i = 0; i < 3; i++) {
          int s_component = (rgb >>> (8 * i)) & 0xff;
          int d_component = (pixels[offset] >>> (8 * i)) & 0xff;
          int merge_component = (s_component * s_alpha +
              d_component * d_alpha * (255-s_alpha) / 256)
              / r_alpha;
          r_pixel |= merge_component << (8 * i);
        }
        r_pixel |= r_alpha << 24;
        pixels[offset] = r_pixel;
      }
    }
  }

  @Override
  public void drawLine(int x1, int y1, int x2, int y2) {
    x1 += tx; y1 += ty;
    x2 += tx; y2 += ty;
    if (y1 == y2 || x1 == x2) {
      if (x1 > x2) {
        x1 ^= x2;
        x2 ^= x1;
        x1 ^= x2;
      }
      if (y1 > y2) {
        y1 ^= y2;
        y2 ^= y1;
        y1 ^= y2;
      }
      if (x2 < 0 || y2 < 0)
        return;
      if (x1 >= image.getWidth() && x1 >= image.getHeight())
        return;
      if (x1 < 0)
        x1 = 0;
      if (y1 < 0)
        y1 = 0;
      if (x2 >= image.getWidth())
        x2 = image.getWidth() - 1;
      if (y2 >= image.getHeight())
        y2 = image.getHeight() - 1;
      // Draw an orthogonal line
      dumpRect(x1, y1, x2 - x1 + 1, y2 - y1 + 1, color);
    } else {
      int dx = Math.abs(x2 - x1);
      int dy = Math.abs(y2 - y1);
      if (dx > dy) {
        if (x1 > x2) {
          // Ensure that x1 <= x2
          x1 ^= x2; x2 ^= x1; x1 ^= x2;
          y1 ^= y2; y2 ^= y1; y1 ^= y2;
        }
        int incy = y1 < y2? 1 : -1;
        int p = dy - dx / 2;
        int y = y1;
        for (int x = x1; x <= x2; x++) {
          setPixel(x, y, color.getRGB());
          if (p > 0) {
            y += incy;
            p += dy - dx;
          } else
            p += dy;
        }
      } else {
        if (y1 > y2) {
          // Ensure that y1 < y2
          x1 ^= x2; x2 ^= x1; x1 ^= x2;
          y1 ^= y2; y2 ^= y1; y1 ^= y2;
        }
        int incx = x1 < x2? 1 : -1;
        int p = dx - dy / 2;
        int x = x1;
        for (int y = y1; y <= y2; y++) {
          setPixel(x, y, color.getRGB());
          if (p > 0) {
            x += incx;
            p += dx - dy;
          } else
            p += dx;
        }
      }
    }
  }

  @Override
  public void fillRect(int x, int y, int width, int height) {
    x += tx; y += ty;
    dumpRect(x, y, width, height, color);
  }

  @Override
  public void clearRect(int x, int y, int width, int height) {
    x += tx; y += ty;
    dumpRect(x, y, width, height, background);
  }
  
  protected void dumpRect(int x, int y, int width, int height, Color color) {
    // Do not apply the transformation here as it is assumed to be applied already
    if (x < 0) {
      width += x;
      x = 0;
    }
    if (y < 0) {
      height += y;
      y = 0;
    }
    if (y + height > image.getHeight())
      height -= y + height - image.getHeight();
    if (x + width > image.getWidth())
      width = image.getWidth() - x;
    if (x >= image.getWidth() || y >= image.getHeight() ||
        width < 0 || height < 0)
      return;
    while (height-- > 0) {
      int offset = y * image.getWidth() + x;
      Arrays.fill(pixels, offset, offset + width, color.getRGB());
      y++;
    }
  }
  
  @Override
  public void drawRoundRect(int x, int y, int width, int height, int arcWidth,
      int arcHeight) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void fillRoundRect(int x, int y, int width, int height, int arcWidth,
      int arcHeight) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawOval(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void fillOval(int x, int y, int width, int height) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawArc(int x, int y, int width, int height, int startAngle,
      int arcAngle) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void fillArc(int x, int y, int width, int height, int startAngle,
      int arcAngle) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public void drawPolyline(int[] xPoints, int[] yPoints, int nPoints) {
    for (int i = 1; i < nPoints; i++)
      drawLine(xPoints[i-1], yPoints[i-1], xPoints[i], yPoints[i]);
  }

  @Override
  public void drawPolygon(int[] xPoints, int[] yPoints, int nPoints) {
    // Do not apply the transformation here as drawLine will apply it
    for (int i = 1; i < nPoints; i++)
      drawLine(xPoints[i-1], yPoints[i-1], xPoints[i], yPoints[i]);
    drawLine(xPoints[nPoints-1], yPoints[nPoints-1], xPoints[0], yPoints[0]);
  }

  @Override
  public void fillPolygon(int[] xPoints, int[] yPoints, int nPoints) {
    throw new RuntimeException("Not implemented");
    
  }

  @Override
  public boolean drawImage(Image img, int x, int y, ImageObserver observer) {
    // Do not apply the transformation here as drawImage will apply it
    return drawImage(img, x, y,
        img.getWidth(observer), img.getHeight(observer), observer);
  }

  @Override
  public boolean drawImage(Image img, int x, int y, int width, int height,
      ImageObserver observer) {
    // Do not apply the transformation here as drawImage will apply it
    return drawImage(img, x, y, x + width - 1, y + height - 1, 0, 0,
        img.getWidth(observer) - 1, img.getHeight(observer) - 1, observer);
  }

  @Override
  public boolean drawImage(Image img, int x, int y, Color bgcolor,
      ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int x, int y, int width, int height,
      Color bgcolor, ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean drawImage(Image img, int dx1, int dy1, int dx2, int dy2,
      int sx1, int sy1, int sx2, int sy2, ImageObserver observer) {
    dx1 += tx; dy1 += ty;
    dx2 += tx; dy2 += ty;
    if (img instanceof BufferedImage) {
      BufferedImage simg = (BufferedImage) img;
      if (dx2 - dx1 != sx2 - sx1 || dy2-dy1 != sy2-sy1)
        throw new RuntimeException("Cannot scale images");
      if (dx1 < 0) {
        sx1 -= dx1;
        dx1 = 0;
      }
      if (dy1 < 0) {
        sy1 -= dy1;
        dy1 = 0;
      }
      if (dx2 >= image.getWidth()) {
        int diff = dx2 - image.getWidth() + 1;
        sx2 -= diff;
        dx2 -= diff;
      }
      if (dy2 >= image.getHeight()) {
        int diff = dy2 - image.getHeight() + 1;
        sy2 -= diff;
        dy2 -= diff;
      }
      if (sy1 < 0) {
        dy1 += sy1;
        sy1 = 0;
      }
      if (sx1 < 0) {
        dx1 += sx1;
        sx1 = 0;
      }
      if (sx2 >= simg.getWidth()) {
        int diff = sx2 - simg.getWidth() + 1;
        sx2 -= diff;
        dx2 -= diff;
      }
      if (sy2 >= simg.getHeight()) {
        int diff = sy2 - simg.getHeight() + 1;
        sy2 -= diff;
        dy2 -= diff;
      }
      int offset = dy1 * image.getWidth() + dx1;
      if ((sx2 - sx1 + 1) * (sy2 - sy1 + 1) <= 0) {
        // No pixels in range. perhaps the whole image is clipped
        return true;
      }
      int[] spixels = new int[(sx2 - sx1 + 1) * (sy2 - sy1 + 1)];
      simg.getRGB(sx1, sy1, sx2 - sx1 + 1, sy2 - sy1 + 1, spixels, 0, sx2 - sx1
          + 1);
      int s_pixel = 0;
      for (int dy = dy1; dy <= dy2; dy++) {
        int d_pixel = offset;
        for (int dx = dx1; dx <= dx2; dx++) {
          int d_alpha = pixels[d_pixel] >>> 24;
          int s_alpha = spixels[s_pixel] >>> 24;
          if (d_alpha == 0) {
            pixels[d_pixel] = spixels[s_pixel];
          } else if (s_alpha != 0) {
            int r_alpha = s_alpha + d_alpha * (255 - s_alpha) / 256;
            int r_pixel = 0;
            for (int i = 0; i < 3; i++) {
              int s_component = (spixels[s_pixel] >>> (8 * i)) & 0xff;
              int d_component = (pixels[d_pixel] >>> (8 * i)) & 0xff;
              int merge_component = (s_component * s_alpha +
                  d_component * d_alpha * (255-s_alpha) / 256)
                  / r_alpha;
              r_pixel |= merge_component << (8 * i);
            }
            r_pixel |= r_alpha << 24;
            pixels[d_pixel] = r_pixel;
          }
    
          d_pixel++;
          s_pixel++;
        }
        offset += image.getWidth();
      }
      return true;
    } else {
      throw new RuntimeException("Not implemented for "+img.getClass());
    }
  }

  @Override
  public boolean drawImage(Image img, int dx1, int dy1, int dx2, int dy2,
      int sx1, int sy1, int sx2, int sy2, Color bgcolor, ImageObserver observer) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void dispose() {
    image.setRGB(0, 0, image.getWidth(), image.getHeight(), pixels, 0, image.getWidth());
  }

  public static void main(String[] args) throws IOException {
    BufferedImage image = new BufferedImage(100, 100, BufferedImage.TYPE_INT_ARGB);
    Graphics2D g = new SimpleGraphics(image);
//    Graphics2D g = image.createGraphics();
    g.setBackground(new Color(255, 0, 0, 128));
    g.translate(20, 15);
    g.clearRect(0, 0, 100, 100);
    g.setColor(new Color(0, 0, 0, 255));
    g.drawLine(0, 0, 50, 10);
    g.drawLine(0, 0, 10, 50);
    g.drawLine(50, 50, 80, 30);
    g.drawLine(50, 50, 30, 80);
    
    BufferedImage image2 = new BufferedImage(30, 30, BufferedImage.TYPE_INT_ARGB);
//    Graphics2D g2 = new SimpleGraphics(image2);
    Graphics2D g2 = image2.createGraphics();
    g2.setBackground(new Color(0, 255, 0, 128));
    g2.clearRect(0, 0, 30, 30);
    g2.dispose();
    g.drawImage(image2, 70, 70, null);
    g.drawImage(image2, 0, 0, null);
    
    g.dispose();
    ImageIO.write(image, "png", new File("test.png"));
  }
    
}
