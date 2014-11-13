/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.image.BufferedImage;

import org.apache.hadoop.conf.Configuration;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**An abstract interface for a component that rasterizes shapes*/
public abstract class Rasterizer {
  
  /**Configuration line for the rasterizer class*/
  private static final String RasterizerClass = "SingleLevelPlot.Rasterizer";
  
  public static void setRasterizer(Configuration job, Class<? extends Rasterizer> rasterizerClass) {
    job.setClass(RasterizerClass, rasterizerClass, Rasterizer.class);
  }
  
  public static Rasterizer getRasterizer(Configuration job) {
    try {
      Class<? extends Rasterizer> rasterizerClass =
          job.getClass(RasterizerClass, null, Rasterizer.class);
      if (rasterizerClass == null)
        throw new RuntimeException("Rasterizer class not set in job");
      return rasterizerClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rasterizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error constructing rasterizer", e);
    }
  }
  
  public void configure(Configuration conf) {
  }
  
  /**
   * Creates an empty raster layer of the given width and height.
   * @param width - Width of the created layer in pixels
   * @param height - Height of the created layer in pixels
   * @param mbr - The minimal bounding rectangle of the layer in the input
   * @return
   */
  public abstract RasterLayer create(int width, int height, Rectangle mbr);

  /**
   * Rasterizes a set of shapes to the given layer
   * @param layer - the layer to rasterize to. This layer has to be created
   * using the method {@link #create(int, int, Rectangle)}.
   * @param shapes - a set of shapes to rasterize
   */
  public void rasterize(RasterLayer layer, Iterable<? extends Shape> shapes) {
    for (Shape shape : shapes)
      rasterize(layer, shape);
  }

  /**
   * Rasterizes one shape to the given layer
   * @param layer - the layer to rasterize to. This layer has to be created
   * using the method {@link #create(int, int, Rectangle)}.
   * @param shape - the shape to rasterize
   */
  public abstract void rasterize(RasterLayer layer, Shape shape);

  /**
   * Returns the raster class associated with this rasterizer
   * @return
   */
  public abstract Class<? extends RasterLayer> getRasterClass();
  
  /**
   * Merges an intermediate layer into the final layer based on its location
   * @param finalLayer
   * @param intermediateLayer
   */
  public abstract void mergeLayers(RasterLayer finalLayer, RasterLayer intermediateLayer);

  /**
   * Converts a raster layer to an image.
   * @param layer
   * @return
   */
  public abstract BufferedImage toImage(RasterLayer layer);
}