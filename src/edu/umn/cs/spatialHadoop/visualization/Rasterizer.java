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
   * Creates a raster layer that represents the given list of shapes
   * @param inputMBR - the MBR of the input file
   * @param imageHeight - Total width for the output image
   * @param imageWidth - Total height for the output image
   * @param partitionMBR - The MBR of the partition to rasterize
   * @param shapes - The shapes to rasterize
   * @return
   */
  public abstract void rasterize(RasterLayer layer, Iterable<? extends Shape> shapes);

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