/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;

/**
 * A raster layer that contains a set of vector data.
 * It seems wrong because vector and raster are two different things.
 * Although this layer contains vector data, it keeps into consideration
 * that this data is drawn at a specific raster resolution. It automatically
 * simplifies objects drawn on it to match the configured resolution.
 * In addition, it automatically removes very small objects that are much
 * smaller than a pixel and removes some records if they are hidden behind
 * other records.
 * @author Ahmed Eldawy
 *
 */
public class SVGRasterLayer extends RasterLayer {

  /**The scale of the image on the x-axis in terms of pixels per input units*/
  protected double xscale;

  /**The scale of the image on the y-axis in terms of pixels per input units*/
  protected double yscale;

  /**Default constructor is necessary to be able to deserialize it*/
  public SVGRasterLayer() {
  }

  /**
   * Creates a raster layer of the given size for a given (portion of) input
   * data.
   * @param inputMBR - the MBR of the input area to rasterize
   * @param width - width the of the image to generate in pixels
   * @param height - height of the image to generate in pixels
   */
  public SVGRasterLayer(Rectangle inputMBR, int width, int height) {
    super(inputMBR, width, height);
    xscale = width / getInputMBR().getWidth();
    yscale = height / getInputMBR().getHeight();
  }

  public void drawShape(OSMPolygon shape) {
    
  }

}
