/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

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
      Rasterizer rasterizer = rasterizerClass.newInstance();
      rasterizer.configure(job);
      return rasterizer;
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rasterizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error constructing rasterizer", e);
    }
  }
  
  /**
   * Configures this rasterizer according to the MapReduce program.
   * @param conf
   */
  public void configure(Configuration conf) {
  }
  
  /**
   * Creates an empty raster layer of the given width and height.
   * @param width - Width of the created layer in pixels
   * @param height - Height of the created layer in pixels
   * @param mbr - The minimal bounding rectangle of the layer in the input
   * @return
   */
  public abstract RasterLayer createRaster(int width, int height, Rectangle mbr);

  /**
   * Rasterizes a set of shapes to the given layer
   * @param layer - the layer to rasterize to. This layer has to be created
   * using the method {@link #createRaster(int, int, Rectangle)}.
   * @param shapes - a set of shapes to rasterize
   */
  public void rasterize(RasterLayer layer, Iterable<? extends Shape> shapes) {
    for (Shape shape : shapes)
      rasterize(layer, shape);
  }

  /**
   * Rasterize a set of records by calling the rasterize function on each one.
   * @param layer
   * @param shapes
   */
  public void rasterize(RasterLayer layer, Iterator<? extends Shape> shapes) {
    while (shapes.hasNext())
      rasterize(layer, shapes.next());
  }

  /**
   * Rasterizes one shape to the given layer
   * @param layer - the layer to rasterize to. This layer has to be created
   * using the method {@link #createRaster(int, int, Rectangle)}.
   * @param shape - the shape to rasterize
   */
  public abstract void rasterize(RasterLayer layer, Shape shape);

  /**
   * Returns the raster class associated with this rasterizer
   * @return
   */
  public Class<? extends RasterLayer> getRasterClass() {
    return this.createRaster(0, 0, new Rectangle()).getClass();
  }
  
  /**
   * Merges an intermediate layer into the final layer based on its location
   * @param finalLayer
   * @param intermediateLayer
   */
  public abstract void merge(RasterLayer finalLayer, RasterLayer intermediateLayer);

  /**
   * Writes a raster layer as an image to the output.
   * @param layer - the layer to be written to the output as an image
   * @param out - the output stream to which the image will be written
   * @param vflip - if <code>true</code>, the image is vertically flipped before written
   */
  public abstract void writeImage(RasterLayer layer, DataOutputStream out,
      boolean vflip) throws IOException;
  
  /**
   * Returns the radius in pixels that one object might affect in the image.
   * This is used to ensure that each image is generated correctly by using
   * all records in the buffer area.
   * @return
   */
  public int getRadius() {
    return 0;
  }
  
  /**
   * Smooth a set of records that are spatially close to each other and returns
   * a new set of smoothed records. This method is called on the original
   * raw data before it is visualized. The results of this function are the
   * records that are visualized.
   * @param r
   * @return
   */
  public <S extends Shape> Iterable<S> smooth(Iterable<S> r) {
    throw new RuntimeException("Not implemented");
  }
  
  /**
   * Tells whether this rasterizer supports a smooth function or not.
   * @return
   */
  public boolean isSmooth() {
    try {
      smooth(new Vector<Shape>());
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }
}