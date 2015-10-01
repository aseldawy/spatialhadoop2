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
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**An abstract interface for a component that draws shapes*/
public abstract class Plotter {
  
  /**Configuration line for the Plotter class*/
  private static final String PlotterClass = "SingleLevelPlot.Plotter";
  
  public static void setPlotter(Configuration job, Class<? extends Plotter> plotterClass) {
    job.setClass(PlotterClass, plotterClass, Plotter.class);
  }
  
  public static Plotter getPlotter(Configuration job) {
    try {
      Class<? extends Plotter> plotterClass =
          job.getClass(PlotterClass, null, Plotter.class);
      if (plotterClass == null)
        throw new RuntimeException("Plotter class not set in job");
      Plotter plotter = plotterClass.newInstance();
      plotter.configure(job);
      return plotter;
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating plotter", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error constructing plotter", e);
    }
  }
  
  /**
   * Configures this plotter according to the MapReduce program.
   * @param conf
   */
  public void configure(Configuration conf) {
  }
  
  /**
   * Creates an empty canvas of the given width and height.
   * @param width - Width of the created layer in pixels
   * @param height - Height of the created layer in pixels
   * @param mbr - The minimal bounding rectangle of the layer in the input
   * @return
   */
  public abstract CanvasLayer createCanvas(int width, int height, Rectangle mbr);

  /**
   * Plots a set of shapes to the given layer
   * @param layer - the canvas to plot to. This canvas has to be created
   * using the method {@link #createCanvas(int, int, Rectangle)}.
   * @param shapes - a set of shapes to plot
   */
  public void plot(CanvasLayer layer, Iterable<? extends Shape> shapes) {
    for (Shape shape : shapes)
      plot(layer, shape);
  }

  /**
   * Plots one shape to the given layer
   * @param layer - the canvas to plot to. This canvas has to be created
   * using the method {@link #createCanvas(int, int, Rectangle)}.
   * @param shape - the shape to plot
   */
  public abstract void plot(CanvasLayer layer, Shape shape);

  /**
   * Returns the raster class associated with this rasterizer
   * @return
   */
  public Class<? extends CanvasLayer> getCanvasClass() {
    return this.createCanvas(0, 0, new Rectangle()).getClass();
  }
  
  /**
   * Merges an intermediate layer into the final layer based on its location
   * @param finalLayer
   * @param intermediateLayer
   */
  public abstract void merge(CanvasLayer finalLayer, CanvasLayer intermediateLayer);

  /**
   * Writes a canvas as an image to the output.
   * @param layer - the layer to be written to the output as an image
   * @param out - the output stream to which the image will be written
   * @param vflip - if <code>true</code>, the image is vertically flipped before written
   */
  public abstract void writeImage(CanvasLayer layer, DataOutputStream out,
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
  public <S extends Shape> Iterable<S> smooth(Iterable<S> r, Rectangle dataMBR,
		  int canvasWidth, int canvasHeight) {
    throw new RuntimeException("Not implemented");
  }
  
  /**
   * Tells whether this plotter supports a smooth function or not.
   * @return
   */
  public boolean isSmooth() {
    try {
      smooth(new Vector<Shape>(), new Rectangle(), 0, 0);
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }
}