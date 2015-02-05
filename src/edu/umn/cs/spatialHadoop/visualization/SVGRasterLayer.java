/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Point;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

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

  /**Underlying SVG Graphics*/
  protected SVGGraphics svgGraphics;
  
  /**The scale of the image on the x-axis in terms of pixels per input units*/
  protected double xscale;

  /**The scale of the image on the y-axis in terms of pixels per input units*/
  protected double yscale;

  /**Default constructor is necessary to be able to deserialize it*/
  public SVGRasterLayer() {
    svgGraphics = new SVGGraphics();
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
    this.svgGraphics = new SVGGraphics(width, height);
    this.svgGraphics.translate((int)(-getInputMBR().x1 * xscale), (int)(-getInputMBR().y1 * yscale));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    svgGraphics.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    svgGraphics.readFields(in);
  }
  
  public void drawShape(Shape shape) {
    shape.draw(svgGraphics, xscale, yscale);
    
  }

  public void mergeWith(SVGRasterLayer intermediateLayer) {
    Point offset = projectToImageSpace(intermediateLayer.getInputMBR().x1,
        intermediateLayer.getInputMBR().y1);
    this.svgGraphics.mergeWith(intermediateLayer.svgGraphics, offset.x, offset.y);
  }

  public void writeToFile(PrintStream ps) {
    svgGraphics.writeAsSVG(ps);
  }

}
