/***********************************************************************
j* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * Plots to an SVG file
 * @author Ahmed Eldawy
 *
 */
public class SVGPlot {
  
  /**
   * A rasterizer that draws the resulting image as SVG.
   * It automatically down samples the data to match the resolution
   * of the generated image. This means that drawing to a small image
   * would reduce the level of details in the vector data.  
   * @author Ahmed Eldawy
   *
   */
  public static class SVGRasterizer extends Rasterizer {

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
    }

    @Override
    public RasterLayer createRaster(int width, int height, Rectangle mbr) {
      SVGRasterLayer svgRasterLayer = new SVGRasterLayer(mbr, width, height);
      return svgRasterLayer;
    }

    @Override
    public void rasterize(RasterLayer rasterLayer, Shape shape) {
      SVGRasterLayer svgLayer = (SVGRasterLayer) rasterLayer;
      svgLayer.drawShape(shape);
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return SVGRasterLayer.class;
    }

    @Override
    public void merge(RasterLayer finalLayer,
        RasterLayer intermediateLayer) {
      ((SVGRasterLayer)finalLayer).mergeWith((SVGRasterLayer) intermediateLayer);
    }

    @Override
    public void writeImage(RasterLayer layer, DataOutputStream out,
        boolean vflip) throws IOException {
      out.flush();
      PrintStream ps = new PrintStream(out);
      ((SVGRasterLayer)layer).writeToFile(ps);
      ps.flush();
    }
  }
  
  private static void printUsage() {
    System.out.println("Plots all shapes to an image");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("width:<w> - Maximum width of the image (1000)");
    System.out.println("height:<h> - Maximum height of the image (1000)");
    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("partition:<data|space> - whether to use data partitioning (default) or space partitioning");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
    System.out.println("-fade: Use the gradual fade option");
    System.out.println("-sample: Use the daptive sample option");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    System.setProperty("java.awt.headless", "true");
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path[] inFiles = params.getInputPaths();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    if (params.getBoolean("pyramid", false)) {
      MultilevelPlot.plot(inFiles, outFile, SVGRasterizer.class, params);
    } else {
      SingleLevelPlot.plot(inFiles, outFile, SVGRasterizer.class, params);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }

}
