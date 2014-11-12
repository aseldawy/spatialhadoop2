/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * @author Ahmed Eldawy
 *
 */
public class GeometricPlot2 {
  
  public static class GeometricRasterizer extends Rasterizer {
    
    private Color strokeColor;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.strokeColor = OperationsParams.getColor(conf, "color", Color.BLACK);
    }

    @Override
    public RasterLayer create(int width, int height, Rectangle mbr) {
      ImageRasterLayer imageRasterLayer = new ImageRasterLayer(mbr, width, height);
      imageRasterLayer.setColor(strokeColor);
      return imageRasterLayer;
    }

    @Override
    public void rasterize(RasterLayer rasterLayer, Iterable<? extends Shape> shapes) {
      ImageRasterLayer imgLayer = (ImageRasterLayer) rasterLayer;
      imgLayer.setColor(strokeColor);
      for (Shape shape : shapes) {
        imgLayer.drawShape(shape);
      }
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return ImageRasterLayer.class;
    }

    @Override
    public void mergeLayers(RasterLayer finalLayer,
        RasterLayer intermediateLayer) {
      // Calculate the offset of the intermediate layer in the final raster layer based on its MBR
      Rectangle finalMBR = finalLayer.getInputMBR();
      Rectangle intermediateLayerMBR = intermediateLayer.getInputMBR();
      int xOffset = (int) Math.floor((intermediateLayerMBR.x1 - finalMBR.x1) * finalLayer.getWidth() / finalMBR.getWidth());
      int yOffset = (int) Math.floor((intermediateLayerMBR.y1 - finalMBR.y1) * finalLayer.getHeight() / finalMBR.getHeight());
      ((ImageRasterLayer)finalLayer).mergeWith(xOffset, yOffset, (ImageRasterLayer) intermediateLayer);
    }

    @Override
    public BufferedImage toImage(RasterLayer layer) {
      return ((ImageRasterLayer)layer).asImage();
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
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path inFile = params.getInputPath();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    SingleLevelPlot.plotMapReduce(inFile, outFile, GeometricRasterizer.class, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }

}
