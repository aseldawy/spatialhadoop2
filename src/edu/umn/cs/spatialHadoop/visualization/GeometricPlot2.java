/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
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
    private Rectangle inputMBR;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.strokeColor = OperationsParams.getColor(conf, "color", Color.BLACK);
      this.inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
    }

    @Override
    public RasterLayer create(int width, int height) {
      ImageRasterLayer imageRasterLayer = new ImageRasterLayer(0, 0, width, height);
      imageRasterLayer.setMBR(inputMBR);
      imageRasterLayer.setColor(strokeColor);
      return imageRasterLayer;
    }

    @Override
    public RasterLayer rasterize(Rectangle inputMBR, int imageWidth,
        int imageHeight, Rectangle partitionMBR, Iterable<? extends Shape> shapes) {
      // Calculate the dimensions of the generated raster layer by calculating
      // the MBR in the image space
      // Note: Do not calculate from the width and height of partition MBR
      // because it will cause round-off errors between adjacent partitions
      // which might leave gaps in the final generated image
      int rasterLayerX1 = (int) Math.floor((partitionMBR.x1 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerX2 = (int) Math.ceil((partitionMBR.x2 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerY1 = (int) Math.floor((partitionMBR.y1 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      int rasterLayerY2 = (int) Math.ceil((partitionMBR.y2 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      ImageRasterLayer rasterLayer = new ImageRasterLayer(rasterLayerX1, rasterLayerY1, rasterLayerX2 - rasterLayerX1,
          rasterLayerY2 - rasterLayerY1);
      rasterLayer.setMBR(inputMBR);
      rasterLayer.setColor(strokeColor);
      for (Shape shape : shapes) {
        rasterLayer.drawShape(shape);
      }
      
      return rasterLayer;
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return ImageRasterLayer.class;
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
