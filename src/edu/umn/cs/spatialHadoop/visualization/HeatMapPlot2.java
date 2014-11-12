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
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.visualization.FrequencyMapRasterLayer.GradientType;
import edu.umn.cs.spatialHadoop.visualization.FrequencyMapRasterLayer.SmoothType;

/**
 * @author Ahmed Eldawy
 *
 */
public class HeatMapPlot2 {

  public static class HeatMapRasterizer extends Rasterizer {

    /**Radius of the heat map smooth in pixels*/
    private int radius;
    /**Type of smoothing to use in the frequency map*/
    private SmoothType smoothType;
    /**Color associated with minimum value*/
    private Color color1;
    /**Color associated with maximum value*/
    private Color color2;
    /**Type of gradient to use between minimum and maximum values*/
    private GradientType gradientType;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.radius = conf.getInt("radius", 5);
      this.smoothType = conf.getBoolean("smooth", true)? SmoothType.Gaussian : SmoothType.Flat;
      this.color1 = OperationsParams.getColor(conf, "color1", new Color(0, 0, 255, 0));
      this.color2 = OperationsParams.getColor(conf, "color2", new Color(255, 0, 0, 255));
      this.gradientType = conf.get("gradient", "hsb").equals("hsb") ? GradientType.GT_HSB : GradientType.GT_RGB;
    }
    
    @Override
    public RasterLayer create(int width, int height, Rectangle mbr) {
      FrequencyMapRasterLayer rasterLayer = new FrequencyMapRasterLayer(mbr, width, height, radius, smoothType);
      rasterLayer.setGradientInfor(color1, color2, gradientType);
      return rasterLayer;
    }

    @Override
    public void rasterize(RasterLayer rasterLayer, Iterable<? extends Shape> shapes) {
      FrequencyMapRasterLayer frequencyMap = (FrequencyMapRasterLayer) rasterLayer;
      Rectangle inputMBR = rasterLayer.getInputMBR();
      for (Shape shape : shapes) {
        Point center;
        if (shape instanceof Point) {
          center = (Point) shape;
        } else if (shape instanceof Rectangle) {
          center = ((Rectangle) shape).getCenterPoint();
        } else {
          Rectangle shapeMBR = shape.getMBR();
          if (shapeMBR == null)
            continue;
          center = shapeMBR.getCenterPoint();
        }
        int centerx = (int) Math.round((center.x - inputMBR.x1) * rasterLayer.getWidth() / inputMBR.getWidth());
        int centery = (int) Math.round((center.y - inputMBR.y1) * rasterLayer.getHeight() / inputMBR.getHeight());

        frequencyMap.addPoint(centerx, centery);
      }
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return FrequencyMapRasterLayer.class;
    }

    @Override
    public void mergeLayers(RasterLayer finalLayer,
        RasterLayer intermediateLayer) {
      // Calculate the offset of the intermediate layer in the final raster layer based on its MBR
      Rectangle finalMBR = finalLayer.getInputMBR();
      Rectangle intermediateLayerMBR = intermediateLayer.getInputMBR();
      int xOffset = (int) Math.floor((intermediateLayerMBR.x1 - finalMBR.x1) * finalLayer.getWidth() / finalMBR.getWidth());
      int yOffset = (int) Math.floor((intermediateLayerMBR.y1 - finalMBR.y1) * finalLayer.getHeight() / finalMBR.getHeight());
      ((FrequencyMapRasterLayer)finalLayer).mergeWith(xOffset, yOffset, (FrequencyMapRasterLayer) intermediateLayer);
    }

    @Override
    public BufferedImage toImage(RasterLayer layer) {
      return ((FrequencyMapRasterLayer)layer).asImage();
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
    SingleLevelPlot.plotMapReduce(inFile, outFile, HeatMapRasterizer.class, params);
//    SingleLevelPlot.plotLocal(inFile, outFile, HeatMapRasterizer.class, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }
}
