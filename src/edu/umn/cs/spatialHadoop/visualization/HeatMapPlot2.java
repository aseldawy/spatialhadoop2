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
    /**Total width of the image to generate in pixels*/
    private int width;
    /**Total height of the image to generate in pixels*/
    private int height;
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
      this.width = conf.getInt("width", 1000);
      this.height = conf.getInt("width", 1000);
      this.color1 = OperationsParams.getColor(conf, "color1", new Color(0, 0, 255, 0));
      this.color2 = OperationsParams.getColor(conf, "color2", new Color(255, 0, 0, 255));
      this.gradientType = conf.get("gradient", "hsb").equals("hsb") ? GradientType.GT_HSB : GradientType.GT_RGB;
    }
    
    @Override
    public RasterLayer create(int width, int height) {
      FrequencyMapRasterLayer rasterLayer = new FrequencyMapRasterLayer(0, 0, width, height, radius, smoothType);
      rasterLayer.setGradientInfor(color1, color2, gradientType);
      return rasterLayer;
      
    }

    @Override
    public RasterLayer rasterize(Rectangle inputMBR, int imageWidth,
        int imageHeight, Rectangle partitionMBR, Iterable<Shape> shapes) {
      // Calculate the dimensions of the generated raster layer by calculating
      // the MBR in the image space and adding a buffer equal to the radius
      // Note: Do not calculate from the width and height of partition MBR
      // because it will cause round-off errors between adjacent partitions
      // which might leave gaps in the final generated image
      int rasterLayerX1 = (int) Math.floor((partitionMBR.x1 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerX2 = (int) Math.ceil((partitionMBR.x2 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerY1 = (int) Math.floor((partitionMBR.y1 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      int rasterLayerY2 = (int) Math.ceil((partitionMBR.y2 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      // Add a buffer equal to the radius of the heat map
      rasterLayerX1 = Math.max(0, rasterLayerX1 - radius);
      rasterLayerX2 = Math.min(width, rasterLayerX2 + radius);
      rasterLayerY1 = Math.max(0, rasterLayerY1 - radius);
      rasterLayerY2 = Math.min(height, rasterLayerY2 + radius);
      
      FrequencyMapRasterLayer frequencyMap = new FrequencyMapRasterLayer(
          rasterLayerX1, rasterLayerY1, rasterLayerX2 - rasterLayerX1,
          rasterLayerY2 - rasterLayerY1, radius, smoothType);
      
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
        int centerx = (int) Math.round((center.x - inputMBR.x1) * imageWidth / inputMBR.getWidth());
        int centery = (int) Math.round((center.y - inputMBR.y1) * imageHeight / inputMBR.getHeight());

        frequencyMap.addPoint(centerx, centery);
      }
      
      return frequencyMap;
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return FrequencyMapRasterLayer.class;
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
