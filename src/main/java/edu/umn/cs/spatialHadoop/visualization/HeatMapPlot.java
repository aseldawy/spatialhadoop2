/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.visualization.FrequencyMap.GradientType;

/**
 * @author Ahmed Eldawy
 *
 */
public class HeatMapPlot {

  public static class HeatMapRasterizer extends Plotter {

    /**Radius of the heat map smooth in pixels*/
    private int radius;
    /**Type of smoothing to use in the frequency map*/
    private FrequencyMap.SmoothType smoothType;
    /**Color associated with minimum value*/
    private Color color1;
    /**Color associated with maximum value*/
    private Color color2;
    /**Type of gradient to use between minimum and maximum values*/
    private GradientType gradientType;
    
    /**Minimum and maximum values to be used while drawing the heat map*/
    private float minValue, maxValue;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.radius = conf.getInt("radius", 5);
      this.smoothType = conf.getBoolean("smooth", true) ? FrequencyMap.SmoothType.Gaussian
          : FrequencyMap.SmoothType.Flat;
      this.color1 = OperationsParams.getColor(conf, "color1", new Color(0, 0, 255, 0));
      this.color2 = OperationsParams.getColor(conf, "color2", new Color(255, 0, 0, 255));
      this.gradientType = conf.get("gradient", "hsb").equals("hsb") ? GradientType.GT_HSB : GradientType.GT_RGB;
      String rangeStr = conf.get("valuerange");
      if (rangeStr != null) {
        String[] parts = rangeStr.split(",");
        this.minValue = Float.parseFloat(parts[0]);
        this.maxValue = Float.parseFloat(parts[1]);
      } else {
        this.minValue = 0;
        this.maxValue = -1;
      }
    }
    
    @Override
    public Canvas createCanvas(int width, int height, Rectangle mbr) {
      FrequencyMap rasterLayer = new FrequencyMap(mbr, width, height, radius, smoothType);
      rasterLayer.setGradientInfor(color1, color2, gradientType);
      if (this.minValue <= maxValue)
        rasterLayer.setValueRange(minValue, maxValue);
      return rasterLayer;
    }

    @Override
    public void plot(Canvas canvasLayer, Shape shape) {
      FrequencyMap frequencyMap = (FrequencyMap) canvasLayer;
      Point center;
      if (shape instanceof Point) {
        center = (Point) shape;
      } else if (shape instanceof Rectangle) {
        center = ((Rectangle) shape).getCenterPoint();
      } else {
        Rectangle shapeMBR = shape.getMBR();
        if (shapeMBR == null)
          return;
        center = shapeMBR.getCenterPoint();
      }
      Rectangle inputMBR = canvasLayer.getInputMBR();
      int centerx = (int) Math.round((center.x - inputMBR.x1) * canvasLayer.getWidth() / inputMBR.getWidth());
      int centery = (int) Math.round((center.y - inputMBR.y1) * canvasLayer.getHeight() / inputMBR.getHeight());

      frequencyMap.addPoint(centerx, centery);
    }

    @Override
    public Class<? extends Canvas> getCanvasClass() {
      return FrequencyMap.class;
    }

    @Override
    public void merge(Canvas finalLayer,
        Canvas intermediateLayer) {
      ((FrequencyMap)finalLayer).mergeWith((FrequencyMap) intermediateLayer);
    }

    @Override
    public void writeImage(Canvas layer, DataOutputStream out,
        boolean vflip) throws IOException {
      BufferedImage img =  ((FrequencyMap)layer).asImage();
      // Flip image vertically if needed
      if (vflip) {
        AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
        tx.translate(0, -img.getHeight());
        AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        img = op.filter(img, null);
      }
      
      ImageIO.write(img, "png", out);
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
    System.out.println("partition:<data|space|flat|pyramid> - which partitioning technique to use");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  public static Job plot(Path[] inFiles, Path outFile, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    if (params.getBoolean("pyramid", false)) {
      return MultilevelPlot.plot(inFiles, outFile, HeatMapRasterizer.class, params);
    } else {
      return SingleLevelPlot.plot(inFiles, outFile, HeatMapRasterizer.class, params);
    }
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
    plot(inFiles, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }
}
