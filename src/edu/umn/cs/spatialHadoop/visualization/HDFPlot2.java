/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.nasa.NASARectangle;
import edu.umn.cs.spatialHadoop.nasa.NASAShape;

/**
 * Draws a heat map for a NASA dataset
 * @author Ahmed Eldawy
 *
 */
public class HDFPlot2 {

  public static class HDFRasterizer extends Rasterizer {

    /**Color associated with minimum value*/
    private Color color1;
    /**Color associated with maximum value*/
    private Color color2;
    /**Type of gradient to use between minimum and maximum values*/
    private HDFRasterLayer.GradientType gradientType;
    
    /**Minimum and maximum values to be used while drawing the heat map*/
    private float minValue, maxValue;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.color1 = OperationsParams.getColor(conf, "color1", new Color(0, 0, 255, 0));
      this.color2 = OperationsParams.getColor(conf, "color2", new Color(255, 0, 0, 255));
      this.gradientType = conf.get("gradient", "hsb").equals("hsb") ?
          HDFRasterLayer.GradientType.GT_HSB : HDFRasterLayer.GradientType.GT_RGB;
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
    public RasterLayer createRaster(int width, int height, Rectangle mbr) {
      HDFRasterLayer rasterLayer = new HDFRasterLayer(mbr, width, height);
      rasterLayer.setGradientInfor(color1, color2, gradientType);
      if (this.minValue <= maxValue)
        rasterLayer.setValueRange(minValue, maxValue);
      return rasterLayer;
    }

    @Override
    public void rasterize(RasterLayer rasterLayer, Shape shape) {
      HDFRasterLayer hdfMap = (HDFRasterLayer) rasterLayer;
      double x, y;
      if (shape instanceof Point) {
        Point np = (Point) shape;
        x = np.x;
        y = np.y;
      } else if (shape instanceof Rectangle) {
        Rectangle r = (Rectangle) shape;
        x = (r.x1 + r.x2)/2;
        y = (r.y1 + r.y2)/2;
      } else {
        Rectangle r = shape.getMBR();
        x = (r.x1 + r.x2)/2;
        y = (r.y1 + r.y2)/2;
      }
      
      Rectangle inputMBR = rasterLayer.getInputMBR();
      int centerx = (int) Math.round((x - inputMBR.x1) * rasterLayer.getWidth() / inputMBR.getWidth());
      int centery = (int) Math.round((y - inputMBR.y1) * rasterLayer.getHeight() / inputMBR.getHeight());

      hdfMap.addPoint(centerx, centery, ((NASAShape)shape).getValue());
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return HDFRasterLayer.class;
    }

    @Override
    public void merge(RasterLayer finalLayer,
        RasterLayer intermediateLayer) {
      ((HDFRasterLayer)finalLayer).mergeWith((HDFRasterLayer) intermediateLayer);
    }

    @Override
    public void writeImage(RasterLayer layer, DataOutputStream out,
        boolean vflip) throws IOException {
      BufferedImage img =  ((HDFRasterLayer)layer).asImage();
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
    System.out.println("Plots NASA data in HDFS files");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("width:<w> - Maximum width of the image (1000)");
    System.out.println("height:<h> - Maximum height of the image (1000)");
    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("partition:<data|space> - whether to use data partitioning (default) or space partitioning");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    
    if (params.get("shape") == null) {
      // Set the default shape value
      params.setClass("shape", NASARectangle.class, Shape.class);
    } else if (!(params.getShape("shape") instanceof NASAShape)) {
      System.err.println("The specified shape "+params.get("shape")+" in not an instance of NASAShape");
      System.exit(1);
    }

    Path inFile = params.getInputPath();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    if (params.getBoolean("pyramid", false)) {
      MultilevelPlot.plot(inFile, outFile, HDFRasterizer.class, params);
    } else {
      SingleLevelPlot.plot(inFile, outFile, HDFRasterizer.class, params);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }
}
