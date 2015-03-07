/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

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
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.visualization.ImageRasterLayer;
import edu.umn.cs.spatialHadoop.visualization.MultilevelPlot;
import edu.umn.cs.spatialHadoop.visualization.RasterLayer;
import edu.umn.cs.spatialHadoop.visualization.Rasterizer;
import edu.umn.cs.spatialHadoop.visualization.SingleLevelPlot;

/**
 * @author Ahmed Eldawy
 *
 */
public class GeometricPlot {
  
  public static class GeometricRasterizer extends Rasterizer {
    
    private Color strokeColor;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.strokeColor = OperationsParams.getColor(conf, "color", Color.BLACK);
    }

    @Override
    public RasterLayer createRaster(int width, int height, Rectangle mbr) {
      ImageRasterLayer imageRasterLayer = new ImageRasterLayer(mbr, width, height);
      imageRasterLayer.setColor(strokeColor);
      return imageRasterLayer;
    }

    @Override
    public void rasterize(RasterLayer rasterLayer, Shape shape) {
      ImageRasterLayer imgLayer = (ImageRasterLayer) rasterLayer;
      imgLayer.drawShape(shape);
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return ImageRasterLayer.class;
    }

    @Override
    public void merge(RasterLayer finalLayer,
        RasterLayer intermediateLayer) {
      ((ImageRasterLayer)finalLayer).mergeWith((ImageRasterLayer) intermediateLayer);
    }

    @Override
    public void writeImage(RasterLayer layer, DataOutputStream out,
        boolean vflip) throws IOException {
      BufferedImage img =  ((ImageRasterLayer)layer).getImage();
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
    System.out.println("partition:<data|space> - whether to use data partitioning (default) or space partitioning");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
    System.out.println("-fade: Use the gradual fade option");
    System.out.println("-sample: Use the daptive sample option");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  /**
   * @param inFiles
   * @param outFile
   * @param params
   * @throws IOException
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static void plot(Path[] inFiles, Path outFile, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    if (params.getBoolean("pyramid", false)) {
      MultilevelPlot.plot(inFiles, outFile, GeometricRasterizer.class, params);
    } else {
      SingleLevelPlot.plot(inFiles, outFile, GeometricRasterizer.class, params);
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
