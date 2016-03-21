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
import java.awt.Graphics;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

/**
 * @author Ahmed Eldawy
 *
 */
public class GeometricPlot {
  
  public static class GeometricRasterizer extends Plotter {
    
    private Color strokeColor;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.strokeColor = OperationsParams.getColor(conf, "color", Color.BLACK);
    }

    @Override
    public Canvas createCanvas(int width, int height, Rectangle mbr) {
      ImageCanvas imageCanvas = new ImageCanvas(mbr, width, height);
      imageCanvas.setColor(strokeColor);
      return imageCanvas;
    }

    @Override
    public void plot(Canvas canvasLayer, Shape shape) {
      ImageCanvas imgLayer = (ImageCanvas) canvasLayer;
      imgLayer.drawShape(shape);
    }

    @Override
    public Class<? extends Canvas> getCanvasClass() {
      return ImageCanvas.class;
    }

    @Override
    public void merge(Canvas finalLayer,
        Canvas intermediateLayer) {
      ((ImageCanvas)finalLayer).mergeWith((ImageCanvas) intermediateLayer);
    }

    @Override
    public void writeImage(Canvas layer, DataOutputStream out,
        boolean vflip) throws IOException {
      BufferedImage img =  ((ImageCanvas)layer).getImage();
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
  
  /**
   * @param inFiles
   * @param outFile
   * @param params
   * @throws IOException
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static Job plot(Path[] inFiles, Path outFile, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    if (params.getBoolean("pyramid", false)) {
      return MultilevelPlot.plot(inFiles, outFile, GeometricRasterizer.class, params);
    } else {
      return SingleLevelPlot.plot(inFiles, outFile, GeometricRasterizer.class, params);
    }
  }
  
  /**
   * Combines images of different datasets into one image that is displayed
   * to users.
   * This method is called from the web interface to display one image for
   * multiple selected datasets.
   * @param conf
   * @param files Paths to directories which contains the datasets
   * @param includeBoundaries Also plot the indexing boundaries of datasets
   * @param width
   * @param height
   * @return An image that is the combination of all datasets images
   * @throws IOException
   * @throws InterruptedException
   */
  public static BufferedImage combineImages(Configuration conf, Path[] files,
      boolean includeBoundaries, int width, int height) throws IOException, InterruptedException {
    BufferedImage result;
    // Retrieve the MBRs of all datasets
    Rectangle allMbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Path file : files) {
      Rectangle mbr = FileMBR.fileMBR(file, new OperationsParams(conf));
      allMbr.expand(mbr);
    }

    // Adjust width and height to maintain aspect ratio
    if ((allMbr.x2 - allMbr.x1) / (allMbr.y2 - allMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((allMbr.y2 - allMbr.y1) * width / (allMbr.x2 - allMbr.x1));
    } else {
      width = (int) ((allMbr.x2 - allMbr.x1) * height / (allMbr.y2 - allMbr.y1));
    }
    result = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);


    for (Path file : files) {
      FileSystem fs = file.getFileSystem(conf);
      if (fs.getFileStatus(file).isDir()) {
        // Retrieve the MBR of this dataset
        Rectangle mbr = FileMBR.fileMBR(file, new OperationsParams(conf));
        // Compute the coordinates of this image in the whole picture
        mbr.x1 = (mbr.x1 - allMbr.x1) * width / allMbr.getWidth();
        mbr.x2 = (mbr.x2 - allMbr.x1) * width / allMbr.getWidth();
        mbr.y1 = (mbr.y1 - allMbr.y1) * height / allMbr.getHeight();
        mbr.y2 = (mbr.y2 - allMbr.y1) * height / allMbr.getHeight();
        // Retrieve the image of this dataset
        Path imagePath = new Path(file, "_data.png");
        if (!fs.exists(imagePath))
          throw new RuntimeException("Image "+imagePath+" not ready");
        FSDataInputStream imageFile = fs.open(imagePath);
        BufferedImage image = ImageIO.read(imageFile);
        imageFile.close();
        // Draw the image
        Graphics graphics = result.getGraphics();
        graphics.drawImage(image, (int) mbr.x1, (int) mbr.y1,
            (int) mbr.getWidth(), (int) mbr.getHeight(), null);
        graphics.dispose();

        if (includeBoundaries) {
          // Plot also the image of the boundaries
          // Retrieve the image of the dataset boundaries
          imagePath = new Path(file, "_partitions.png");
          if (fs.exists(imagePath)) {
            imageFile = fs.open(imagePath);
            image = ImageIO.read(imageFile);
            imageFile.close();
            // Draw the image
            graphics = result.getGraphics();
            graphics.drawImage(image, (int) mbr.x1, (int) mbr.y1,
                (int) mbr.getWidth(), (int) mbr.getHeight(), null);
            graphics.dispose();
          }
        }
      }
    }

    return result;
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
