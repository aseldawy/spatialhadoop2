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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

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
import edu.umn.cs.spatialHadoop.osm.OSMEdge;

/**
 * A visualizer that uses ImageMagick as a plotter. It is used to seamlessly
 * parallelize visualization work to distributed instances of ImageMagick.
 * @author Ahmed Eldawy
 *
 */
public class MagickPlot {

  public static class MagickPlotter extends Plotter {

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
    public void plot(Canvas canvasLayer, Iterable<? extends Shape> shapes) {
      ImageCanvas imgLayer = (ImageCanvas) canvasLayer;

      // Get the process.
      Process p;
      try {
        p = Runtime.getRuntime().exec("convert - png:-");
        OutputStream out = p.getOutputStream();
        PrintWriter pw = new PrintWriter(out);
        pw.println("push graphic-context");
        pw.println("viewbox 0 0 "+canvasLayer.getWidth() + " " + canvasLayer.getHeight());
        pw.println("push graphic-context");
        pw.println("stroke 'black'");
        for (Shape s : shapes) {
          OSMEdge edge = (OSMEdge) s;
          /*pw.println("line " + (edge.lon1 + 180) + "," + (edge.lat1 + 90) + " "
              + (edge.lon2 + 180) + "," + (edge.lat2 + 90));*/
          pw.println("line "
              + ((edge.lon1 - (canvasLayer.inputMBR.x1)) * canvasLayer.width / canvasLayer.inputMBR
                  .getWidth())
              + ","
              + ((edge.lat1 - (canvasLayer.inputMBR.y1)) * canvasLayer.height / canvasLayer.inputMBR
                  .getHeight())
              + " "
              + ((edge.lon2 - (canvasLayer.inputMBR.x1)) * canvasLayer.width / canvasLayer.inputMBR
                  .getWidth())
              + ","
              + ((edge.lat2 - (canvasLayer.inputMBR.y1)) * canvasLayer.height / canvasLayer.inputMBR
                  .getHeight()));
        }
        pw.println("pop graphic-context");
        pw.println("pop graphic-context");
        System.out.println("Finish reading the shape.");
        pw.close();
        out.close();
        System.out.println("Starting to wait.");
        // p.waitFor();
        System.out.println("Finish waiting.");
        InputStream in = p.getInputStream();
        System.out.println("Reading InputStream.");
        byte[] buffer = new byte[4096];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int readBytes;
        while ((readBytes = in.read(buffer)) >= 0) {
          baos.write(buffer, 0, readBytes);
        }
        System.out.println("Finished Reading From Loop.");
        baos.close();
        buffer = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
        BufferedImage image = ImageIO.read(bais);
        in.close();
        p.waitFor();
        
        System.out.println("Finish Reading InputStream.");
        imgLayer.image = image;
        System.out.println("Graphics Created.");
      } catch (IOException e2) {
        e2.printStackTrace();
      } catch (InterruptedException e3) {
        e3.printStackTrace();
      }
    }

    @Override
    public Class<? extends Canvas> getCanvasClass() {
      return ImageCanvas.class;
    }

    @Override
    public void merge(Canvas finalLayer, Canvas intermediateLayer) {
      ((ImageCanvas) finalLayer).mergeWith((ImageCanvas) intermediateLayer);
    }

    @Override
    public void writeImage(Canvas layer, DataOutputStream out, boolean vflip)
        throws IOException {
      BufferedImage img = ((ImageCanvas) layer).getImage();
      // Flip image vertically if needed
      if (vflip) {
        AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
        tx.translate(0, -img.getHeight());
        AffineTransformOp op = new AffineTransformOp(tx,
            AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        img = op.filter(img, null);
      }

      ImageIO.write(img, "png", out);
    }

    @Override
    public void plot(Canvas layer, Shape shape) {

    }
  }

  private static void printUsage() {
    System.out.println("Plots all shapes to an image");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out
        .println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("width:<w> - Maximum width of the image (1000)");
    System.out.println("height:<h> - Maximum height of the image (1000)");
    System.out
        .println("color:<c> - Main color used to draw the picture (black)");
    System.out
        .println("partition:<data|space|flat|pyramid> - which partitioning technique to use");
    System.out.println("-overwrite: Override output file without notice");
    System.out
        .println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
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
      return MultilevelPlot.plot(inFiles, outFile, MagickPlotter.class,
          params);
    } else {
      return SingleLevelPlot.plot(inFiles, outFile, MagickPlotter.class,
          params);
    }
  }

  /**
   * Combines images of different datasets into one image that is displayed to
   * users. This method is called from the web interface to display one image
   * for multiple selected datasets.
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
      boolean includeBoundaries, int width, int height) throws IOException,
      InterruptedException {
    BufferedImage result;
    // Retrieve the MBRs of all datasets
    Rectangle allMbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Path file : files) {
      Rectangle mbr = FileMBR.fileMBR(file, new OperationsParams(conf));
      allMbr.expand(mbr);
    }

    // Adjust width and height to maintain aspect ratio
    if ((allMbr.x2 - allMbr.x1) / (allMbr.y2 - allMbr.y1) > (double) width
        / height) {
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
          throw new RuntimeException("Image " + imagePath + " not ready");
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
  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    System.setProperty("java.awt.headless", "true");
    OperationsParams params = new OperationsParams(new GenericOptionsParser(
        args));
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path[] inFiles = params.getInputPaths();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    plot(inFiles, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in " + (t2 - t1) + " millis");
  }

}
