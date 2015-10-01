/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.visualization.MultilevelPlot;
import edu.umn.cs.spatialHadoop.visualization.Canvas;
import edu.umn.cs.spatialHadoop.visualization.Plotter;
import edu.umn.cs.spatialHadoop.visualization.SingleLevelPlot;

/**
 * Draws a heat map for a NASA dataset
 * @author Ahmed Eldawy
 *
 */
public class HDFPlot {
  static final Log LOG = LogFactory.getLog(HDFPlot.class);

  /**Configuration line for setting a file that contains water_mask*/
  public static final String PREPROCESSED_WATERMARK = "water_mask";

  /**
   * Rasterizes HDF files as heat map images.
   * @author Ahmed Eldawy
   *
   */
  public static class HDFRasterizer extends Plotter {

    /**Color associated with minimum value*/
    private Color color1;
    /**Color associated with maximum value*/
    private Color color2;
    /**Type of gradient to use between minimum and maximum values*/
    private HDFRasterLayer.GradientType gradientType;
    
    /**Minimum and maximum values to be used while drawing the heat map*/
    private float minValue, maxValue;
    /**Path of the water mask if we need to recover on write*/
    private Path waterMaskPath;
    /**FileSystem of the water mask*/
    private FileSystem waterMaskFS;
    /**The date format if we need to add date to the generated image*/
    private SimpleDateFormat dateFormat;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.color1 = OperationsParams.getColor(conf, "color1", new Color(0, 0, 255, 255));
      this.color2 = OperationsParams.getColor(conf, "color2", new Color(255, 0, 0, 255));
      this.gradientType = conf.get("gradient", "hsb").equals("hsb") ?
          HDFRasterLayer.GradientType.GT_HSB : HDFRasterLayer.GradientType.GT_RGB;
      String rangeStr = conf.get("valuerange");
      if (rangeStr != null) {
        String[] parts = rangeStr.split("\\.\\.");
        this.minValue = Float.parseFloat(parts[0]);
        this.maxValue = Float.parseFloat(parts[1]);
      } else {
        this.minValue = 0;
        this.maxValue = -1;
      }
      if (conf.get("recover", "none").equals("write")) {
        try {
          waterMaskPath = new Path(conf.get(PREPROCESSED_WATERMARK));
          waterMaskFS = waterMaskPath.getFileSystem(conf);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (conf.getBoolean("adddate", false)) {
        dateFormat = new SimpleDateFormat(conf.get("dateformat", "dd-MM-yyyy"));
      }
    }
    
    @Override
    public Canvas createCanvas(int width, int height, Rectangle mbr) {
      HDFRasterLayer rasterLayer = new HDFRasterLayer(mbr, width, height);
      rasterLayer.setGradientInfo(color1, color2, gradientType);
      if (this.minValue <= maxValue)
        rasterLayer.setValueRange(minValue, maxValue);
      return rasterLayer;
    }
    
    @Override
    public void plot(Canvas layer, Iterable<? extends Shape> shapes) {
      Iterator<? extends Shape> iShapes = shapes.iterator();
      if (!iShapes.hasNext())
        return;
      // Retrieve timestamp
      Shape s = iShapes.next();
      ((HDFRasterLayer)layer).setTimestamp(((NASAShape)s).getTimestamp());
      this.plot(layer, s);
      while (iShapes.hasNext()) {
        this.plot(layer, iShapes.next());
      }
    }
    
    @Override
    public void plot(Canvas canvasLayer, Shape shape) {
      HDFRasterLayer hdfMap = (HDFRasterLayer) canvasLayer;
      int x1, y1, x2, y2;
      Rectangle inputMBR = canvasLayer.getInputMBR();
      if (shape instanceof NASAPoint) {
        NASAPoint p = (NASAPoint) shape;
        x1 = (int) Math.round((p.x - inputMBR.x1) * canvasLayer.getWidth() / inputMBR.getWidth());
        y1 = (int) Math.round((p.y - inputMBR.y1) * canvasLayer.getHeight() / inputMBR.getHeight());
        hdfMap.addPoints(x1, y1, 1, 1, p.getValue());
      } else if (shape instanceof NASARectangle) {
        NASARectangle r = (NASARectangle) shape;
        x1 = (int) Math.round((r.x1 - inputMBR.x1) * canvasLayer.getWidth() / inputMBR.getWidth());
        y1 = (int) Math.round((r.y1 - inputMBR.y1) * canvasLayer.getHeight() / inputMBR.getHeight());
        x2 = (int) Math.ceil((r.x2 - inputMBR.x1) * canvasLayer.getWidth() / inputMBR.getWidth());
        y2 = (int) Math.ceil((r.y2 - inputMBR.y1) * canvasLayer.getHeight() / inputMBR.getHeight());
        hdfMap.addPoints(x1, y1, x2, y2, r.getValue());
      } else {
        throw new RuntimeException("Cannot parse shapes of type "+shape.getClass());
      }

    }

    @Override
    public Class<? extends Canvas> getCanvasClass() {
      return HDFRasterLayer.class;
    }

    @Override
    public void merge(Canvas finalLayer,
        Canvas intermediateLayer) {
      ((HDFRasterLayer)finalLayer).mergeWith((HDFRasterLayer) intermediateLayer);
    }

    @Override
    public void writeImage(Canvas layer, DataOutputStream out,
        boolean vflip) throws IOException {
      HDFRasterLayer hdfLayer = (HDFRasterLayer)layer;
      if (waterMaskPath != null) {
        // Recover holes on write
        FSDataInputStream waterMaskFile = waterMaskFS.open(waterMaskPath);
        BitArray bitMask = new BitArray();
        bitMask.readFields(waterMaskFile);
        waterMaskFile.close();
        hdfLayer.recoverHoles(bitMask);
      }
      BufferedImage img =  hdfLayer.asImage();
      // Flip image vertically if needed
      if (vflip) {
        AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
        tx.translate(0, -img.getHeight());
        AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        img = op.filter(img, null);
      }
      
      if (dateFormat != null) {
        long timestamp = hdfLayer.getTimestamp();
        String strDate = dateFormat.format(timestamp);
        final int fontSize = 48;
        Graphics2D g = img.createGraphics();
        g.setFont(new Font("Arial", Font.BOLD, fontSize));
        g.setColor(Color.BLACK);
        g.drawString(strDate, 10, img.getHeight() - 10);
        g.setColor(Color.WHITE);
        g.drawString(strDate, 10-2, img.getHeight() - 10 - 2);
        g.dispose();
      }
      ImageIO.write(img, "png", out);
    }
  }
  
  public static class HDFRasterizeWaterMask extends HDFRasterizer {
    @Override
    public void writeImage(Canvas layer, DataOutputStream out,
        boolean vflip) throws IOException {
      HDFRasterLayer hdfLayer = ((HDFRasterLayer)layer);
      BitArray bits = new BitArray((long)hdfLayer.getWidth() * hdfLayer.getHeight());
      for (int x = 0; x < hdfLayer.getWidth(); x++) {
        for (int y = 0; y < hdfLayer.getHeight(); y++) {
          long sum = hdfLayer.getSum(x, y);
          long count = hdfLayer.getCount(x, y);
          if (sum < count / 2) {
            // On land
            bits.set(y * hdfLayer.getWidth() + x, true);
          } else {
            // Not on land
            bits.set(y * hdfLayer.getWidth() + x, false);
          }
        }
      }
      // Write the bit array to the output
      bits.write(out);
    }
  }
  
  public static Job plotHeatMap(Path[] inFiles, Path outFile,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    if (params.get("shape") == null) {
      // Set the default shape value
      params.setClass("shape", NASARectangle.class, Shape.class);
    } else if (!(params.getShape("shape") instanceof NASAShape)) {
      System.err.println("The specified shape "+params.get("shape")+" in not an instance of NASAShape");
      System.exit(1);
    }
    
    if (params.get("mbr") == null) {
      // Set to the same value as query rectangle or the whole world
      params.set("mbr", params.get("rect", "-180,-90,180,90"));
    }

    // Restrict to HDF files if working on a directory
    for (int i = 0; i < inFiles.length; i++) {
      if (!inFiles[i].getName().toLowerCase().endsWith(".hdf"))
        inFiles[i] = new Path(inFiles[i], "*.hdf");
    }
    String recover = params.get("recover", "none").toLowerCase();
    if (recover.equals("none")) {
      // Don't recover holes
      params.setBoolean("recoverholes", false);
    } else if (recover.equals("read")) {
      // Recover holes on read
      params.setBoolean("recoverholes", true);
    } else if (recover.equals("write")) {
      // Recover holes upon writing the final image
      params.setBoolean("recoverholes", false);
      if (params.get(PREPROCESSED_WATERMARK) == null) {
        OperationsParams params2 = new OperationsParams(params);
        params2.setBoolean("background", false);
        Path wmImage = new Path(outFile.getParent(), outFile.getName()+"_WaterMask");
        generateWaterMask(wmImage, params2);
        params.set(PREPROCESSED_WATERMARK, wmImage.toString());
      }
    }
    if (params.getBoolean("pyramid", false))
      return MultilevelPlot.plot(inFiles, outFile, HDFRasterizer.class, params);
    else
      return SingleLevelPlot.plot(inFiles, outFile, HDFRasterizer.class, params);
  }

  public static void generateWaterMask(Path wmImage, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    // Need to recover holes on write but the water mask is not set,
    // need to put it first
    Path wmPath = new Path(params.get(HDFRecordReader.WATER_MASK_PATH,
        "http://e4ftl01.cr.usgs.gov/MOLT/MOD44W.005/2000.02.24/"));
    wmPath = new Path(wmPath, "*.hdf");
    params.set("recover", "none");
    params.setBoolean("recoverholes", false);
    params.set("dataset", "water_mask");
    if (params.get("shape") == null) {
      // Set the default shape value
      params.setClass("shape", NASARectangle.class, Shape.class);
    } else if (!(params.getShape("shape") instanceof NASAShape)) {
      System.err.println("The specified shape "+params.get("shape")+" in not an instance of NASAShape");
      System.exit(1);
    }
    
    if (params.get("mbr") == null) {
      // Set to the same value as query rectangle or the whole world
      params.set("mbr", params.get("rect", "-180,-90,180,90"));
    }

    SingleLevelPlot.plot(new Path[] {wmPath}, wmImage,
        HDFRasterizeWaterMask.class, params);
    FileSystem outFS = wmImage.getFileSystem(params);
    outFS.deleteOnExit(wmImage);
  }

  private static void printUsage() {
    System.out.println("Plots NASA data in HDFS files");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("width:<w> - Maximum width of the image (1000)");
    System.out.println("height:<h> - Maximum height of the image (1000)");
    System.out.println("partition:<data|space|flat|pyramid> - which partitioning technique to use");
    System.out.println("valuerange:<v1..v2> - Range of values for the generated heat map");
    System.out.println("color1:<c1> - The color associated with v1");
    System.out.println("color2:<c2> - The color associated with v2");
    System.out.println("gradient:<rgb|hsb> - Type of gradient to use");
    System.out.println("recover:<read|write|none> - (none) How to recover holes in the data");
    System.out.println("-adddate: Write the date on each generated image (false)");
    System.out.println("dateformat<df>: The format of the date to write on each image (dd-MM-yyyy)");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
    GenericOptionsParser.printGenericCommandUsage(System.out);
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
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    
    Path[] inFiles = params.getInputPaths();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    plotHeatMap(inFiles, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }
}
