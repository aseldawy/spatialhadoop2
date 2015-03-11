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

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.operations.Aggregate.MinMax;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.visualization.MultilevelPlot;
import edu.umn.cs.spatialHadoop.visualization.RasterLayer;
import edu.umn.cs.spatialHadoop.visualization.Rasterizer;
import edu.umn.cs.spatialHadoop.visualization.SingleLevelPlot;

/**
 * Draws a heat map for a NASA dataset
 * @author Ahmed Eldawy
 *
 */
public class HDFPlot2 {

  private static final String WATER_MASK_PATH = "water_mask";

  /***
   * Rasterizes HDF files as heat map images.
   * @author Ahmed Eldawy
   *
   */
  public static class HDFRasterizer extends Rasterizer {

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

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.color1 = OperationsParams.getColor(conf, "color1", new Color(0, 0, 255, 0));
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
          waterMaskPath = new Path(conf.get(WATER_MASK_PATH));
          waterMaskFS = waterMaskPath.getFileSystem(conf);
        } catch (IOException e) {
          e.printStackTrace();
        }
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
      
      ImageIO.write(img, "png", out);
    }
  }
  
  public static class HDFRasterizeWaterMask extends HDFRasterizer {
    @Override
    public void writeImage(RasterLayer layer, DataOutputStream out,
        boolean vflip) throws IOException {
      HDFRasterLayer hdfLayer = ((HDFRasterLayer)layer);
      BitArray bits = new BitArray((long)hdfLayer.getWidth() * hdfLayer.getHeight());
      for (int x = 0; x < hdfLayer.getWidth(); x++) {
        for (int y = 0; y < hdfLayer.getHeight(); y++) {
          long sum = hdfLayer.getSum(x, y);
          long count = hdfLayer.getCount(x, y);
          if (sum < count / 2) {
            bits.set(y * hdfLayer.getWidth() + x, false);
          } else {
            bits.set(y * hdfLayer.getWidth() + x, true);
          }
        }
      }
      // Write the bit array to the output
      bits.write(out);
    }
  }
  
  /**
   * Draws a scale used with the heat map
   * @param output
   * @param valueRange
   * @param width
   * @param height
   * @throws IOException
   */
  public static void drawScale(Path output, MinMax valueRange, int width, int height) throws IOException {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    Graphics2D g = image.createGraphics();
    g.setBackground(Color.BLACK);
    g.clearRect(0, 0, width, height);

    // fix this part to work according to color1, color2 and gradient type
    for (int y = 0; y < height; y++) {
      Color color = NASARectangle.calculateColor(y);
      g.setColor(color);
      g.drawRect(width * 3 / 4, y, width / 4, 1);
    }

    int fontSize = 24;
    g.setFont(new Font("Arial", Font.BOLD, fontSize));
    int step = (valueRange.maxValue - valueRange.minValue) * fontSize * 10 / height;
    step = (int) Math.pow(10, Math.round(Math.log10(step)));
    int min_value = valueRange.minValue / step * step;
    int max_value = valueRange.maxValue / step * step;

    for (int value = min_value; value <= max_value; value += step) {
      int y = fontSize + (height - fontSize) - value * (height - fontSize) /
          (valueRange.maxValue - valueRange.minValue);
      g.setColor(Color.WHITE);
      g.drawString(String.valueOf(value), 5, y);
    }

    g.dispose();

    FileSystem fs = output.getFileSystem(new Configuration());
    FSDataOutputStream outStream = fs.create(output, true);
    ImageIO.write(image, "png", outStream);
    outStream.close();
  }

  /**
   * Plot a water mask for a region and store the result in a binary format.
   * @param inFiles
   * @param outFile
   * @param params
   * @return
   * @throws IOException
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static Job plotWaterMask(Path[] inFiles, Path outFile,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    // Restrict to HDF files if working on a directory
    for (int i = 0; i < inFiles.length; i++) {
      if (!inFiles[i].getName().toLowerCase().endsWith(".hdf"))
        inFiles[i] = new Path(inFiles[i], "*.hdf");
    }
    params.setBoolean("recoverholes", false);
    params.set("recover", "none");
    if (params.getBoolean("pyramid", false))
      return MultilevelPlot.plot(inFiles, outFile, HDFRasterizeWaterMask.class, params);
    else
      return SingleLevelPlot.plot(inFiles, outFile, HDFRasterizeWaterMask.class, params);
  }
  
  public static Job plotHeatMap(Path[] inFiles, Path outFile,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
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
      if (params.get(WATER_MASK_PATH) == null) {
        // TODO if not set, create it first using a separate job with
        // HDFRasterizerWaterMask
        throw new RuntimeException("You need to set 'water_mask' to recover holes on write");
      }
    }
    if (params.getBoolean("pyramid", false))
      return MultilevelPlot.plot(inFiles, outFile, HDFRasterizer.class, params);
    else
      return SingleLevelPlot.plot(inFiles, outFile, HDFRasterizer.class, params);
  }

  private static void printUsage() {
    System.out.println("Plots NASA data in HDFS files");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("width:<w> - Maximum width of the image (1000)");
    System.out.println("height:<h> - Maximum height of the image (1000)");
    System.out.println("partition:<data|space> - whether to use data partitioning (default) or space partitioning");
    System.out.println("valuerange:<v1..v2> - Range of values for the generated heat map");
    System.out.println("color1:<c1> - The color associated with v1");
    System.out.println("color2:<c2> - The color associated with v2");
    System.out.println("gradient:<rgb|hsb> - Type of gradient to use");
    System.out.println("recover:<read|write|none> - (none) How to recover holes in the data");
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

    Path[] inFiles = params.getInputPaths();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    plotHeatMap(inFiles, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }
}
