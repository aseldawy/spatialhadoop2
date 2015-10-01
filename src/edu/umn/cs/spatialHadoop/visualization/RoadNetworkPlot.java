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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.vividsolutions.jts.geom.Geometry;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;
import edu.umn.cs.spatialHadoop.util.Progressable;

/**
 * @author Ahmed Eldawy
 *
 */
public class RoadNetworkPlot {
  
  public static class GeometricPlotter extends Plotter {
    
    private Color strokeColor;
    private double bufferPt;
    private int bufferPx;
    private boolean vector;

    @Override
    public void configure(Configuration conf) {
      super.configure(conf);
      this.strokeColor = OperationsParams.getColor(conf, "color", Color.BLACK);
      this.bufferPx = conf.getInt("buffer", 2);
      Rectangle inputMBR = OperationsParams.getShape(conf, "mbr").getMBR();
      this.bufferPt = bufferPx * inputMBR.getWidth() / conf.getInt("width", 1000);
      this.vector = conf.getBoolean("vector", true);
      Log.info("Using a buffer of size: "+bufferPt);
    }
    
    @Override
    public <S extends Shape> Iterable<S> smooth(Iterable<S> r, Rectangle dataMBR,
    		int canvasWidth, int canvasHeight) {
      List<Geometry> bufferedRoads = new ArrayList<Geometry>();
      for (S s : r) {
        OSMPolygon poly = (OSMPolygon) s;
        bufferedRoads.add(poly.geom.buffer(bufferPt));
      }
      final List<S> finalResult = new ArrayList<S>();
      try {
        SpatialAlgorithms.unionGroup(bufferedRoads.toArray(new Geometry[bufferedRoads.size()]),
            new Progressable.NullProgressable(), new ResultCollector<Geometry>() {
          @Override
          public void collect(Geometry r) {
            finalResult.add((S) new OSMPolygon(r));
          }
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
      return finalResult;
    }

    @Override
    public CanvasLayer createCanvas(int width, int height, Rectangle mbr) {
      if (!vector) {
        ImageCanvas imageCanvas = new ImageCanvas(mbr, width, height);
        imageCanvas.setColor(strokeColor);
        return imageCanvas;
      } else {
        return new SVGCanvas(mbr,  width, height);
      }
    }

    @Override
    public void plot(CanvasLayer canvasLayer, Shape shape) {
      if (!vector) {
        ImageCanvas imgLayer = (ImageCanvas) canvasLayer;
        imgLayer.drawShape(shape);
      } else {
        SVGCanvas svgLayer = (SVGCanvas) canvasLayer;
        svgLayer.drawShape(shape);
      }
    }

    @Override
    public Class<? extends CanvasLayer> getCanvasClass() {
      return vector? SVGCanvas.class : ImageCanvas.class;
    }

    @Override
    public void merge(CanvasLayer finalLayer,
        CanvasLayer intermediateLayer) {
      if (!vector ) {
        ((ImageCanvas)finalLayer).mergeWith((ImageCanvas) intermediateLayer);
      } else {
        ((SVGCanvas)finalLayer).mergeWith((SVGCanvas) intermediateLayer);
      }
    }
    
    @Override
    public int getRadius() {
      return bufferPx;
    }

    @Override
    public void writeImage(CanvasLayer layer, DataOutputStream out,
        boolean vflip) throws IOException {
      if (!vector) {
        BufferedImage img =  ((ImageCanvas)layer).getImage();
        // Flip image vertically if needed
        if (vflip) {
          AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
          tx.translate(0, -img.getHeight());
          AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
          img = op.filter(img, null);
        }
        
        ImageIO.write(img, "png", out);
      } else {
        out.flush();
        PrintStream ps = new PrintStream(out);
        ((SVGCanvas)layer).writeToFile(ps);
        ps.flush();
      }
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
    if (params.getBoolean("pyramid", false)) {
      MultilevelPlot.plot(inFiles, outFile, GeometricPlotter.class, params);
    } else {
      SingleLevelPlot.plot(inFiles, outFile, GeometricPlotter.class, params);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }

}
