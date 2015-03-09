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
import java.util.Vector;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;

/**
 * @author Ahmed Eldawy
 *
 */
public class RoadNetworkPlot {
  
  public static class GeometricRasterizer extends Rasterizer {
    
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
    public <S extends Shape> Iterable<S> smooth(Iterable<S> r) {
      Vector<Geometry> bufferedRoads = new Vector<Geometry>();
      for (S s : r) {
        OSMPolygon poly = (OSMPolygon) s;
        bufferedRoads.add(poly.geom.buffer(bufferPt));
      }
      
      Geometry merged = new GeometryCollection(bufferedRoads.toArray(new Geometry[bufferedRoads.size()]), new GeometryFactory());
      merged = merged.buffer(0);
      
      Vector<S> finalResult = new Vector<S>();
      finalResult.add((S) new OSMPolygon(merged));
      return finalResult;
    }

    @Override
    public RasterLayer createRaster(int width, int height, Rectangle mbr) {
      if (!vector) {
        ImageRasterLayer imageRasterLayer = new ImageRasterLayer(mbr, width, height);
        imageRasterLayer.setColor(strokeColor);
        return imageRasterLayer;
      } else {
        return new SVGRasterLayer(mbr,  width, height);
      }
    }

    @Override
    public void rasterize(RasterLayer rasterLayer, Shape shape) {
      if (!vector) {
        ImageRasterLayer imgLayer = (ImageRasterLayer) rasterLayer;
        imgLayer.drawShape(shape);
      } else {
        SVGRasterLayer svgLayer = (SVGRasterLayer) rasterLayer;
        svgLayer.drawShape(shape);
      }
    }

    @Override
    public Class<? extends RasterLayer> getRasterClass() {
      return vector? SVGRasterLayer.class : ImageRasterLayer.class;
    }

    @Override
    public void merge(RasterLayer finalLayer,
        RasterLayer intermediateLayer) {
      if (!vector ) {
        ((ImageRasterLayer)finalLayer).mergeWith((ImageRasterLayer) intermediateLayer);
      } else {
        ((SVGRasterLayer)finalLayer).mergeWith((SVGRasterLayer) intermediateLayer);
      }
    }
    
    @Override
    public int getRadius() {
      return bufferPx;
    }

    @Override
    public void writeImage(RasterLayer layer, DataOutputStream out,
        boolean vflip) throws IOException {
      if (!vector) {
        BufferedImage img =  ((ImageRasterLayer)layer).getImage();
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
        ((SVGRasterLayer)layer).writeToFile(ps);
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
      MultilevelPlot.plot(inFiles, outFile, GeometricRasterizer.class, params);
    } else {
      SingleLevelPlot.plot(inFiles, outFile, GeometricRasterizer.class, params);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }

}
