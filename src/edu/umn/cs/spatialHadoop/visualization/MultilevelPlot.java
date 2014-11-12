/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.operations.PyramidPlot.TileIndex;

/**
 * Generates a multilevel image
 * @author Ahmed Eldawy
 *
 */
public class MultilevelPlot {
  
  public static class DataPartitionMap extends MapReduceBase 
    implements Mapper<Rectangle, Iterable<? extends Shape>, TileIndex, ImageWritable> {

    /**Configuration entry for input MBR*/
    private static final String InputMBR = "mbr";
    
    /**Minimum and maximum levels of the pyramid to plot (inclusive and zero-based)*/
    private int minLevel, maxLevel;
    
    /**The grid at the bottom level (i.e., maxLevel)*/
    private GridInfo bottomGrid;

    /**The MBR of the input area to draw*/
    private Rectangle inputMBR;

    /**The rasterizer associated with this job*/
    private Rasterizer rasterizer;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      String[] strLevels = job.get("levels", "7").split("\\.\\.");
      if (strLevels.length == 1) {
        minLevel = 0;
        maxLevel = Integer.parseInt(strLevels[0]);
      } else {
        minLevel = Integer.parseInt(strLevels[0]);
        maxLevel = Integer.parseInt(strLevels[1]);
      }
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      bottomGrid = new GridInfo(inputMBR.x1, inputMBR.y1, inputMBR.x2, inputMBR.y2);
      bottomGrid.rows = bottomGrid.columns = 1 << maxLevel;
      this.rasterizer = SingleLevelPlot.getRasterizer(job);
      this.rasterizer.configure(job);
    }
    
    @Override
    public void map(Rectangle inMBR, Iterable<? extends Shape> shapes,
        OutputCollector<TileIndex, ImageWritable> output, Reporter reporter)
        throws IOException {
      TileIndex key = new TileIndex();
      Map<TileIndex, RasterLayer> rasterLayers = new HashMap<TileIndex, RasterLayer>();
      for (Shape shape : shapes) {
        Rectangle shapeMBR = shape.getMBR();
        if (shapeMBR == null)
          continue;
        java.awt.Rectangle overlappingCells =
            bottomGrid.getOverlappingCells(shapeMBR);
        // Iterate over levels from bottom up
        for (key.level = maxLevel; key.level >= minLevel; key.level--) {
          for (key.x = overlappingCells.x; key.x < overlappingCells.x + overlappingCells.width; key.x++) {
            for (key.y = overlappingCells.y; key.y < overlappingCells.y + overlappingCells.height; key.y++) {
              RasterLayer rasterLayer = rasterLayers.get(key);
              if (rasterLayer == null) {
                rasterLayer = rasterizer.create(width, height)
              }
            }
          }
        }
      }
    }
    
  }


}
