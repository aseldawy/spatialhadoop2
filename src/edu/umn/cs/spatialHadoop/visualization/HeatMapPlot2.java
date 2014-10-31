/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;

/**
 * @author Ahmed Eldawy
 *
 */
public class HeatMapPlot2 {

  public static class HeatMapRasterizer extends Rasterizer {

    @Override
    public RasterLayer create(int width, int height) {
    }

    @Override
    public RasterLayer rasterize(Rectangle inputMBR, int imageWidth,
        int imageHeight, Rectangle partitionMBR, Iterable<Shape> shapes) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
