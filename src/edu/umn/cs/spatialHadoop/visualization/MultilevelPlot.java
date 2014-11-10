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

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.ImageWritable;
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

    @Override
    public void map(Rectangle inMBR, Iterable<? extends Shape> shapes,
        OutputCollector<TileIndex, ImageWritable> output, Reporter reporter)
        throws IOException {
      Map<TileIndex, RasterLayer> rasterLayers = new HashMap<TileIndex, RasterLayer>();
    }
    
  }


}
