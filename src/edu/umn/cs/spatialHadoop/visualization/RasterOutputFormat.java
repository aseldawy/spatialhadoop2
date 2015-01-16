/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * Writes raster layers to a binary output file
 * @author Ahmed Eldawy
 *
 */
public class RasterOutputFormat extends FileOutputFormat<Object, RasterLayer> {

  /**
   * Writes raster layers to a file
   * @author Ahmed Eldawy
   *
   */
  class RasterRecordWriter implements RecordWriter<Object, RasterLayer> {
    /**Progress of the output format*/
    private Progressable progress;
    /**The output file where all raster layers are written*/
    private FSDataOutputStream outFile;
    /**Rasterizer used to merge intermediate raster layers*/
    private Rasterizer rasterizer;
    /**The raster layer resulting of merging all written raster layers*/
    private RasterLayer mergedRasterLayer;

    public RasterRecordWriter(FileSystem fs, Path taskOutputPath, JobConf job,
        Progressable progress) throws IOException {
      this.progress = progress;
      this.outFile = fs.create(taskOutputPath);
      this.rasterizer = Rasterizer.getRasterizer(job);
      int imageWidth = job.getInt("width", 1000);
      int imageHeight = job.getInt("height", 1000);
      Rectangle inputMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
      this.mergedRasterLayer = rasterizer.createRaster(imageWidth, imageHeight, inputMBR);
    }

    @Override
    public void write(Object dummy, RasterLayer r) throws IOException {
      rasterizer.merge(mergedRasterLayer, r);
      progress.progress();
    }
    
    @Override
    public void close(Reporter reporter) throws IOException {
      // Write the merged raster layer
      mergedRasterLayer.write(outFile);
      outFile.close();
    }
  }
  
  @Override
  public RecordWriter<Object, RasterLayer> getRecordWriter(
      FileSystem fs, JobConf job, String name, Progressable progress)
      throws IOException {
    Path taskOutputPath = getTaskOutputPath(job, name);
    return new RasterRecordWriter(fs, taskOutputPath, job, progress);
  }

}
