/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
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

/**
 * Writes raster layers as images to the output file
 * @author Ahmed Eldawy
 *
 */
public class ImageOutputFormat extends FileOutputFormat<Object, RasterLayer> {

  /**
   * Writes raster layers to a file
   * @author Ahmed Eldawy
   *
   */
  class ImageRecordWriter implements RecordWriter<Object, RasterLayer> {
    /**Progress of the output format*/
    private Progressable progress;
    /**Rasterizer used to merge intermediate raster layers*/
    private Rasterizer rasterizer;
    private Path outPath;
    private FileSystem outFS;
    private int rasterLayersWritten;
    private boolean vflip;

    public ImageRecordWriter(FileSystem fs, Path taskOutputPath, JobConf job,
        Progressable progress) throws IOException {
      this.progress = progress;
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.outPath = taskOutputPath;
      this.outFS = this.outPath.getFileSystem(job);
      this.rasterLayersWritten = 0;
      this.vflip = job.getBoolean("vflip", true);
    }

    @Override
    public void write(Object dummy, RasterLayer r) throws IOException {
      String suffix = String.format("-%05d.png", rasterLayersWritten++);
      Path p = new Path(outPath.getParent(), outPath.getName()+suffix);
      FSDataOutputStream outFile = outFS.create(p);
      // Write the merged raster layer
      rasterizer.writeImage(r, outFile, this.vflip);
      outFile.close();
      progress.progress();
    }
    
    @Override
    public void close(Reporter reporter) throws IOException {
    }
  }
  
  @Override
  public RecordWriter<Object, RasterLayer> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    Path taskOutputPath = getTaskOutputPath(job, name);
    FileSystem fs = taskOutputPath.getFileSystem(job);
    return new ImageRecordWriter(fs, taskOutputPath, job, progress);
  }

}
