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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
  class ImageRecordWriter extends RecordWriter<Object, RasterLayer> {
    /**Rasterizer used to merge intermediate raster layers*/
    private Rasterizer rasterizer;
    private Path outPath;
    private FileSystem outFS;
    private int rasterLayersWritten;
    private boolean vflip;
    /**The associated reduce task. Used to report progress*/
    private TaskAttemptContext task;

    public ImageRecordWriter(FileSystem fs, Path taskOutputPath,
        TaskAttemptContext task) throws IOException {
      Configuration conf = task.getConfiguration();
      this.task = task;
      this.rasterizer = Rasterizer.getRasterizer(conf);
      this.outPath = taskOutputPath;
      this.outFS = this.outPath.getFileSystem(conf);
      this.rasterLayersWritten = 0;
      this.vflip = conf.getBoolean("vflip", true);
    }

    @Override
    public void write(Object dummy, RasterLayer r) throws IOException {
      String suffix = String.format("-%05d.png", rasterLayersWritten++);
      Path p = new Path(outPath.getParent(), outPath.getName()+suffix);
      FSDataOutputStream outFile = outFS.create(p);
      // Write the merged raster layer
      rasterizer.writeImage(r, outFile, this.vflip);
      outFile.close();
      task.progress();
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      
    }
  }
  
  @Override
  public RecordWriter<Object, RasterLayer> getRecordWriter(
      TaskAttemptContext task) throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(task, "");
    FileSystem fs = file.getFileSystem(task.getConfiguration());
    return new ImageRecordWriter(fs, file, task);
  }

}
