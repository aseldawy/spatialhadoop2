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

/**
 * An output format that is used to plot ImageWritable to PNG image.
 * @author Ahmed Eldawy
 *
 */
public class PyramidOutputFormat2 extends FileOutputFormat<TileIndex, RasterLayer> {
  
  static class ImageRecordWriter implements RecordWriter<TileIndex, RasterLayer> {

    private Rasterizer rasterizer;
    private final FileSystem outFS;
    private final Path outPath;
    /**Number of raster layers written so far*/
    private int rasterLayersWritten;
    private boolean vflip;
    /**Used to indicate progress to Hadoop*/
    private Progressable progress;
    
    ImageRecordWriter(FileSystem outFs, Path taskOutPath, JobConf job,
        Progressable progress) {
      System.setProperty("java.awt.headless", "true");
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.outPath = taskOutPath;
      this.outFS = outFs;
      this.vflip = job.getBoolean("vflip", true);
      this.progress = progress;
    }

    @Override
    public void write(TileIndex tileIndex, RasterLayer r) throws IOException {
      String suffix = rasterLayersWritten == 0? ".png" :
        String.format("-%05d.png", rasterLayersWritten);
      Path p = new Path(outPath.getParent(), outPath.getName()+suffix);
      FSDataOutputStream outFile = outFS.create(p);
      
      // Write the merged raster layer
      rasterizer.writeImage(r, outFile, this.vflip);
      outFile.close();
      progress.progress();

      rasterLayersWritten++;
    }


    @Override
    public void close(Reporter reporter) throws IOException {
    }
  }
  
  @Override
  public RecordWriter<TileIndex, RasterLayer> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    
    Path file = FileOutputFormat.getTaskOutputPath(job, name).getParent();
    FileSystem fs = file.getFileSystem(job);

    return new ImageRecordWriter(fs, file, job, progress);
  }
}
