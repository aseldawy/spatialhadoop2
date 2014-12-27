/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

import javax.imageio.ImageIO;

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
public class PyramidOutputFormat2 extends FileOutputFormat<TileIndex, BufferedImage> {
  /**Used to indicate the progress*/
  private Progressable progress;
  private boolean vflip;
  
  class ImageRecordWriter implements RecordWriter<TileIndex, BufferedImage> {

    private final FileSystem outFs;
    private final Path out;
    
    ImageRecordWriter(Path out, FileSystem outFs) {
      System.setProperty("java.awt.headless", "true");
      this.out = out;
      this.outFs = outFs;
    }

    @Override
    public void write(TileIndex tileIndex, BufferedImage image) throws IOException {
      progress.progress();

      if (vflip) {
        AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
        tx.translate(0, -image.getHeight());
        AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        image = op.filter(image, null);
        tileIndex.y = ((1 << tileIndex.level) - 1) - tileIndex.y;
      }
      
      Path imagePath = new Path(out, tileIndex.getImageFileName());
      OutputStream output = outFs.create(imagePath);
      ImageIO.write(image, "png", output);
      output.close();
    }


    @Override
    public void close(Reporter reporter) throws IOException {
    }
  }
  
  @Override
  public RecordWriter<TileIndex, BufferedImage> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    this.progress = progress;
    this.vflip = job.getBoolean("vflip", true);
    Path file = FileOutputFormat.getTaskOutputPath(job, name).getParent();
    FileSystem fs = file.getFileSystem(job);

    return new ImageRecordWriter(file, fs);
  }
}
