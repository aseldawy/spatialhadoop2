/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop;

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

import edu.umn.cs.spatialHadoop.operations.PlotPyramid.TileIndex;

/**
 * An output format that is used to plot ImageWritable to PNG image.
 * @author Ahmed Eldawy
 *
 */
public class PyramidOutputFormat extends FileOutputFormat<TileIndex, ImageWritable> {
  /**Used to indicate the progress*/
  private Progressable progress;
  
  class ImageRecordWriter implements RecordWriter<TileIndex, ImageWritable> {

    private final FileSystem outFs;
    private final Path out;
    
    ImageRecordWriter(Path out, FileSystem outFs) {
      System.setProperty("java.awt.headless", "true");
      this.out = out;
      this.outFs = outFs;
    }

    @Override
    public void write(TileIndex tileIndex, ImageWritable value) throws IOException {
      progress.progress();
      Path imagePath = getImageFile(tileIndex);

      BufferedImage image = value.getImage();
      OutputStream output = outFs.create(imagePath);
      ImageIO.write(image, "png", output);
      output.close();
    }

    private Path getImageFile(TileIndex tileIndex) {
      String filename = "tile_"+tileIndex.level+"_"+tileIndex.x+"-"+tileIndex.y+".png";
      return new Path(out, filename);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
    }
  }

  @Override
  public RecordWriter<TileIndex, ImageWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    this.progress = progress;
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);

    return new ImageRecordWriter(file, fs);
  }
}
