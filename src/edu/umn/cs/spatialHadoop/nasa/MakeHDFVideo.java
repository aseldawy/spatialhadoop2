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

package edu.umn.cs.spatialHadoop.nasa;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.Plot;

/**
 * Create a video from a range of HDF files. It works in the following steps:
 *  1. Call HDFPlot to generate all images with a wider area
 *  2. Call RecoverHoles to recover empty holes in the generated images
 *  3. Crop the images to keep only the selected area
 *  4. Add the dates
 *  5. Generate the scale
 *  6. Call ffmpeg to generate the final video
 * @author Ahmed Eldawy
 *
 */
public class MakeHDFVideo {

  /**
   * Crop all images in the given directory.
   * @param output
   * @throws IOException 
   */
  public static void cropImages(Path dir, Rectangle original, Rectangle extended) throws IOException {
    FileSystem fs = dir.getFileSystem(new Configuration());
    FileStatus[] allImages = CommandLineArguments.isWildcard(dir) ?
        fs.globStatus(dir) : fs.listStatus(dir);
    if (!extended.contains(original))
      throw new RuntimeException("Original rectangle must be totally contained in the extended rectangle. "
          + original+" is not contained in "+extended);

    for (FileStatus imageFile : allImages) {
      FSDataInputStream instream = fs.open(imageFile.getPath());
      BufferedImage img = ImageIO.read(instream);
      instream.close();
      
      int crop_x1 = (int) Math.floor((original.x1 - extended.x1) * img.getWidth() / extended.getWidth());
      int crop_y1 = (int) Math.floor((original.y1 - extended.y1) * img.getHeight() / extended.getHeight()); 
      int crop_x2 = (int) Math.ceil((original.x2 - extended.x1) * img.getWidth() / extended.getWidth());
      int crop_y2 = (int) Math.ceil((original.y2 - extended.y1) * img.getHeight() / extended.getHeight());
      if ((crop_y1 - crop_y1) % 2 == 1)
        crop_y2++;
      
      BufferedImage cropped = new BufferedImage(crop_x2 - crop_x1, crop_y2 - crop_y1, BufferedImage.TYPE_INT_ARGB);
      Graphics2D g = cropped.createGraphics();
      g.setBackground(new Color(0, true));
      g.clearRect(0, 0, cropped.getWidth(), cropped.getHeight());
      g.drawImage(img, 0, 0, cropped.getWidth(), cropped.getHeight(), crop_x1, crop_y1, crop_x2, crop_y2, null);
      g.dispose();
      
      FSDataOutputStream outstream = fs.create(imageFile.getPath(), true);
      ImageIO.write(cropped, "png", outstream);
      outstream.close();
    }
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    if (!cla.checkInputOutput(new Configuration())) {
      return;
    }
    
    Path input = cla.getPaths()[0];
    Path output = cla.getPaths()[1];
    boolean recoverHoles = cla.is("recoverholes");
    boolean addDate = cla.is("adddate");

    Vector<String> vargs = new Vector<String>(Arrays.asList(args));
    Rectangle plotRange = cla.getRectangle();
    if (plotRange != null && recoverHoles) {
      // Extend the plot range to improve the quality of RecoverHoles
      for (int i = 0; i < vargs.size();) {
        if (vargs.get(i).startsWith("rect:") || vargs.get(i).startsWith("mbr:")
            || vargs.get(i).startsWith("width:") || vargs.get(i).startsWith("height:")) {
          vargs.remove(i);
        } else {
          i++;
        }
      }
      double w = plotRange.getWidth();
      double h = plotRange.getHeight();
      plotRange = plotRange.buffer(w / 2, h / 2);
      
      int new_width = cla.getWidth(1000) * 2;
      int new_height = cla.getHeight(1000) * 2;
      
      vargs.add(plotRange.toText(new Text("rect:")).toString());
      vargs.add("width:"+new_width);
      vargs.add("height:"+new_height);
    }

    // 1- Call HDF plot to generate all images
    HDFPlot.main(vargs.toArray(new String[vargs.size()]));
    
    // 2- Call RecoverHoles to recover holes (surprise)
    if (recoverHoles) {
      RecoverHoles.recoverInterpolation(output);
      if (plotRange != null) {
        // Need to crop all images to restore original selection
        cropImages(output, cla.getRectangle(), plotRange);
      }
    }
    
    if (addDate) {
      RecoverHoles.addDate(output);
    }
    
    //Plot.drawScale(output, valueRange, width, height);
    
  }

}
