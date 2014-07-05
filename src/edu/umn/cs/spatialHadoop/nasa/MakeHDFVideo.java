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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.Aggregate.MinMax;
import edu.umn.cs.spatialHadoop.operations.GeometricPlot;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;

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
    FileStatus[] allImages = OperationsParams.isWildcard(dir) ?
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
      // Ensure even height for compatibility with some codecs
      if ((crop_y2 - crop_y1) % 2 == 1)
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
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInputOutput()) {
      System.exit(1);
    }
    
    //Path input = params.getPaths()[0];
    Path output = params.getPaths()[1];
    boolean recoverHoles = params.is("recoverholes");
    boolean addDate = params.is("adddate");

    Vector<String> vargs = new Vector<String>(Arrays.asList(args));
    Rectangle plotRange = (Rectangle) params.getShape("rect");
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
      
      int new_width = params.getInt("width", 1000) * 2;
      int new_height = params.getInt("height", 1000) * 2;
      
      vargs.add(plotRange.toText(new Text("rect:")).toString());
      vargs.add("width:"+new_width);
      vargs.add("height:"+new_height);
    }

    // 1- Call HDF plot to generate all images
    HDFPlot.main(vargs.toArray(new String[vargs.size()]));
    
    // 2- Call RecoverHoles to recover holes (surprise)
    if (recoverHoles) {
      RecoverHoles.recoverInterpolationDir(output);
      if (plotRange != null) {
        // Need to crop all images to restore original selection
        cropImages(output, (Rectangle)params.getShape("rect"), plotRange);
      }
    }
    
    if (addDate) {
      RecoverHoles.addDate(output);
    }
    
    FileSystem outFs = output.getFileSystem(params);
    FileStatus[] generatedImages = outFs.listStatus(output, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().toLowerCase().endsWith(".png");
      }
    });
    if (generatedImages.length == 0) {
      Log.warn("No generated images");
      System.exit(1);
    }
    
    InputStream inStream = outFs.open(generatedImages[0].getPath());
    BufferedImage firstImage = ImageIO.read(inStream);
    inStream.close();
    
    int imageWidth = firstImage.getWidth();
    int imageHeight = firstImage.getHeight();
    
    String scaleRangeStr = params.get("scale-range");
    if (scaleRangeStr != null) {
      String[] parts = scaleRangeStr.split("\\.\\.");
      MinMax scaleRange = new MinMax();
      scaleRange.minValue = Integer.parseInt(parts[0]);
      scaleRange.maxValue = Integer.parseInt(parts[1]);
      GeometricPlot.drawScale(new Path(output, "scale.png"), scaleRange, 64, imageHeight);
    }
    
    InputStream logoInputStream = MakeHDFVideo.class.getResourceAsStream("/gistic_logo.png");
    OutputStream logoOutputStream = outFs.create(new Path(output, "gistic_logo.png"));
    byte[] buffer = new byte[4096];
    int size = 0;
    while ((size = logoInputStream.read(buffer)) > 0) {
      logoOutputStream.write(buffer, 0, size);
    }
    logoOutputStream.close();
    
    // Rename files to be ready to use with ffmpeg
    FileStatus[] all_images = outFs.listStatus(output, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().matches("\\d+\\.\\d+\\.\\d+\\.png");
      }
    });
    
    Arrays.sort(all_images, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus f1, FileStatus f2) {
        return f1.getPath().getName().compareTo(f2.getPath().getName());
      }
    });
    
    int day = 1;
    for (FileStatus image : all_images) {
      String newFileName = String.format("day_%03d.png", day++);
      outFs.rename(image.getPath(), new Path(output, newFileName));
    }

    // Plot the overlay image
    String overlay = params.get("overlay");
    if (overlay != null) {
      vargs = new Vector<String>(Arrays.asList(args));
      // Keep all arguments except input and output which change for each call
      // to Plot or PlotPyramid
      for (int i = 0; i < vargs.size();) {
        if (vargs.get(i).startsWith("-") && vargs.get(i).length() > 1) {
          i++; // Keep this argument
        } else if (vargs.get(i).indexOf(':') != -1 && vargs.get(i).indexOf(":/") == -1) {
          if (vargs.get(i).toLowerCase().startsWith("scale:")
              || vargs.get(i).startsWith("shape:")
              || vargs.get(i).startsWith("dataset:")
              || vargs.get(i).startsWith("width:")
              || vargs.get(i).startsWith("height:"))
            vargs.remove(i);
          else
            i++; // Keep this argument
        } else {
          vargs.remove(i);
        }
      }
      vargs.add(overlay);
      vargs.add("width:"+imageWidth);
      vargs.add("height:"+imageHeight);
      vargs.add("-no-keep-ratio");
      vargs.add(new Path(output, "overlay.png").toString());
      vargs.add("shape:"+OSMPolygon.class.getName());
      GeometricPlot.main(vargs.toArray(new String[vargs.size()]));
    }

    String video_command;
    if (overlay != null) {
      video_command = "avconv -r 4 -i day_%3d.png "
          + "-vf \"movie=gistic_logo.png [watermark]; "
          + "movie=overlay.png [ways]; " 
          + "movie=scale.png [scale]; "
          + "[in] crop="+plotRange.getWidth()+":"+plotRange.getHeight()+"[in]; "
          + "[ways] crop="+plotRange.getWidth()+":"+plotRange.getHeight()+"[ways]; "
          + "[in][watermark] overlay=main_w-overlay_w-10:10 [mid]; "
          + "[mid][ways] overlay=0:0 [mid2]; "
          + "[mid2] pad=iw+64:ih [mid3]; "
          + "[mid3][scale] overlay=main_w-overlay_w:0 [out]\" "
          + "-r 4 -pix_fmt yuv420p output.mp4 ";
    } else {
      video_command = "avconv -r 4 -i day_%3d.png -vf "
          + "\"movie=gistic_logo.png [watermark]; "
          + "movie=scale.png [scale]; "
          + "[in][watermark] overlay=main_w-overlay_w-10:10 [mid]; "
          + "[mid] pad=iw+64:ih [mid2]; "
          + "[mid2][scale] overlay=main_w-overlay_w:0 [out]\" "
          + "-r 4 -pix_fmt yuv420p output.mp4 ";
    }
    PrintStream video_script = new PrintStream(outFs.create(new Path(output, "make_video.sh")));
    video_script.println(video_command);
    video_script.close();
  }

}
