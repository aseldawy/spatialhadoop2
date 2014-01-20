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
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
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

import edu.umn.cs.spatialHadoop.CommandLineArguments;

/**
 * Recovers missing data from a set of images by substituting missing points
 * from neighboring images based on time.
 * @author Ahmed Eldawy
 *
 */
public class RecoverHoles {

  /**
   * Recover all images in the given dir. The passed directory contains a set of
   * images each corresponding to a specific date. The algorithm works in
   * iterations. In the first iteration, each image is used to recover holes in
   * the image that directly follows it. For example, the valid points in the
   * image of day 1 is used to recover missing points in the image of day 2. In
   * the second iteration, each image is used to recover missing points in the
   * preceding day. All odd numbered iterations work like the first iteration
   * while all even numbered iterations work like the second one. The algorithm
   * works like this until either all images are recovered or we run n iterations
   * where n is the total number of all images (time points).
   * 
   * @param dir
   * @throws IOException 
   */
  public static void recoverNearest(Path dir) throws IOException {
    FileSystem fs = dir.getFileSystem(new Configuration());
    FileStatus[] allImages = CommandLineArguments.isWildcard(dir)?
        fs.globStatus(dir) : fs.listStatus(dir);
    Arrays.sort(allImages, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        // Sort alphabetically based on file name
        return o1.getPath().getName().compareTo(o2.getPath().getName());
      }
    });
    // A sorted list of the index of all images that still have holes
    Vector<Integer> hollowImages = new Vector<Integer>();
    for (int i_image = 0; i_image < allImages.length; i_image++)
      hollowImages.add(i_image);
    for (int iter = 0; iter < allImages.length; iter++) {
      int i1, i2, increment;
      if (iter % 2 == 0) {
        i1 = hollowImages.size() - 2;
        i2 = -1;
        increment = -1;
      } else {
        i1 = 1;
        i2 = hollowImages.size();
        increment = 1;
      }
      for (int i_img = i1; i_img != i2; i_img += increment) {
        FSDataInputStream instream =
            fs.open(allImages[hollowImages.get(i_img)].getPath());
        BufferedImage img = ImageIO.read(instream);
        instream.close();
        instream =
            fs.open(allImages[hollowImages.get(i_img) - increment].getPath());
        BufferedImage img_bg = ImageIO.read(instream);
        instream.close();
        
        Graphics2D graphics = img_bg.createGraphics();
        graphics.drawImage(img, 0, 0, null);
        graphics.dispose();
        FSDataOutputStream outstream =
            fs.create(allImages[hollowImages.get(i_img)].getPath(), true);
        ImageIO.write(img_bg, "png", outstream);
        outstream.close();
      }
    }
  }
  
  /**
   * Recover all images in the given directory by performing an interpolation
   * on each missing point from the two nearest points on each side on its
   * horizontal line (the nearest left and right points).
   * 
   * To determine which points should be interpolated (e.g., under cloud) and
   * which points should remain blank (e.g., in sea), we first overlay all
   * images on top of each other. If a point is missing from all images, it
   * indicates with very high probability that it should remain blank. 
   * @param dir
   * @throws IOException
   */
  public static void recoverInterpolation(Path dir) throws IOException {
    FileSystem fs = dir.getFileSystem(new Configuration());
    FileStatus[] allImages = CommandLineArguments.isWildcard(dir)?
        fs.globStatus(dir) : fs.listStatus(dir);
    Arrays.sort(allImages, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        // Sort alphabetically based on file name
        return o1.getPath().getName().compareTo(o2.getPath().getName());
      }
    });

    // Create a mask of valid points by overlaying all images on each other
    BufferedImage mask = null;
    Graphics g = null;
    for (FileStatus imageFile : allImages) {
      FSDataInputStream instream = fs.open(imageFile.getPath());
      BufferedImage img = ImageIO.read(instream);
      instream.close();
      
      if (g == null) {
        mask = img;
        g = mask.getGraphics();
      } else {
        g.drawImage(img, 0, 0, null);
      }
    }
    g.dispose();
    
    // Recover missing points on each image
    for (FileStatus imageFile : allImages) {
      FSDataInputStream instream = fs.open(imageFile.getPath());
      BufferedImage img = ImageIO.read(instream);
      instream.close();
      
      // Go over this image row by row
      for (int y = 0; y < img.getHeight(); y++) {
        // First empty point in current run
        int x1 = 0;
        // Last non-empty point in current run
        int x2 = 0;
        while (x1 < img.getWidth()) {
          // Detect next run of empty points
          x1 = x2;
          while (x1 < img.getWidth() && (img.getRGB(x1, y) >> 24) != 0)
            x1++;
          x2 = x1;
          while (x2 < img.getWidth() && (img.getRGB(x2, y) >> 24) == 0)
            x2++;
          if (x1 > 0 && x2 < img.getWidth()) {
            float[] hsbvals = new float[3];
            int color1 = img.getRGB(x1-1, y);
            Color.RGBtoHSB((color1 >> 16) & 0xff, (color1 >> 8) & 0xff, color1 & 0xff, hsbvals);
            float hue1 = hsbvals[0];

            int color2 = img.getRGB(x2, y);
            Color.RGBtoHSB((color2 >> 16) & 0xff, (color2 >> 8) & 0xff, color2 & 0xff, hsbvals);
            float hue2 = hsbvals[0];
            
            int[] recoveredPoints = new int[x2 - x1];
            
            // Recover all missing points that should be recovered in this run
            for (int x = x1; x < x2; x++) {
              if (mask.getRGB(x, y) != 0) {
                // Should be recovered
                float recoveredHue = (hue1 * (x2 - x) + hue2 * (x - x1)) / (x2 - x1);
                int recoveredColor = Color.HSBtoRGB(recoveredHue, hsbvals[1], hsbvals[2]);
                recoveredPoints[x - x1] = recoveredColor;
                img.setRGB(x, y, recoveredColor);
              }
            }
            //img.setRGB(x1, y, x2 - x1, 1, recoveredPoints, 0, 0);
          }
        }
      }
      
      FSDataOutputStream outstream = fs.create(imageFile.getPath(), true);
      ImageIO.write(img, "png", outstream);
      outstream.close();
      break;
    }
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    recoverInterpolation(new Path("jan"));
  }

}
