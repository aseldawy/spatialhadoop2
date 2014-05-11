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
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Vector;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

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
    FileStatus[] allImages = OperationsParams.isWildcard(dir)?
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
  public static void recoverInterpolationDir(Path dir) throws IOException {
    FileSystem fs = dir.getFileSystem(new Configuration());
    FileStatus[] allImages = OperationsParams.isWildcard(dir)?
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
    Graphics2D g = null;
    for (FileStatus imageFile : allImages) {
      FSDataInputStream instream = fs.open(imageFile.getPath());
      BufferedImage img = ImageIO.read(instream);
      instream.close();
      
      if (g == null) {
        mask = img;
        g = mask.createGraphics();
      } else {
        g.drawImage(img, 0, 0, null);
      }
    }
    g.dispose();
    
    // Recover missing points on each image
    for (FileStatus imageFile : allImages) {
      recoverImageInterpolation(fs, mask, imageFile);
    }
  }

  /**
   * Recover holes in one image using the interpolation technique.
   * @param fs
   * @param mask
   * @param imageFile
   * @throws IOException
   */
  private static void recoverImageInterpolation(FileSystem fs,
      BufferedImage mask, FileStatus imageFile) throws IOException {
    Graphics2D g;
    System.out.println("Working on "+imageFile.getPath().getName());
    FSDataInputStream instream = fs.open(imageFile.getPath());
    BufferedImage img = ImageIO.read(instream);
    instream.close();
    
    // All recovered values are stored in this image
    BufferedImage recoveryInterpolation = new BufferedImage(img.getWidth(),
        img.getHeight(), BufferedImage.TYPE_INT_ARGB);
    g = recoveryInterpolation.createGraphics();
    g.setBackground(new Color(0, 0, 0, 0));
    g.clearRect(0, 0, recoveryInterpolation.getWidth(), recoveryInterpolation.getHeight());
    g.dispose();
    
    BufferedImage recoveryCopy = new BufferedImage(img.getWidth(),
        img.getHeight(), BufferedImage.TYPE_INT_ARGB);
    g = recoveryCopy.createGraphics();
    g.setBackground(new Color(0, 0, 0, 0));
    g.clearRect(0, 0, recoveryCopy.getWidth(), recoveryCopy.getHeight());
    g.dispose();
    
    // Go over this image row by row
    for (int y = 0; y < img.getHeight(); y++) {
      // First empty point in current run
      int x1 = 0;
      // First non-empty point in next run (last empty point + 1)
      int x2 = 0;
      while (x2 < img.getWidth()) {
        // Detect next run of empty points
        x1 = x2;
        while (x1 < img.getWidth() && !isTransparent(img, x1, y))
          x1++;
        x2 = x1;
        while (x2 < img.getWidth() && isTransparent(img, x2, y))
          x2++;
        if (x1 == 0 && x2 < img.getWidth() || x1 > 0 && x2 == img.getWidth()) {
          // Only one point at one end is inside image boundaries.
          // Use the available point to paint all missing points
          int color = x1 > 0? img.getRGB(x1-1, y) : img.getRGB(x2, y);
          for (int x = x1; x < x2; x++) {
            if (!isTransparent(mask, x, y)) {
              mergePoints(recoveryCopy, x, y, color);
            }
          }
        } else if (x1 > 0 && x2 < img.getWidth()) {
          // Two ends are available, interpolate to recover points
          float[] hsbvals = new float[3];
          int color1 = img.getRGB(x1-1, y);
          Color.RGBtoHSB((color1 >> 16) & 0xff, (color1 >> 8) & 0xff, color1 & 0xff, hsbvals);
          float hue1 = hsbvals[0];

          int color2 = img.getRGB(x2, y);
          Color.RGBtoHSB((color2 >> 16) & 0xff, (color2 >> 8) & 0xff, color2 & 0xff, hsbvals);
          float hue2 = hsbvals[0];
          
          // Recover all missing points that should be recovered in this run
          for (int x = x1; x < x2; x++) {
            if (!isTransparent(mask, x, y)) {
              // Should be recovered
              float recoveredHue = (hue1 * (x2 - x) + hue2 * (x - x1)) / (x2 - x1);
              int recoveredColor = Color.HSBtoRGB(recoveredHue, hsbvals[1], hsbvals[2]);
              mergePoints(recoveryInterpolation, x, y, recoveredColor);
            }
          }
        }
      }
    }
    
    // Go over image column by column
    for (int x = 0; x < img.getWidth(); x++) {
      // First empty point in current run
      int y1 = 0;
      // Last non-empty point in current run
      int y2 = 0;
      while (y2 < img.getHeight()) {
        // Detect next run of empty points
        y1 = y2;
        while (y1 < img.getHeight() && !isTransparent(img, x, y1))
          y1++;
        y2 = y1;
        while (y2 < img.getHeight() && isTransparent(img, x, y2))
          y2++;
        if (y1 == 0 && y2 < img.getHeight() || y1 > 0 && y2 == img.getHeight()) {
          // Only one point at one end is inside image boundaries.
          // Use the available point to paint all missing points
          int color = y1 > 0? img.getRGB(x, y1-1) : img.getRGB(x, y2);
          for (int y = y1; y < y2; y++) {
            if (!isTransparent(mask, x, y)) {
              mergePoints(recoveryCopy, x, y, color);
            }
          }
        } else if (y1 > 0 && y2 < img.getHeight()) {
          // Two ends are available, interpolate to recover points
          float[] hsbvals = new float[3];
          int color1 = img.getRGB(x, y1-1);
          Color.RGBtoHSB((color1 >> 16) & 0xff, (color1 >> 8) & 0xff, color1 & 0xff, hsbvals);
          float hue1 = hsbvals[0];

          int color2 = img.getRGB(x, y2);
          Color.RGBtoHSB((color2 >> 16) & 0xff, (color2 >> 8) & 0xff, color2 & 0xff, hsbvals);
          float hue2 = hsbvals[0];
          
          // Recover all missing points that should be recovered in this run
          for (int y = y1; y < y2; y++) {
            if (!isTransparent(mask, x, y)) {
              // Should be recovered
              float recoveredHue = (hue1 * (y2 - y) + hue2 * (y - y1)) / (y2 - y1);
              int recoveredColor = Color.HSBtoRGB(recoveredHue, hsbvals[1], hsbvals[2]);
              mergePoints(recoveryInterpolation, x, y, recoveredColor);
            }
          }
        }
      }
    }
    
    // Overlay the layer of recovered points
    // Overlay recoveryCopy first then recoveryInterpolation to give higher
    // priority to interpolation as it's smoother.
    // In other words, points that could be interpolated overwrite points
    // that could only be copied for the same location.
    g = img.createGraphics();
    g.drawImage(recoveryCopy, 0, 0, null);
    g.drawImage(recoveryInterpolation, 0, 0, null);
    g.dispose();
    
    FSDataOutputStream outstream = fs.create(imageFile.getPath(), true);
    ImageIO.write(img, "png", outstream);
    outstream.close();
  }
  
  private static void mergePoints(BufferedImage img, int x, int y, int newClr) {
    int old_clr = img.getRGB(x, y);
    if ((old_clr >> 24) == 0) {
      img.setRGB(x, y, newClr);
    } else {
      // Merge two colors
      float[] old_hsbvals = new float[3];
      Color.RGBtoHSB((old_clr >> 16) & 0xff, (old_clr >> 8) & 0xff, old_clr & 0xff, old_hsbvals);
      float[] new_hsbvals = new float[3];
      Color.RGBtoHSB((newClr >> 16) & 0xff, (newClr >> 8) & 0xff, newClr & 0xff, new_hsbvals);
      for (int i = 0; i < old_hsbvals.length; i++) {
        new_hsbvals[i] = (old_hsbvals[i] + new_hsbvals[i]) / 2;
      }
      img.setRGB(x, y, Color.HSBtoRGB(new_hsbvals[0], new_hsbvals[1], new_hsbvals[2]));
    }
  }

  private static boolean isTransparent(BufferedImage img, int x, int y) {
    return (img.getRGB(x, y) >> 24) == 0;
  }

  public static void addDate(Path dir) throws IOException {
    FileSystem fs = dir.getFileSystem(new Configuration());
    FileStatus[] allImages = OperationsParams.isWildcard(dir) ?
        fs.globStatus(dir) : fs.listStatus(dir);

    final Font font = new Font("Arial", Font.BOLD, 48);
    final SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy.MM.dd");
    final SimpleDateFormat outputDateFormat = new SimpleDateFormat("dd MMM");

    for (FileStatus imageFile : allImages) {
      try {
        FSDataInputStream instream = fs.open(imageFile.getPath());
        BufferedImage img = ImageIO.read(instream);
        instream.close();
        
        Graphics2D g = img.createGraphics();
        g.setFont(font);
        String filename = imageFile.getPath().getName();
        String dateStr = filename.substring(0, filename.length() - 4);
        Date date = inputDateFormat.parse(dateStr);
        String text = outputDateFormat.format(date);
        g.setColor(Color.BLACK);
        g.drawString(text, 5, img.getHeight() - 5);
        g.dispose();
        
        FSDataOutputStream outstream = fs.create(imageFile.getPath(), true);
        ImageIO.write(img, "png", outstream);
        outstream.close();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    OperationsParams cla = new OperationsParams(new GenericOptionsParser(args));
    Path dir = cla.getPath();
    if (dir == null) {
      System.err.println("Please provide an input directory");
      return;
    }
    boolean addDate = cla.is("adddate");
    long t1 = System.currentTimeMillis();
    recoverInterpolationDir(dir);
    if (addDate) {
      System.out.println("Adding dates");
      addDate(dir);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Total time millis "+(t2-t1));
  }

}
