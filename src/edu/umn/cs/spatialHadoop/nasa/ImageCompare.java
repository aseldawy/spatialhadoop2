/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

package edu.umn.cs.spatialHadoop.nasa;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Compares two images for one part in the world for similarity
 * @author Ahmed Eldawy
 *
 */
public class ImageCompare {
  
  /**
   * Returns a similarity measure between two images on the scale from 0 to 1.
   * 0 indicates no similarity at all while 1 indicates exact similarity
   * (i.e., the same image).
   * The images are assumed to be generated using Plot of PoltPyramid for
   * NASA data.
   * @param file1 - path to the first image
   * @param file2 - path to the second image
   * @return the similarity measure on a scale from 0 to 1
   * @throws IOException - if an error occurs while loading any of the images
   */
  public static double compareImages(Path file1, Path file2) throws IOException {
    BufferedImage image1 =
        ImageIO.read(file1.getFileSystem(new Configuration()).open(file1));
    BufferedImage image2 =
        ImageIO.read(file2.getFileSystem(new Configuration()).open(file2));
    
    if (image1.getWidth() != image2.getWidth()
        || image1.getHeight() != image2.getHeight())
      return 0.0;
    
    double error_sum_square = 0.0;
    int numPoints = 0;
    int numValidPoints = 0;
    
    for (int x = 0; x < image1.getWidth(); x++) {
      for (int y = 0; y < image1.getHeight(); y++) {
        int color1 = image1.getRGB(x, y);
        int color2 = image2.getRGB(x, y);
        // A transparent pixel (color = 0) indicates an empty part of the image
        if (color1 == 0 && color2 == 0) {
          // Two empty holes in both images. Perfect match
          //numPoints++;
        } else if (color1 == 0 || color2 == 0) {
          // Don't take into consideration
          
          // One hole and one data
          numPoints++;
          // Maximum possible error
          error_sum_square += NASAPoint.hue2 * NASAPoint.hue2;
        } else {
          // Both values are valid, compute the error based on Hue values
          float[] hsbvals = new float[3];
          Color.RGBtoHSB(color1 & 0xff, (color1 >> 8) & 0xff, (color1 >> 16) & 0xff, hsbvals);
          float val1 = hsbvals[0];
          Color.RGBtoHSB(color2 & 0xff, (color2 >> 8) & 0xff, (color2 >> 16) & 0xff, hsbvals);
          float val2 = hsbvals[0];
          error_sum_square += (val1 - val2) * (val1 - val2);
          numPoints++;
          numValidPoints++;
        }
      }
    }
    if (numValidPoints < (image1.getWidth() * image1.getHeight() / 10))
      return 0.0;
    float max_error = numPoints * NASAPoint.hue2;
    return (max_error - error_sum_square) / max_error;
  }
  
  /**
   * Compares two directories for similar images with matching names.
   * @param dir1
   * @param dir2
   * @throws IOException 
   */
  public static void compareFolders(Path dir1, Path dir2) throws IOException {
    final PathFilter png_filter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().toLowerCase().endsWith(".png");
      }
    };
    // Retrieve all images in dir1
    FileStatus[] images1 =
        dir1.getFileSystem(new Configuration()).listStatus(dir1, png_filter);
    Map<String, Path> images1ByName = new HashMap<String, Path>();
    for (FileStatus fstatus : images1)
      images1ByName.put(fstatus.getPath().getName(), fstatus.getPath());
    
    // Retrieve all images in dir2
    FileStatus[] images2 =
        dir2.getFileSystem(new Configuration()).listStatus(dir2, png_filter);
    Map<String, Path> images2ByName = new HashMap<String, Path>();
    for (FileStatus fstatus : images2)
      images2ByName.put(fstatus.getPath().getName(), fstatus.getPath());
    
    final Vector<Double> similarities = new Vector<Double>();
    final Vector<String> names = new Vector<String>();
    
    // Compare every pair of images with similar names
    for (String imageName : images2ByName.keySet()) {
      Path image1 = images1ByName.get(imageName);
      if (image1 == null)
        continue;
      Path image2 = images2ByName.get(imageName);
      double similarity = compareImages(image1, image2);
      

      if (similarity > 0.1) {
        System.out.println(image1+","+image2+","+similarity);
        similarities.add(similarity);
        names.add(imageName);
      }
    }
    /*
    // Sort images by similarity
    IndexedSortable sortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diff = similarities.get(i) - similarities.get(j);
        if (diff < 0)
          return -1;
        if (diff > 0)
          return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        double tempSim = similarities.get(i);
        similarities.set(i, similarities.get(j));
        similarities.set(j, tempSim);
        
        String tempName = names.get(i);
        names.set(i, names.get(j));
        names.set(j, tempName);
      }
    };
    
    final IndexedSorter sorter = new QuickSort();
    sorter.sort(sortable, 0, names.size());
    final float threshold = 0.0f;
    // Display to 10 percentile matches
    for (int i = (int) (names.size() * threshold); i < names.size(); i++) {
      System.out.println(similarities.get(i)+ " ... "+names.get(i));
    }*/
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    long time1 = System.currentTimeMillis();
    String[] days = new File("one_year").list();
    for (int day1 = 0; day1 < days.length; day1++) {
      for (int day2 = day1+1; day2 < days.length; day2++) {
        compareFolders(
            new Path("one_year/"+days[day1]),
            new Path("one_year/"+days[day2]));
        
      }
    }
    long time2 = System.currentTimeMillis();
    System.out.println("Total time in millis "+(time2-time1));
  }

}
