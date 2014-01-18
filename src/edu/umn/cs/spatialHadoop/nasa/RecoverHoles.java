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
  public static void recoverLocal(Path dir) throws IOException {
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
      System.out.println("Iteration #"+iter);
      int i1, i2, increment;
      if (iter % 2 == 0) {
        i1 = hollowImages.size() - 2;
        i2 = 0;
        increment = -1;
      } else {
        i1 = 1;
        i2 = hollowImages.size() - 1;
        increment = 1;
      }
      for (int i_img = i1; i_img != i2; i_img += increment) {
        FSDataInputStream instream = fs.open(allImages[i_img].getPath());
        BufferedImage img = ImageIO.read(instream);
        instream.close();
        instream = fs.open(allImages[i_img - increment].getPath());
        BufferedImage img_bg = ImageIO.read(instream);
        instream.close();
        
        Graphics2D graphics = img_bg.createGraphics();
        graphics.drawImage(img, 0, 0, null);
        graphics.dispose();
        FSDataOutputStream outstream = fs.create(allImages[i_img].getPath(), true);
        ImageIO.write(img_bg, "png", outstream);
        outstream.close();
      }
    }
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    recoverLocal(new Path("jan"));
  }

}
