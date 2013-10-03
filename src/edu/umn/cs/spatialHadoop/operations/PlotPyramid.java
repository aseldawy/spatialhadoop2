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
package edu.umn.cs.spatialHadoop.operations;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.SimpleGraphics;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * Plots all tile images needed to naviagte through the image using Google Maps.
 * Each image is 256x256. In each level, the combination of all images represent
 * the whole dataset. Each image is encoded with its 3D location in the pyramid.
 * The first coordinate is the pyramid level starting at zero in the root level
 * which represents the whole dataset in one tile. The second and third
 * coordinates represent the location of this tile in the grid of this level.
 * The top left tile is (0,0). The next tiles are (0,1) and (1,0) and so on.
 * @author Ahmed Eldawy
 *
 */
public class PlotPyramid {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(PlotPyramid.class);
  
  /**Minimal Bounding Rectangle of input file*/
  private static final String InputMBR = "PlotPyramid.InputMBR";
  /**Number of levels in the pyramid ,to plot (+ve Integer)*/
  private static final String NumLevels = "PlotPyramid.NumLevel";
  /**Width of each tile in pixels*/
  private static final String TileWidth = "PlotPyramid.TileWidth";
  /**Height of each tile in pixels*/
  private static final String TileHeight = "PlotPyramid.TileHeight";

  /**
   * An internal class that indicates a position of a tile in the pyramid.
   * Level is the level of the tile starting with 0 at the top.
   * x and y are the index of the column and row of the tile in the grid
   * at this level.
   * @author Ahmed Eldawy
   *
   */
  public static class TileIndex implements WritableComparable<TileIndex> {
    public int level, x, y;
    
    public TileIndex() {}
    
    @Override
    public void readFields(DataInput in) throws IOException {
      level = in.readInt();
      x = in.readInt();
      y = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(level);
      out.writeInt(x);
      out.writeInt(y);
    }

    @Override
    public int compareTo(TileIndex a) {
      if (this.level != a.level)
        return this.level - a.level;
      if (this.x != a.x)
        return this.x - a.x;
      return this.y - a.y;
    }
    
  }
  
  /**
   * The map function replicates each object to all the tiles it overlaps with.
   * It starts with the bottom level and replicates each shape to all the
   * levels up to the top of the pyramid. 
   * @author Ahmed Eldawy
   */
  public static class PlotMap extends MapReduceBase 
    implements Mapper<Rectangle, Shape, TileIndex, Shape> {

    /**Number of levels in the pyramid*/
    private int numLevels;
    /**The grid at the bottom level of the pyramid*/
    private GridInfo bottomGrid;
    /**Used as a key for output*/
    private TileIndex key;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      numLevels = job.getInt(NumLevels, 1);
      Rectangle inputMBR = SpatialSite.getRectangle(job, InputMBR);
      bottomGrid = new GridInfo(inputMBR.x1, inputMBR.y1, inputMBR.x2, inputMBR.y2);
      bottomGrid.rows = bottomGrid.columns =
          (int) Math.round(Math.pow(2, numLevels - 1));
      this.key = new TileIndex();
    }
    
    public void map(Rectangle cell, Shape shape,
        OutputCollector<TileIndex, Shape> output, Reporter reporter)
        throws IOException {
      java.awt.Rectangle overlappingCells =
          bottomGrid.getOverlappingCells(shape.getMBR());
      for (key.level = numLevels - 1; key.level >= 0; key.level--) {
        for (int i = 0; i <= overlappingCells.width; i++) {
          key.x = i + overlappingCells.x;
          for (int j = 0; j <= overlappingCells.height; j++) {
            key.y = j + overlappingCells.y;
            // Replicate to the cell at (x + i, y + j) on the current level
            output.collect(key, shape);
          }
        }
        // Shrink overlapping cells to match the upper level
        int updatedX1 = overlappingCells.x / 2;
        int updatedY1 = overlappingCells.y / 2;
        int updatedX2 = (overlappingCells.x + overlappingCells.width) / 2;
        int updatedY2 = (overlappingCells.y + overlappingCells.height) / 2;
        overlappingCells.x = updatedX1;
        overlappingCells.y = updatedY1;
        overlappingCells.width = updatedX2 - updatedX1 + 1;
        overlappingCells.height = updatedY2 - updatedY1 + 1;
      }
    }
  }
  
  /**
   * The reducer class draws an image for contents (shapes) in each tile
   * @author Ahmed Eldawy
   *
   */
  public static class PlotReduce extends MapReduceBase
      implements Reducer<TileIndex, Shape, TileIndex, ImageWritable> {
    
    private Rectangle fileMBR;
    private int tileWidth, tileHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      fileMBR = SpatialSite.getRectangle(job, InputMBR);
      tileWidth = job.getInt(TileWidth, 256);
      tileHeight = job.getInt(TileHeight, 256);
    }

    @Override
    public void reduce(TileIndex tileIndex, Iterator<Shape> values,
        OutputCollector<TileIndex, ImageWritable> output, Reporter reporter)
        throws IOException {
      // Coordinates of the current tile in data coordinates
      Rectangle tileMBR = new Rectangle();
      // Edge length of tile in the current level
      double tileSize = fileMBR.getWidth() / Math.pow(2, tileIndex.level);
      tileMBR.x1 = fileMBR.x1 + tileSize * tileIndex.x;
      tileMBR.y1 = fileMBR.y1 + tileSize * tileIndex.y;
      tileMBR.x2 = tileMBR.x1 + tileSize;
      tileMBR.y2 = tileMBR.y1 + tileSize;
      this.scale2 = (double)tileWidth * tileHeight /
          ((double)(tileMBR.x2 - tileMBR.x1) * (tileMBR.y2 - tileMBR.y1));
      try {
        // Initialize the image
//        int image_x1 = (int) ((cellInfo.x1 - fileMBR.x1) * imageWidth / (fileMBR.x2 - fileMBR.x1));
//        int image_y1 = (int) ((cellInfo.y1 - fileMBR.y1) * imageHeight / (fileMBR.y2 - fileMBR.y1));
//        int image_x2 = (int) (((cellInfo.x2) - fileMBR.x1) * imageWidth / (fileMBR.x2 - fileMBR.x1));
//        int image_y2 = (int) (((cellInfo.y2) - fileMBR.y1) * imageHeight / (fileMBR.y2 - fileMBR.y1));
//        int tile_width = image_x2 - image_x1;
//        int tile_height = image_y2 - image_y1;
//        if (tile_width == 0 || tile_height == 0)
//          return;

        BufferedImage image = new BufferedImage(tileWidth, tileHeight,
            BufferedImage.TYPE_INT_ARGB);
        Color bg_color = new Color(0,0,0,0);
        Color stroke_color = Color.BLUE;

        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        graphics.setBackground(bg_color);
        graphics.clearRect(0, 0, tileWidth, tileHeight);
        graphics.setColor(stroke_color);
        
        // Translate tile origin (x1, y1) to image origin (0, 0)
        int dx = (int) ((tileMBR.x1 - fileMBR.x1) * tileWidth / fileMBR.getWidth());
        int dy = (int) ((tileMBR.y1 - fileMBR.y1) * tileHeight / fileMBR.getHeight());
        graphics.translate(-dx, -dy);

        while (values.hasNext()) {
          Shape s = values.next();
          Plot.drawShape(graphics, s, fileMBR, tileWidth, tileHeight, scale2);
        }
        
        graphics.dispose();
        
        sharedValue.setImage(image);
        output.collect(tileIndex, sharedValue);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }

  }  
  
  public static <S extends Shape> void plotMapReduce(Path inFile, Path outFile,
      Shape shape, int tileWidth, int tileHeight, int numLevels)
          throws IOException {
    JobConf job = new JobConf(PlotPyramid.class);
    job.setJobName("Plot");
    
    job.setMapperClass(PlotMap.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setReducerClass(PlotReduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
    job.setMapOutputKeyClass(Rectangle.class);
    SpatialSite.setShapeClass(job, shape.getClass());
    job.setMapOutputValueClass(shape.getClass());
    
    FileSystem inFs = inFile.getFileSystem(job);
    Rectangle fileMBR = FileMBR.fileMBRMapReduce(inFs, inFile, shape);
    FileStatus inFileStatus = inFs.getFileStatus(inFile);
    
    SpatialSite.setRectangle(job, InputMBR, fileMBR);
    job.setInt(TileWidth, tileWidth);
    job.setInt(TileHeight, tileHeight);
    job.setInt(NumLevels, numLevels);
    
    // Set input and output
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    
    job.setOutputFormat(ImageOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outFile);
    
    JobClient.runJob(job);
  }
  
  public static <S extends Shape> void plot(Path inFile, Path outFile,
      S shape, int tileWidth, int tileHeight, int numLevels)
          throws IOException {
    plotMapReduce(inFile, outFile, shape, tileWidth, tileHeight, numLevels);
  }

  
  private static void printUsage() {
    System.out.println("Plots a file to a set of images that form a pyramid to be used by Google Maps or a similar engine");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("tilewidth:<w> - Width of each tile in pixels");
    System.out.println("tileheight:<h> - Height of each tile in pixels");
//    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("numlevels:<n> - Number of levels in the pyrmaid");
    System.out.println("-overwrite: Override output file without notice");
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(PlotPyramid.class);
    Path[] files = cla.getPaths();
    if (files.length < 2) {
      printUsage();
      throw new RuntimeException("Illegal arguments. File names missing");
    }
    
    Path inFile = files[0];
    FileSystem inFs = inFile.getFileSystem(conf);
    if (!inFs.exists(inFile)) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }
    
    boolean overwrite = cla.isOverwrite();
    Path outFile = files[1];
    FileSystem outFs = outFile.getFileSystem(conf);
    if (outFs.exists(outFile)) {
      if (overwrite)
        outFs.delete(outFile, true);
      else
        throw new RuntimeException("Output file exists and overwrite flag is not set");
    }

    Shape shape = cla.getShape(true);
    
    int tileWidth = cla.getInt("tilewidth", 256);
    int tileHeight = cla.getInt("tileheight", 256);
    int numLevels = cla.getInt("numlevels", 8);
    
    plot(inFile, outFile, shape, tileWidth, tileHeight, numLevels);
  }

}
