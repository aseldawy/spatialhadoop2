/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.PyramidOutputFormat;
import edu.umn.cs.spatialHadoop.SimpleGraphics;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

/**
 * Generates a multilevel image
 * @author Ahmed Eldawy
 *
 */
public class MultilevelPlot {
  /**Configuration entry for input MBR*/
  private static final String InputMBR = "mbr";
  
  public static class DataPartitionMap extends MapReduceBase 
    implements Mapper<Rectangle, Iterable<? extends Shape>, TileIndex, RasterLayer> {

    
    /**Minimum and maximum levels of the pyramid to plot (inclusive and zero-based)*/
    private int minLevel, maxLevel;
    
    /**The grid at the bottom level (i.e., maxLevel)*/
    private GridInfo bottomGrid;

    /**The MBR of the input area to draw*/
    private Rectangle inputMBR;

    /**The rasterizer associated with this job*/
    private Rasterizer rasterizer;

    /**Fixed width for one tile*/
    private int tileWidth;

    /**Fixed height for one tile */
    private int tileHeight;
    
    /**Buffer size that should be taken in the maximum level*/
    private double bufferSizeXMaxLevel;

    private double bufferSizeYMaxLevel;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      String[] strLevels = job.get("levels", "7").split("\\.\\.");
      if (strLevels.length == 1) {
        minLevel = 0;
        maxLevel = Integer.parseInt(strLevels[0]);
      } else {
        minLevel = Integer.parseInt(strLevels[0]);
        maxLevel = Integer.parseInt(strLevels[1]);
      }
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      this.bottomGrid = new GridInfo(inputMBR.x1, inputMBR.y1, inputMBR.x2, inputMBR.y2);
      this.bottomGrid.rows = bottomGrid.columns = 1 << maxLevel;
      this.tileWidth = job.getInt("tilewidth", 256);
      this.tileHeight = job.getInt("tileheight", 256);
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.rasterizer.configure(job);
      int radius = rasterizer.getRadius();
      this.bufferSizeXMaxLevel = radius * inputMBR.getWidth() / (tileWidth * (1 << maxLevel));
      this.bufferSizeYMaxLevel = radius * inputMBR.getHeight() / (tileHeight * (1 << maxLevel));
    }
    
    @Override
    public void map(Rectangle inMBR, Iterable<? extends Shape> shapes,
        OutputCollector<TileIndex, RasterLayer> output, Reporter reporter)
        throws IOException {
      TileIndex key = new TileIndex();
      Map<TileIndex, RasterLayer> rasterLayers = new HashMap<TileIndex, RasterLayer>();
      for (Shape shape : shapes) {
        Rectangle shapeMBR = shape.getMBR();
        if (shapeMBR == null)
          continue;
        java.awt.Rectangle overlappingCells =
            bottomGrid.getOverlappingCells(shapeMBR.buffer(bufferSizeXMaxLevel, bufferSizeYMaxLevel));
        // Iterate over levels from bottom up
        for (key.level = maxLevel; key.level >= minLevel; key.level--) {
          for (key.x = overlappingCells.x; key.x < overlappingCells.x + overlappingCells.width; key.x++) {
            for (key.y = overlappingCells.y; key.y < overlappingCells.y + overlappingCells.height; key.y++) {
              RasterLayer rasterLayer = rasterLayers.get(key);
              if (rasterLayer == null) {
                Rectangle tileMBR = new Rectangle();
                int gridSize = 1 << key.level;
                tileMBR.x1 = (inputMBR.x1 * (gridSize - key.x) + inputMBR.x2 * key.x) / gridSize;
                tileMBR.x2 = (inputMBR.x1 * (gridSize - (key.x + 1)) + inputMBR.x2 * (key.x+1)) / gridSize;
                tileMBR.y1 = (inputMBR.y1 * (gridSize - key.y) + inputMBR.y2 * key.y) / gridSize;
                tileMBR.y2 = (inputMBR.y1 * (gridSize - (key.y + 1)) + inputMBR.y2 * (key.y+1)) / gridSize;
                rasterLayer = rasterizer.create(tileWidth, tileHeight, tileMBR);
                rasterLayers.put(key.clone(), rasterLayer);
              }
              rasterizer.rasterize(rasterLayer, shape);
            }
          }
          // Update overlappingCells for the higher level
          int updatedX1 = overlappingCells.x / 2;
          int updatedY1 = overlappingCells.y / 2;
          int updatedX2 = (overlappingCells.x + overlappingCells.width - 1) / 2;
          int updatedY2 = (overlappingCells.y + overlappingCells.height - 1) / 2;
          overlappingCells.x = updatedX1;
          overlappingCells.y = updatedY1;
          overlappingCells.width = updatedX2 - updatedX1 + 1;
          overlappingCells.height = updatedY2 - updatedY1 + 1;
        }
      }
      // Write all created layers to the output
      for (Map.Entry<TileIndex, RasterLayer> entry : rasterLayers.entrySet()) {
        output.collect(entry.getKey(), entry.getValue());
      }
    }
    
  }

  public static class DataPartitionReduce extends MapReduceBase
    implements Reducer<TileIndex, RasterLayer, TileIndex, BufferedImage> {
    
    private Rasterizer rasterizer;
    private Rectangle inputMBR;
    private int tileWidth;
    private int tileHeight;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.rasterizer.configure(job);
      this.tileWidth = job.getInt("tilewidth", 256);
      this.tileHeight = job.getInt("tileheight", 256);
    }

    @Override
    public void reduce(TileIndex tileID, Iterator<RasterLayer> intermediateLayers,
        OutputCollector<TileIndex, BufferedImage> output, Reporter reporter)
        throws IOException {
      Rectangle tileMBR = new Rectangle();
      int gridSize = 1 << tileID.level;
      tileMBR.x1 = (inputMBR.x1 * (gridSize - tileID.x) + inputMBR.x2 * tileID.x) / gridSize;
      tileMBR.x2 = (inputMBR.x1 * (gridSize - (tileID.x + 1)) + inputMBR.x2 * (tileID.x+1)) / gridSize;
      tileMBR.y1 = (inputMBR.y1 * (gridSize - tileID.y) + inputMBR.y2 * tileID.y) / gridSize;
      tileMBR.y2 = (inputMBR.y1 * (gridSize - (tileID.y + 1)) + inputMBR.y2 * (tileID.y+1)) / gridSize;

      RasterLayer finalLayer = rasterizer.create(tileWidth, tileHeight, tileMBR);
      while (intermediateLayers.hasNext()) {
        rasterizer.mergeLayers(finalLayer, intermediateLayers.next());
      }
      
      BufferedImage image = rasterizer.toImage(finalLayer);
      output.collect(tileID, image);
    }
  }
  
  /**
   * Finalizes the job by combining all images of all reduces jobs into one
   * folder.
   * @author Ahmed Eldawy
   *
   */
  public static class MultiLevelOutputCommitter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);

      JobConf job = context.getJobConf();
      Path outPath = PyramidOutputFormat.getOutputPath(job);
      FileSystem outFs = outPath.getFileSystem(job);

      // Write a default empty image to be displayed for non-generated tiles
      int tileWidth = job.getInt("tilewidth", 256);
      int tileHeight = job.getInt("tileheight", 256);
      BufferedImage emptyImg = new BufferedImage(tileWidth, tileHeight,
          BufferedImage.TYPE_INT_ARGB);
      Graphics2D g = new SimpleGraphics(emptyImg);
      g.setBackground(new Color(0, 0, 0, 0));
      g.clearRect(0, 0, tileWidth, tileHeight);
      g.dispose();

      OutputStream out = outFs.create(new Path(outPath, "default.png"));
      ImageIO.write(emptyImg, "png", out);
      out.close();

      // Get the correct levels.
      String[] strLevels = job.get("levels", "7").split("\\.\\.");
      int minLevel, maxLevel;
      if (strLevels.length == 1) {
        minLevel = 0;
        maxLevel = Integer.parseInt(strLevels[0]);
      } else {
        minLevel = Integer.parseInt(strLevels[0]);
        maxLevel = Integer.parseInt(strLevels[1]);
      }

      // Add an HTML file that visualizes the result using Google Maps
      LineReader templateFileReader = new LineReader(getClass()
          .getResourceAsStream("/zoom_view.html"));
      PrintStream htmlOut = new PrintStream(outFs.create(new Path(outPath,
          "index.html")));
      Text line = new Text();
      while (templateFileReader.readLine(line) > 0) {
        String lineStr = line.toString();
        lineStr = lineStr.replace("#{TILE_WIDTH}", Integer.toString(tileWidth));
        lineStr = lineStr.replace("#{TILE_HEIGHT}",
            Integer.toString(tileHeight));
        lineStr = lineStr.replace("#{MAX_ZOOM}", Integer.toString(maxLevel));
        lineStr = lineStr.replace("#{MIN_ZOOM}", Integer.toString(minLevel));

        htmlOut.println(lineStr);
      }
      templateFileReader.close();
      htmlOut.close();
    }
  }
  
  public static void plotMapReduce(Path inFile, Path outFile,
      Class<? extends Rasterizer> rasterizerClass, OperationsParams params) throws IOException {
    Rasterizer rasterizer;
    try {
      rasterizer = rasterizerClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    }
    
    JobConf job = new JobConf(params, SingleLevelPlot.class);
    job.setJobName("MultiLevelPlot");
    // Set rasterizer
    Rasterizer.setRasterizer(job, rasterizerClass);
    // Set input file MBR
    Rectangle inputMBR = (Rectangle) params.getShape("rect");
    if (inputMBR == null)
      inputMBR = FileMBR.fileMBR(inFile, params);
    
    // Adjust width and height if aspect ratio is to be kept
    if (params.is("keepratio", true)) {
      // Expand input file to a rectangle for compatibility with the pyramid
      // structure
      if (inputMBR.getWidth() > inputMBR.getHeight()) {
        inputMBR.y1 -= (inputMBR.getWidth() - inputMBR.getHeight()) / 2;
        inputMBR.y2 = inputMBR.y1 + inputMBR.getWidth();
      } else {
        inputMBR.x1 -= (inputMBR.getHeight() - inputMBR.getWidth() / 2);
        inputMBR.x2 = inputMBR.x1 + inputMBR.getHeight();
      }
    }
    OperationsParams.setShape(job, InputMBR, inputMBR);
    
    // Set input and output
    job.setInputFormat(ShapeIterInputFormat.class);
    ShapeIterInputFormat.setInputPaths(job, inFile);
    job.setOutputFormat(PyramidOutputFormat2.class);
    PyramidOutputFormat.setOutputPath(job, outFile);
    
    // Set mapper, reducer and committer
    job.setMapperClass(DataPartitionMap.class);
    job.setMapOutputKeyClass(TileIndex.class);
    job.setMapOutputValueClass(rasterizer.getRasterClass());
    job.setReducerClass(DataPartitionReduce.class);
    job.setOutputCommitter(MultiLevelOutputCommitter.class);
    
    // Use multithreading in case the job is running locally
    job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());

    // Start the job
    JobClient.runJob(job);
  }
}
