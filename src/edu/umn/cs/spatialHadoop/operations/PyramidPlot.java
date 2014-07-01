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
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.PyramidOutputFormat;
import edu.umn.cs.spatialHadoop.SimpleGraphics;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeArrayInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;
import edu.umn.cs.spatialHadoop.nasa.NASARectangle;
import edu.umn.cs.spatialHadoop.operations.Aggregate.MinMax;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeFilter;

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
public class PyramidPlot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(PyramidPlot.class);
  
  /**Minimal Bounding Rectangle of input file*/
  private static final String InputMBR = "PlotPyramid.InputMBR";
  /**Valid range of values for HDF dataset*/
  private static final String MinValue = "plot.min_value";
  private static final String MaxValue = "plot.max_value";

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
    
    public TileIndex(int level, int x, int y) {
      super();
      this.level = level;
      this.x = x;
      this.y = y;
    }

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
    
    @Override
    public String toString() {
      return "Level: "+level+" @("+x+","+y+")";
    }
    
    @Override
    public int hashCode() {
      return level * 31 + x * 25423 + y;
    }
    
    @Override
    public boolean equals(Object obj) {
      TileIndex b = (TileIndex) obj;
      return this.level == b.level && this.x == b.x && this.y == b.y;
    }
    
    @Override
    public TileIndex clone() {
      return new TileIndex(this.level, this.x, this.y);
    }

    public String getImageFileName() {
      return "tile_"+this.level+"_"+this.x+"-"+this.y+".png";
    }
  }
  
  /**
   * The map function replicates each object to all the tiles it overlaps with.
   * It starts with the bottom level and replicates each shape to all the
   * levels up to the top of the pyramid. 
   * @author Ahmed Eldawy
   */
  public static class SpacePartitionMap extends MapReduceBase 
    implements Mapper<Rectangle, Shape, TileIndex, Shape> {

    /**Number of levels in the pyramid*/
    private int numLevels;
    /**The grid at the bottom level of the pyramid*/
    private GridInfo bottomGrid;
    /**Used as a key for output*/
    private TileIndex key;
    /**The probability of replicating a point to level i*/
    private double[] levelProb;
    private double[] scale;
    private boolean adaptiveSampling;
    private boolean gradualFade;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.numLevels = job.getInt("numlevels", 1);
      Rectangle inputMBR = SpatialSite.getRectangle(job, InputMBR);
      this.bottomGrid = new GridInfo(inputMBR.x1, inputMBR.y1, inputMBR.x2, inputMBR.y2);
      this.bottomGrid.rows = bottomGrid.columns =
          (int) Math.round(Math.pow(2, numLevels - 1));
      this.key = new TileIndex();
      this.adaptiveSampling = job.getBoolean("sample", false);
      this.levelProb = new double[this.numLevels];
      this.scale = new double[numLevels];
      int tileWidth = job.getInt("tilewidth", 256);
      int tileHeight = job.getInt("tileheight", 256);
      this.scale[0] = Math.sqrt((double)tileWidth * tileHeight /
          (inputMBR.getWidth() * inputMBR.getHeight()));
      this.levelProb[0] = job.getFloat(GeometricPlot.AdaptiveSampleRatio, 0.1f);
      for (int level = 1; level < numLevels; level++) {
        this.levelProb[level] = this.levelProb[level - 1] * 4;
        this.scale[level] = this.scale[level - 1] * (1 << level);
      }
      this.gradualFade = job.getBoolean("fade", false);
    }
    
    public void map(Rectangle cell, Shape shape,
        OutputCollector<TileIndex, Shape> output, Reporter reporter)
        throws IOException {
      Rectangle shapeMBR = shape.getMBR();
      if (shapeMBR == null)
        return;
      
      int min_level = 0;
      if (adaptiveSampling && shape instanceof Point) {
        // Special handling for NASA data
        double p = Math.random();
        // Skip levels that do not satisfy the probability
        while (min_level < numLevels && p > levelProb[min_level])
          min_level++;
      }
      
      java.awt.Rectangle overlappingCells =
          bottomGrid.getOverlappingCells(shapeMBR);
      for (key.level = numLevels - 1; key.level >= min_level; key.level--) {
        if (gradualFade && !(shape instanceof Point)) {
          double areaInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale[key.level];
          if (areaInPixels < 1.0 && Math.round(areaInPixels * 255) < 1.0) {
            // This shape can be safely skipped as it is too small to be plotted
            return;
          }
        }

        for (int i = 0; i < overlappingCells.width; i++) {
          key.x = i + overlappingCells.x;
          for (int j = 0; j < overlappingCells.height; j++) {
            key.y = j + overlappingCells.y;
            // Replicate to the cell at (x + i, y + j) on the current level
            output.collect(key, shape);
          }
        }
        // Shrink overlapping cells to match the upper level
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
  }
  
  /**
   * The reducer class draws an image for contents (shapes) in each tile
   * @author Ahmed Eldawy
   *
   */
  public static class SpacePartitionReduce extends MapReduceBase
      implements Reducer<TileIndex, Shape, TileIndex, ImageWritable> {
    
    private Rectangle fileMBR;
    private int tileWidth, tileHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2;
    private Color strokeColor;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      fileMBR = SpatialSite.getRectangle(job, InputMBR);
      tileWidth = job.getInt("tilewidth", 256);
      tileHeight = job.getInt("tileheight", 256);
      this.strokeColor = new Color(job.getInt("color", 0));
      NASAPoint.minValue = job.getInt(MinValue, 0);
      NASAPoint.maxValue = job.getInt(MaxValue, 65535);
      NASAPoint.setColor1(OperationsParams.getColor(job, "color1", Color.BLUE));
      NASAPoint.setColor2(OperationsParams.getColor(job, "color2", Color.RED));
      NASAPoint.gradientType = OperationsParams.getGradientType(job, "gradient", NASAPoint.GradientType.GT_HUE);
    }

    @Override
    public void reduce(TileIndex tileIndex, Iterator<Shape> values,
        OutputCollector<TileIndex, ImageWritable> output, Reporter reporter)
        throws IOException {
      // Size of the whole file in pixels at current level
      int imageWidth = tileWidth * (1 << tileIndex.level);
      int imageHeight = tileHeight * (1 << tileIndex.level);
      // Coordinates of this tile in image coordinates
      int tileX1 = tileWidth * tileIndex.x;
      int tileY1 = tileHeight * tileIndex.y;
      this.scale2 = (double)imageWidth * imageHeight/ (fileMBR.getWidth() * fileMBR.getHeight());
      
      try {
        // Initialize the image
        BufferedImage image = new BufferedImage(tileWidth, tileHeight,
            BufferedImage.TYPE_INT_ARGB);
        Color bg_color = new Color(0,0,0,0);

        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        graphics.setBackground(bg_color);
        graphics.clearRect(0, 0, tileWidth, tileHeight);
        graphics.setColor(strokeColor);
        graphics.translate(-tileX1, -tileY1);
        
        while (values.hasNext()) {
          Shape s = values.next();
          s.draw(graphics, fileMBR, imageWidth, imageHeight, scale2);
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
  
  public static class DataPartitionMap extends MapReduceBase 
    implements Mapper<Rectangle, ArrayWritable, TileIndex, ImageWritable> {

    /**The image of each tile that has data*/
    private Map<TileIndex, BufferedImage> tileImages =
        new HashMap<PyramidPlot.TileIndex, BufferedImage>();
    
    /**The graphics of each image created in the pyramid*/
    private Map<TileIndex, Graphics2D> tileGraphics =
        new HashMap<PyramidPlot.TileIndex, Graphics2D>();

    /**Number of levels in the pyramid*/
    private int numLevels;
    /**The grid at the bottom level of the pyramid*/
    private GridInfo bottomGrid;
    /**Used as a key for output*/
    private TileIndex key;
    /**The probability of replicating a point to level i*/
    private float[] levelProb;
    /**Whether to use adaptive sampling with points or not*/
    private boolean adaptiveSampling;
    /**Width of each tile in pixels*/
    private int tileWidth;
    /**Height of each tile in pixels*/
    private int tileHeight;
    /**Scale^2 at each level of the pyramid*/
    private double scale2[];
    private double scale[];

    private Rectangle fileMBR;

    private Color strokeColor;

    private boolean gradualFade;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      this.tileWidth = job.getInt("tilewidth", 256);
      this.tileHeight = job.getInt("tileheight", 256);
      this.numLevels = job.getInt("numlevels", 1);
      this.fileMBR = SpatialSite.getRectangle(job, InputMBR);
      this.bottomGrid = new GridInfo(fileMBR.x1, fileMBR.y1, fileMBR.x2, fileMBR.y2);
      this.bottomGrid.rows = bottomGrid.columns =
          (int) Math.round(Math.pow(2, numLevels - 1));
      this.key = new TileIndex();
      this.adaptiveSampling = job.getBoolean("sample", false);
      this.levelProb = new float[this.numLevels];
      this.scale2 = new double[numLevels];
      this.scale = new double[numLevels];
      this.levelProb[0] = job.getFloat(GeometricPlot.AdaptiveSampleRatio, 0.1f);
      // Size of the whole file in pixels at the f
      this.scale2[0] = (double)tileWidth * tileHeight /
          (fileMBR.getWidth() * fileMBR.getHeight());
      this.scale[0] = Math.sqrt(scale2[0]);
      for (int level = 1; level < numLevels; level++) {
        this.levelProb[level] = this.levelProb[level - 1] * 4;
        this.scale2[level] = this.scale2[level - 1] *
            (1 << level) * (1 << level);
        this.scale[level] = this.scale[level - 1] * (1 << level);
      }
      this.strokeColor = new Color(job.getInt("color", 0));
      this.gradualFade = job.getBoolean("fade", false);
    }
    
    @Override
    public void map(Rectangle d, ArrayWritable value,
        OutputCollector<TileIndex, ImageWritable> output, Reporter reporter)
        throws IOException {
      for (Shape shape : (Shape[])value.get()) {
        Rectangle shapeMBR = shape.getMBR();
        if (shapeMBR == null)
          return;
        
        int min_level = 0;
        
        if (adaptiveSampling) {
          // Special handling for NASA data
          double p = Math.random();
          // Skip levels that do not satisfy the probability
          while (min_level < numLevels && p > levelProb[min_level])
            min_level++;
        }
        
        java.awt.Rectangle overlappingCells =
            bottomGrid.getOverlappingCells(shapeMBR);
        for (key.level = numLevels - 1; key.level >= min_level; key.level--) {
          if (gradualFade && !(shape instanceof Point)) {
            double areaInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale[key.level];
            if (areaInPixels < 1.0 && Math.round(areaInPixels * 255) < 1.0) {
              // This shape can be safely skipped as it is too small to be plotted
              return;
            }
          }
          for (int i = 0; i < overlappingCells.width; i++) {
            key.x = i + overlappingCells.x;
            for (int j = 0; j < overlappingCells.height; j++) {
              key.y = j + overlappingCells.y;
              // Draw in image associated with this tile
              Graphics2D g = getGraphics(key);
              shape.draw(g, fileMBR, tileWidth * (1 << key.level),
                  tileHeight * (1 << key.level), scale2[key.level]);
            }
          }
          // Shrink overlapping cells to match the upper level
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
      for (Map.Entry<TileIndex, Graphics2D> tileGraph : tileGraphics.entrySet()) {
        tileGraph.getValue().dispose();
      }
      ImageWritable outValue = new ImageWritable();
      for (Map.Entry<TileIndex, BufferedImage> tileImage : tileImages.entrySet()) {
        outValue.setImage(tileImage.getValue());
        output.collect(tileImage.getKey(), outValue);
      }
    }

    private Graphics2D getGraphics(TileIndex tileIndex) {
      Graphics2D g = tileGraphics.get(tileIndex);
      if (g == null) {
        TileIndex key = tileIndex.clone();
        BufferedImage image = new BufferedImage(tileWidth, tileHeight,
            BufferedImage.TYPE_INT_ARGB);
        if (tileImages.put(key, image) != null)
          throw new RuntimeException("Error! Image is already there but graphics is not "+tileIndex);
        
        Color bg_color = new Color(0,0,0,0);

        try {
          g = image.createGraphics();
        } catch (Throwable e) {
          g = new SimpleGraphics(image);
        }
        g.setBackground(bg_color);
        g.clearRect(0, 0, tileWidth, tileHeight);
        g.setColor(strokeColor);
        // Coordinates of this tile in image coordinates
        g.translate(-(tileWidth * tileIndex.x), -(tileHeight * tileIndex.y));

        tileGraphics.put(key, g);
      }
      return g;
    }
  }
  
  public static class DataPartitionReduce extends MapReduceBase
    implements Reducer<TileIndex, ImageWritable, TileIndex, ImageWritable> {

    @Override
    public void reduce(TileIndex cellIndex, Iterator<ImageWritable> images,
        OutputCollector<TileIndex, ImageWritable> output, Reporter reporter)
        throws IOException {
      if (images.hasNext()) {
        ImageWritable mergedImage = images.next();
        BufferedImage image = mergedImage.getImage();
        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        // Overlay all other images on top of it
        while (images.hasNext()) {
          BufferedImage img = images.next().getImage();
          graphics.drawImage(img, 0, 0, null);
        }
        graphics.dispose();

        mergedImage.setImage(image);

        output.collect(cellIndex, mergedImage);
      }
    }
  }

  
  /**
   * Finalizes the job by combining all images of all reduces jobs into one
   * folder.
   * @author Ahmed Eldawy
   *
   */
  public static class PlotPyramidOutputCommitter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);

      JobConf job = context.getJobConf();
      Path outPath = PyramidOutputFormat.getOutputPath(job);
      FileSystem outFs = outPath.getFileSystem(job);

      // Write a default empty image to be displayed for non-generated tiles
      int tileWidth = job.getInt("tilewidth", 256);
      int tileHeight = job.getInt("tileheight", 256);
      BufferedImage emptyImg = new BufferedImage(tileWidth, tileHeight, BufferedImage.TYPE_INT_ARGB);
      Graphics2D g = new SimpleGraphics(emptyImg);
      g.setBackground(new Color(0,0,0,0));
      g.clearRect(0, 0, tileWidth, tileHeight);
      g.dispose();

      OutputStream out = outFs.create(new Path(outPath, "default.png"));
      ImageIO.write(emptyImg, "png", out);
      out.close();

      // Add an HTML file that visualizes the result using Google Maps
      int numLevels = job.getInt("numlevels", 7);
      LineReader templateFileReader = new LineReader(getClass().getResourceAsStream("/zoom_view.html"));
      PrintStream htmlOut = new PrintStream(outFs.create(new Path(outPath, "index.html")));
      Text line = new Text();
      while (templateFileReader.readLine(line) > 0) {
        String lineStr = line.toString();
        lineStr = lineStr.replace("#{TILE_WIDTH}", Integer.toString(tileWidth));
        lineStr = lineStr.replace("#{TILE_HEIGHT}", Integer.toString(tileHeight));
        lineStr = lineStr.replace("#{MAX_ZOOM}", Integer.toString(numLevels-1));

        htmlOut.println(lineStr);
      }
      templateFileReader.close();
      htmlOut.close();
    }
  }
  
  private static void plotLocal(Path inFile, Path outFile,
      OperationsParams params) throws IOException {
    int tileWidth = params.getInt("tilewidth", 256);
    int tileHeight = params.getInt("tileheight", 256);

    Color strokeColor = params.getColor("color", Color.BLACK);

    String hdfDataset = (String) params.get("dataset");
    Shape shape = hdfDataset != null ? new NASARectangle() : (Shape) params.getShape("shape", null);
    Shape plotRange = params.getShape("rect", null);

    String valueRangeStr = (String) params.get("valuerange");
    MinMax valueRange;
    if (valueRangeStr == null) {
      valueRange = null;
    } else {
      String[] parts = valueRangeStr.split(",");
      valueRange = new MinMax(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    InputSplit[] splits;
    FileSystem inFs = inFile.getFileSystem(params);
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFStatus != null && !inFStatus.isDir()) {
      // One file, retrieve it immediately.
      // This is useful if the input is a hidden file which is automatically
      // skipped by FileInputFormat. We need to plot a hidden file for the case
      // of plotting partition boundaries of a spatial index
      splits = new InputSplit[] {new FileSplit(inFile, 0, inFStatus.getLen(), new String[0])};
    } else {
      JobConf job = new JobConf(params);
      ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();
      ShapeInputFormat.addInputPath(job, inFile);
      splits = inputFormat.getSplits(job, 1);
    }

    boolean vflip = params.is("vflip");

    Rectangle fileMBR;
    if (plotRange != null) {
      fileMBR = plotRange.getMBR();
    } else if (hdfDataset != null) {
      // Plotting a NASA file
      fileMBR = new Rectangle(-180, -90, 180, 90);
    } else {
      fileMBR = FileMBR.fileMBR(inFile, params);
    }

    boolean keepAspectRatio = params.is("keep-ratio", true);
    if (keepAspectRatio) {
      // Adjust width and height to maintain aspect ratio
      if (fileMBR.getWidth() > fileMBR.getHeight()) {
        fileMBR.y1 -= (fileMBR.getWidth() - fileMBR.getHeight()) / 2;
        fileMBR.y2 = fileMBR.y1 + fileMBR.getWidth();
      } else {
        fileMBR.x1 -= (fileMBR.getHeight() - fileMBR.getWidth() / 2);
        fileMBR.x2 = fileMBR.x1 + fileMBR.getHeight();
      }
    }

    if (hdfDataset != null) {
      // Collects some stats about the HDF file
      if (valueRange == null)
        valueRange = Aggregate.aggregate(new Path[] {inFile}, params);
      NASAPoint.minValue = valueRange.minValue;
      NASAPoint.maxValue = valueRange.maxValue;
      NASAPoint.setColor1(params.getColor("color1", Color.BLUE));
      NASAPoint.setColor2(params.getColor("color2", Color.RED));
      NASAPoint.gradientType = params.getGradientType("gradient", NASAPoint.GradientType.GT_HUE);
    }
    
    boolean adaptiveSampling = params.getBoolean("sample", false);
    
    int numLevels = params.getInt("numlevels", 7);

    float[] levelProb = new float[numLevels];
    double[] scale2 = new double[numLevels];
    double[] scale = new double[numLevels];
    levelProb[0] = params.getFloat(GeometricPlot.AdaptiveSampleRatio, 0.1f);
    // Size of the whole file in pixels at the f
    
    scale2[0] = (double)tileWidth * tileHeight /
        (fileMBR.getWidth() * fileMBR.getHeight());
    scale[0] = Math.sqrt(scale2[0]);
    for (int level = 1; level < numLevels; level++) {
      levelProb[level] = levelProb[level - 1] * 4;
      scale2[level] = scale2[level - 1] *
          (1 << level) * (1 << level);
      scale[level] = scale[level - 1] * (1 << level);
    }

    Map<TileIndex, BufferedImage> tileImages =
        new HashMap<PyramidPlot.TileIndex, BufferedImage>();
    
    Map<TileIndex, Graphics2D> tileGraphics =
        new HashMap<PyramidPlot.TileIndex, Graphics2D>();
    
    GridInfo bottomGrid = new GridInfo(fileMBR.x1, fileMBR.y1,
        fileMBR.x2, fileMBR.y2);
    bottomGrid.rows = bottomGrid.columns =
        (int) Math.round(Math.pow(2, numLevels - 1));
    
    TileIndex tileIndex = new TileIndex();
    boolean gradualFade = !(shape instanceof Point) && params.getBoolean("fade", false);
    
    for (InputSplit split : splits) {
      ShapeRecordReader<Shape> reader = new ShapeRecordReader<Shape>(params,
          (FileSplit)split);
      Rectangle cell = reader.createKey();
      while (reader.next(cell, shape)) {
        Rectangle shapeMBR = shape.getMBR();
        if (shapeMBR != null) {
          int min_level = 0;
          
          if (adaptiveSampling) {
            // Special handling for NASA data
            double p = Math.random();
            // Skip levels that do not satisfy the probability
            while (min_level < numLevels && p > levelProb[min_level])
              min_level++;
          }
          
          java.awt.Rectangle overlappingCells =
              bottomGrid.getOverlappingCells(shapeMBR);
          for (tileIndex.level = numLevels - 1; tileIndex.level >= min_level; tileIndex.level--) {
            if (gradualFade && !(shape instanceof Point)) {
              double areaInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale[tileIndex.level];
              if (areaInPixels < 1.0 && Math.round(areaInPixels * 255) < 1.0) {
                // This shape can be safely skipped as it is too small to be plotted
                return;
              }
            }

            for (int i = 0; i < overlappingCells.width; i++) {
              tileIndex.x = i + overlappingCells.x;
              for (int j = 0; j < overlappingCells.height; j++) {
                tileIndex.y = j + overlappingCells.y;
                // Draw in image associated with this tile
                Graphics2D g;
                {
                  g = tileGraphics.get(tileIndex);
                  if (g == null) {
                    TileIndex key = tileIndex.clone();
                    BufferedImage image = new BufferedImage(tileWidth, tileHeight,
                        BufferedImage.TYPE_INT_ARGB);
                    if (tileImages.put(key, image) != null)
                      throw new RuntimeException("Error! Image is already there but graphics is not "+tileIndex);
                    
                    Color bg_color = new Color(0,0,0,0);

                    try {
                      g = image.createGraphics();
                    } catch (Throwable e) {
                      g = new SimpleGraphics(image);
                    }
                    g.setBackground(bg_color);
                    g.clearRect(0, 0, tileWidth, tileHeight);
                    g.setColor(strokeColor);
                    // Coordinates of this tile in image coordinates
                    g.translate(-(tileWidth * tileIndex.x), -(tileHeight * tileIndex.y));

                    tileGraphics.put(key, g);
                  }
                }
                
                shape.draw(g, fileMBR, tileWidth * (1 << tileIndex.level),
                    tileHeight * (1 << tileIndex.level), scale2[tileIndex.level]);
              }
            }
            // Shrink overlapping cells to match the upper level
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
      }
      reader.close();
    }
    // Write image to output
    for (Map.Entry<TileIndex, Graphics2D> tileGraph : tileGraphics.entrySet()) {
      tileGraph.getValue().dispose();
    }
    FileSystem outFS = outFile.getFileSystem(params);
    for (Map.Entry<TileIndex, BufferedImage> tileImage : tileImages.entrySet()) {
      tileIndex = tileImage.getKey();
      BufferedImage image = tileImage.getValue();
      if (vflip) {
        AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
        tx.translate(0, -image.getHeight());
        AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        image = op.filter(image, null);
        tileIndex.y = ((1 << tileIndex.level) - 1) - tileIndex.y;
      }
      Path imagePath = new Path(outFile, tileIndex.getImageFileName());
      FSDataOutputStream outStream = outFS.create(imagePath);
      ImageIO.write(image, "png", outStream);
      outStream.close();
    }
  }

  /**Last submitted job of type PlotPyramid*/
  public static RunningJob lastSubmittedJob;

  /**
   * Plot a file to a set of images in different zoom levels using a MapReduce
   * program.
   * @param <S> type of shapes stored in file
   * @param inFile - Path to the input file(s)
   * @param outFile - Path to the output file (image)
   * @param shape - A sample object to be used for parsing input file
   * @param tileWidth - With of each tile 
   * @param tileHeight - Height of each tile
   * @param vflip - Set to <code>true</code> to file the whole image vertically
   * @param color - Color used to draw single shapes
   * @param numLevels - Number of zoom levels to plot
   * @throws IOException
   */
  private static <S extends Shape> RunningJob plotMapReduce(Path inFile,
      Path outFile, OperationsParams params) throws IOException {
    Color color = params.getColor("color", Color.BLACK);
    
    String hdfDataset = (String) params.get("dataset");
    Shape shape = hdfDataset != null ? new NASARectangle() : params.getShape("shape");
    Shape plotRange = params.getShape("rect");

    boolean background = params.is("background");
    
    JobConf job = new JobConf(params, PyramidPlot.class);
    job.setJobName("PlotPyramid");
    
    String partition = job.get("partition", "space").toLowerCase();
    if (partition.equals("space")) {
      job.setMapperClass(SpacePartitionMap.class);
      job.setReducerClass(SpacePartitionReduce.class);
      job.setMapOutputKeyClass(TileIndex.class);
      job.setMapOutputValueClass(shape.getClass());
      job.setInputFormat(ShapeInputFormat.class);
    } else {
      job.setMapperClass(DataPartitionMap.class);
      job.setReducerClass(DataPartitionReduce.class);
      job.setMapOutputKeyClass(TileIndex.class);
      job.setMapOutputValueClass(ImageWritable.class);
      job.setInputFormat(ShapeArrayInputFormat.class);
    }

    job.setInt("color", color.getRGB());
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
    
    if (shape instanceof Point && job.getBoolean("sample", false)) {
      // Enable adaptive sampling
      int imageWidthRoot = job.getInt("tilewidth", 256);
      int imageHeightRoot = job.getInt("tileheight", 256);
      long recordCount = FileMBR.fileMBR(inFile, params).recordCount;
      float sampleRatio = params.getFloat(GeometricPlot.AdaptiveSampleFactor, 1.0f) *
          imageWidthRoot * imageHeightRoot / recordCount;
      job.setFloat(GeometricPlot.AdaptiveSampleRatio, sampleRatio);
    }

    
    Rectangle fileMBR;
    if (hdfDataset != null) {
      // Input is HDF
      job.set(HDFRecordReader.DatasetName, hdfDataset);
      job.setBoolean(HDFRecordReader.SkipFillValue, true);
      job.setClass("shape", NASARectangle.class, Shape.class);
      // Determine the range of values by opening one of the HDF files
      Aggregate.MinMax minMax = Aggregate.aggregate(new Path[] {inFile}, params);
      job.setInt(MinValue, minMax.minValue);
      job.setInt(MaxValue, minMax.maxValue);
      //fileMBR = new Rectangle(-180, -90, 180, 90);
      fileMBR = plotRange != null?
          plotRange.getMBR() : new Rectangle(-180, -140, 180, 169);
//      job.setClass(HDFRecordReader.ProjectorClass, MercatorProjector.class,
//          GeoProjector.class);
    } else {
      fileMBR = FileMBR.fileMBR(inFile, params);
    }
    
    boolean keepAspectRatio = params.is("keep-ratio", true);
    if (keepAspectRatio) {
      // Expand input file to a rectangle for compatibility with the pyramid
      // structure
      if (fileMBR.getWidth() > fileMBR.getHeight()) {
        fileMBR.y1 -= (fileMBR.getWidth() - fileMBR.getHeight()) / 2;
        fileMBR.y2 = fileMBR.y1 + fileMBR.getWidth();
      } else {
        fileMBR.x1 -= (fileMBR.getHeight() - fileMBR.getWidth() / 2);
        fileMBR.x2 = fileMBR.x1 + fileMBR.getHeight();
      }
    }

    SpatialSite.setRectangle(job, InputMBR, fileMBR);
    
    // Set input and output
    ShapeInputFormat.addInputPath(job, inFile);
    if (plotRange != null) {
      job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    }
    
    job.setOutputFormat(PyramidOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outFile);
    job.setOutputCommitter(PlotPyramidOutputCommitter.class);
    
    if (background) {
      JobClient jc = new JobClient(job);
      return lastSubmittedJob = jc.submitJob(job);
    } else {
      return lastSubmittedJob = JobClient.runJob(job);
    }

  }
  
  public static <S extends Shape> void plot(Path inFile, Path outFile, OperationsParams params)
          throws IOException {
    if (params.getBoolean("local", false)) {
      plotLocal(inFile, outFile, params);
    } else {
      plotMapReduce(inFile, outFile, params);
    }
  }

  
  private static void printUsage() {
    System.out.println("Plots a file to a set of images that form a pyramid to be used by Google Maps or a similar engine");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("tilewidth:<w> - Width of each tile in pixels");
    System.out.println("tileheight:<h> - Height of each tile in pixels");
    System.out.println("color:<c> - Main color used to draw shapes (black)");
    System.out.println("numlevels:<n> - Number of levels in the pyrmaid");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("rect:<x1,y1,x2,y2> - Limit drawing to the selected area");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    OperationsParams cla = new OperationsParams(new GenericOptionsParser(args));
    if (!cla.checkInputOutput()) {
      printUsage();
      return;
    }
    Path inFile = cla.getInputPath();
    Path outFile = cla.getOutputPath();
    long t1 = System.currentTimeMillis();
    plot(inFile, outFile, cla);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time for plot pyramid "+(t2-t1)+" millis");
  }

}
