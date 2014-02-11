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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputCommitter;
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
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.PyramidOutputFormat;
import edu.umn.cs.spatialHadoop.SimpleGraphics;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.GeoProjector;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.nasa.MercatorProjector;
import edu.umn.cs.spatialHadoop.nasa.NASADataset;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;
import edu.umn.cs.spatialHadoop.nasa.NASARectangle;
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
  /**Color uses to plot shapes and points*/
  private static final String StrokeColor = "plot.stroke_color";
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
    private int tileWidth, tileHeight;
    private InputSplit currentSplit;
    /**The probability of replicating a point to level i*/
    private double[] levelProb;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.numLevels = job.getInt(NumLevels, 1);
      Rectangle inputMBR = SpatialSite.getRectangle(job, InputMBR);
      this.bottomGrid = new GridInfo(inputMBR.x1, inputMBR.y1, inputMBR.x2, inputMBR.y2);
      this.bottomGrid.rows = bottomGrid.columns =
          (int) Math.round(Math.pow(2, numLevels - 1));
      this.key = new TileIndex();
      this.tileWidth = job.getInt(TileWidth, 256);
      this.tileHeight = job.getInt(TileHeight, 256);
      this.currentSplit = null;
      this.levelProb = new double[this.numLevels];
    }
    
    public void map(Rectangle cell, Shape shape,
        OutputCollector<TileIndex, Shape> output, Reporter reporter)
        throws IOException {
      if (currentSplit != reporter.getInputSplit()) {
        this.currentSplit = reporter.getInputSplit();
        if (cell instanceof NASADataset) {
          // Calculate the ratio of replicating a point to each level
          NASADataset dataset = (NASADataset) cell;
          this.levelProb[0] = 1.0 *
              // Number of pixels in one tile
              (double) tileWidth * tileHeight /
              // Area of a tile at level 0
              (bottomGrid.getWidth() * bottomGrid.getHeight()) /(
              // Number of points in file
              (double) dataset.resolution * dataset.resolution /
              // Area of file
              (dataset.getWidth() * dataset.getHeight())
              );
          for (int level = 1; level < numLevels; level++) {
            this.levelProb[level] = this.levelProb[level - 1] * 4;
          }
        }
      }
      Rectangle shapeMBR = shape.getMBR();
      if (shapeMBR == null)
        return;
      
      int min_level = 0;
//      if (cell instanceof NASADataset) {
//        // Special handling for NASA data
//        double p = Math.random();
//        // Skip levels that do not satisfy the probability
//        while (min_level < numLevels && p > levelProb[min_level])
//          min_level++;
//      }
      
      java.awt.Rectangle overlappingCells =
          bottomGrid.getOverlappingCells(shapeMBR);
      for (key.level = numLevels - 1; key.level >= min_level; key.level--) {
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
  public static class PlotReduce extends MapReduceBase
      implements Reducer<TileIndex, Shape, TileIndex, ImageWritable> {
    
    private Rectangle fileMBR;
    private int tileWidth, tileHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2;
    private boolean vflip;
    private Color strokeColor;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      fileMBR = SpatialSite.getRectangle(job, InputMBR);
      tileWidth = job.getInt(TileWidth, 256);
      tileHeight = job.getInt(TileHeight, 256);
      this.vflip = job.getBoolean(ImageOutputFormat.VFlip, false);
      if (vflip) {
        double temp = this.fileMBR.y1;
        this.fileMBR.y1 = -this.fileMBR.y2;
        this.fileMBR.y2 = -temp;
      }
      this.strokeColor = new Color(job.getInt(StrokeColor, 0));
      NASAPoint.minValue = job.getInt(MinValue, 0);
      NASAPoint.maxValue = job.getInt(MaxValue, 65535);
    }

    @Override
    public void reduce(TileIndex tileIndex, Iterator<Shape> values,
        OutputCollector<TileIndex, ImageWritable> output, Reporter reporter)
        throws IOException {
      if (vflip)
        tileIndex = new TileIndex(tileIndex.level, tileIndex.x, ((1 << tileIndex.level) - 1) - tileIndex.y);
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
          s.draw(graphics, fileMBR, imageWidth, imageHeight, vflip, scale2);
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

      // Collect all output folders
      FileStatus[] reducesOut = outFs.listStatus(outPath, SpatialSite.NonHiddenFileFilter);
      
      if (reducesOut.length == 0) {
        LOG.warn("No output files were written by reducers");
      } else {
        // Write a default empty image to be displayed for non-generated tiles
        int tileWidth = job.getInt(TileWidth, 256);
        int tileHeight = job.getInt(TileHeight, 256);
        BufferedImage emptyImg = new BufferedImage(tileWidth, tileHeight, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = new SimpleGraphics(emptyImg);
        g.setBackground(new Color(0,0,0,0));
        g.clearRect(0, 0, tileWidth, tileHeight);
        g.dispose();
        
        OutputStream out = outFs.create(new Path(outPath, "default.png"));
        ImageIO.write(emptyImg, "png", out);
        out.close();
        
        // Add an HTML file that visualizes the result using Google Maps
        int numLevels = job.getInt(NumLevels, 7);
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
  }

  private static RunningJob lastSubmittedJob;

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
  public static <S extends Shape> RunningJob plotMapReduce(Path inFile,
      Path outFile, Shape shape, int tileWidth, int tileHeight, boolean vflip,
      Color color, int numLevels, String hdfDataset, Rectangle plotRange,
      boolean keepAspectRatio, boolean background) throws IOException {
    JobConf job = new JobConf(PlotPyramid.class);
    job.setJobName("PlotPyramid");
    
    job.setMapperClass(PlotMap.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
    SpatialSite.setShapeClass(job, shape.getClass());
    job.setMapOutputKeyClass(TileIndex.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setInt(StrokeColor, color.getRGB());

    job.setReducerClass(PlotReduce.class);
    
    FileSystem inFs = inFile.getFileSystem(job);

    Rectangle fileMBR;
    if (hdfDataset != null) {
      // Input is HDF
      job.set(HDFRecordReader.DatasetName, hdfDataset);
      job.setBoolean(HDFRecordReader.SkipFillValue, true);
      // Determine the range of values by opening one of the HDF files
      Aggregate.MinMax minMax = Aggregate.aggregate(inFs, new Path[] {inFile}, plotRange, false);
      job.setInt(MinValue, minMax.minValue);
      job.setInt(MaxValue, minMax.maxValue);
      //fileMBR = new Rectangle(-180, -90, 180, 90);
      fileMBR = new Rectangle(-180, -140, 180, 169);
      job.setClass(HDFRecordReader.ProjectorClass, MercatorProjector.class,
          GeoProjector.class);
    } else {
      fileMBR = FileMBR.fileMBR(inFs, inFile, shape);
    }
    
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
    job.setInt(TileWidth, tileWidth);
    job.setInt(TileHeight, tileHeight);
    job.setInt(NumLevels, numLevels);
    job.setBoolean(ImageOutputFormat.VFlip, vflip);
    
    // Set input and output
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    if (plotRange != null) {
      job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
      RangeFilter.setQueryRange(job, plotRange); // Set query range for filter
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
  
  public static <S extends Shape> void plot(Path inFile, Path outFile, S shape,
      int tileWidth, int tileHeight, boolean vflip, Color color, int numLevels,
      String hdfDataset, Rectangle plotRange, boolean keepAspectRatio, boolean background)
          throws IOException {
    plotMapReduce(inFile, outFile, shape, tileWidth, tileHeight, vflip, color, numLevels, hdfDataset, plotRange, keepAspectRatio, background);
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
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(PlotPyramid.class);
    if (!cla.checkInputOutput(conf)) {
      printUsage();
      return;
    }
    Path[] files = cla.getPaths();
    Path inFile = files[0];
    Path outFile = files[1];

    int tileWidth = cla.getInt("tilewidth", 256);
    int tileHeight = cla.getInt("tileheight", 256);
    int numLevels = cla.getInt("numlevels", 8);
    boolean vflip = cla.is("vflip");
    Color color = cla.getColor();
    
    String hdfDataset = cla.get("dataset");
    Shape shape = hdfDataset != null ? new NASARectangle() : cla.getShape(true);
    Rectangle plotRange = cla.getRectangle();

    boolean keepAspectRatio = cla.is("keep-ratio", true);
    boolean background = cla.is("background");
    
    plot(inFile, outFile, shape, tileWidth, tileHeight, vflip, color,
        numLevels, hdfDataset, plotRange, keepAspectRatio, background);
  }

}
