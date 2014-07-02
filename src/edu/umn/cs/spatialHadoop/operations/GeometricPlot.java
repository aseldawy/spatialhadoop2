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
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.SimpleGraphics;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Partition;
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
import edu.umn.cs.spatialHadoop.nasa.NASADataset;
import edu.umn.cs.spatialHadoop.nasa.NASAPoint;
import edu.umn.cs.spatialHadoop.nasa.NASARectangle;
import edu.umn.cs.spatialHadoop.nasa.NASAShape;
import edu.umn.cs.spatialHadoop.operations.Aggregate.MinMax;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeFilter;

/**
 * Draws an image of all shapes in an input file.
 * 
 * This operation has three implementations.
 * 1) Fast Plot. This operation uses data partitioning to draw an image for each
 *   split and then overlay images on top of each other to produce final result.
 *   To use this method, you need to set the parameter "partition:data"
 * 2) Partitioned Plot. This implementation uses space partitioning to
 *   partition the data and then plots an image for each partition. After that,
 *   it stitches these images together to produce the final picture.
 *   To use this method, set the parameter "partition:space"
 * 3) If the data is already spatially partitioned using an R+-tree or
 *   Grid file, and "partition:space" is provided, this algorithm uses the
 *   existing partitioning to draw an image for each partition and then combine
 *   the results back in one machine by stitching them.
 * @author Ahmed Eldawy
 *
 */
public class GeometricPlot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(GeometricPlot.class);

  /**The grid used to partition data across reducers*/
  private static final String PartitionGrid = "plot.partition_grid";

  /**Sample ratio to use when adaptive sample is turned on*/
  public static final String AdaptiveSampleRatio = "Plot.AdaptiveSampleRatio";
  /**Base sample factor used to increase probability if needed (defaults to 1)*/
  public static final String AdaptiveSampleFactor = "Plot.AdaptiveSample.Factor";

  /**
   * If the processed block is already partitioned (via global index), then
   * the output is the same as input (Identity map function). If the input
   * partition is not partitioned (a heap file), then the given shape is output
   * to all overlapping partitions.
   * @author Ahmed Eldawy
   *
   */
  public static class GridPartitionMap extends MapReduceBase 
  implements Mapper<Rectangle, Shape, IntWritable, Shape> {

    private GridInfo partitionGrid;
    private IntWritable cellNumber;
    private Shape queryRange;
    private Rectangle fileMbr;
    private int imageWidth, imageHeight;
    private double scale2, scale;
    private boolean gradualFade;
    private boolean adaptiveSample;
    private float adaptiveSampleRatio;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.partitionGrid = (GridInfo) OperationsParams.getShape(job, PartitionGrid);
      this.cellNumber = new IntWritable();
      this.queryRange = OperationsParams.getShape(job, "rect");
      this.gradualFade = job.getBoolean("fade", false);
      this.fileMbr = ImageOutputFormat.getFileMBR(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.scale2 = (double)imageWidth * imageHeight /
          (this.fileMbr.getWidth() * this.fileMbr.getHeight());
      this.scale = Math.sqrt(this.scale2);
      this.adaptiveSample = job.getBoolean("sample", false);
      if (this.adaptiveSample) {
        // Calculate the sample ratio
        this.adaptiveSampleRatio = job.getFloat(AdaptiveSampleRatio, 0.01f);
      }
    }

    public void map(Rectangle cell, Shape shape,
        OutputCollector<IntWritable, Shape> output, Reporter reporter)
            throws IOException {
      Rectangle shapeMbr = shape.getMBR();
      if (shapeMbr == null)
        return;
      // Check if this shape can be skipped using the gradual fade option
      if (gradualFade && !(shape instanceof Point)) {
        double areaInPixels = (shapeMbr.getWidth() + shapeMbr.getHeight()) * scale;
        if (areaInPixels < 1.0 && Math.round(areaInPixels * 255) < 1.0) {
          // This shape can be safely skipped as it is too small to be plotted
          return;
        }
      }
      if (adaptiveSample && shape instanceof Point) {
        if (Math.random() > adaptiveSampleRatio)
          return;
      }

      // Skip shapes outside query range if query range is set
      if (queryRange != null && !shapeMbr.isIntersected(queryRange))
        return;
      java.awt.Rectangle overlappingCells = partitionGrid.getOverlappingCells(shapeMbr);
      for (int i = 0; i < overlappingCells.width; i++) {
        int x = overlappingCells.x + i;
        for (int j = 0; j < overlappingCells.height; j++) {
          int y = overlappingCells.y + j;
          cellNumber.set(y * partitionGrid.columns + x + 1);
          output.collect(cellNumber, shape);
        }
      }
    }
  }

  /**
   * The reducer class draws an image for contents (shapes) in each cell info
   * @author Ahmed Eldawy
   *
   */
  public static class GridPartitionReduce extends MapReduceBase
  implements Reducer<IntWritable, Shape, Rectangle, ImageWritable> {

    private GridInfo partitionGrid;
    private Rectangle fileMbr;
    private int imageWidth, imageHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2, scale;
    private int strokeColor;
    private boolean fade;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      this.partitionGrid = (GridInfo) OperationsParams.getShape(job, PartitionGrid);
      this.fileMbr = ImageOutputFormat.getFileMBR(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.strokeColor = job.getInt("color", 0);
      this.fade = job.getBoolean("fade", false);

      this.scale2 = (double)imageWidth * imageHeight /
          (this.fileMbr.getWidth() * this.fileMbr.getHeight());
      this.scale = Math.sqrt(scale2);

      String valueRangeStr = job.get("valuerange");
      if (valueRangeStr != null) {
        String[] parts = valueRangeStr.contains("..") ? valueRangeStr.split("\\.\\.", 2) : valueRangeStr.split(",", 2);
        NASAPoint.minValue = Integer.parseInt(parts[0]);
        NASAPoint.maxValue = Integer.parseInt(parts[1]);
      }

      NASAPoint.setColor1(OperationsParams.getColor(job, "color1", Color.BLUE));
      NASAPoint.setColor2(OperationsParams.getColor(job, "color2", Color.RED));
      NASAPoint.gradientType = OperationsParams.getGradientType(job, "gradient", NASAPoint.GradientType.GT_HUE);
    }

    @Override
    public void reduce(IntWritable cellNumber, Iterator<Shape> values,
        OutputCollector<Rectangle, ImageWritable> output, Reporter reporter)
            throws IOException {
      try {
        CellInfo cellInfo = partitionGrid.getCell(cellNumber.get());
        // Initialize the image
        int image_x1 = (int) Math.floor((cellInfo.x1 - fileMbr.x1) * imageWidth / fileMbr.getWidth());
        int image_y1 = (int) Math.floor((cellInfo.y1 - fileMbr.y1) * imageHeight / fileMbr.getHeight());
        int image_x2 = (int) Math.ceil((cellInfo.x2 - fileMbr.x1) * imageWidth / fileMbr.getWidth());
        int image_y2 = (int) Math.ceil((cellInfo.y2 - fileMbr.y1) * imageHeight / fileMbr.getHeight());
        int tile_width = image_x2 - image_x1;
        int tile_height = image_y2 - image_y1;

        BufferedImage image = new BufferedImage(tile_width, tile_height,
            BufferedImage.TYPE_INT_ARGB);

        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        graphics.setBackground(new Color(0, 0, 0, 0));
        graphics.clearRect(0, 0, tile_width, tile_height);
        Color strokeClr = new Color(strokeColor);
        graphics.setColor(strokeClr);
        graphics.translate(-image_x1, -image_y1);

        while (values.hasNext()) {
          Shape s = values.next();
          if (fade) {
            Rectangle shapeMBR = s.getMBR();
            double areaInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale;
            if (areaInPixels > 1.0) {
              graphics.setColor(strokeClr);
            } else {
              byte alpha = (byte) Math.round(areaInPixels * 255);
              if (alpha == 0) {
                // Skip this shape
                continue;
              } else {
                graphics.setColor(new Color(((int)alpha << 24) | strokeColor, true));
              }
            }
          }
          s.draw(graphics, fileMbr, imageWidth, imageHeight, scale2);
        }

        graphics.dispose();

        sharedValue.setImage(image);
        output.collect(cellInfo, sharedValue);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  /**
   * Partitions the data according to cells configured in the job.
   * Automatically prunes records that will not make it to the final image
   * based on gradual fade and adaptive sampling features.
   * @author Ahmed Eldawy
   */
  public static class SkewedPartitionMap extends MapReduceBase implements
  Mapper<Rectangle, Shape, IntWritable, Shape> {

    /**The cells used to partition the space*/
    private CellInfo[] cells;
    private IntWritable cellNumber;
    private Shape queryRange;
    private Rectangle fileMbr;
    private int imageWidth, imageHeight;
    private double scale2, scale;
    private boolean gradualFade;
    private boolean adaptiveSample;
    private float adaptiveSampleRatio;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      try {
        this.cells = SpatialSite.getCells(job);
      } catch (IOException e) {
        throw new RuntimeException("Cannot get cells for the job", e);
      }
      this.cellNumber = new IntWritable();
      this.queryRange = OperationsParams.getShape(job, "rect");
      this.gradualFade = job.getBoolean("fade", false);
      this.adaptiveSample = job.getBoolean("sample", false);
      this.fileMbr = ImageOutputFormat.getFileMBR(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.scale2 = (double)imageWidth * imageHeight /
          (this.fileMbr.getWidth() * this.fileMbr.getHeight());
      this.scale = Math.sqrt(this.scale2);
      if (this.adaptiveSample) {
        // Calculate the sample ratio
        this.adaptiveSampleRatio = job.getFloat(AdaptiveSampleRatio, 0.01f);
      }
    }

    public void map(Rectangle dummy, Shape shape,
        OutputCollector<IntWritable, Shape> output, Reporter reporter)
            throws IOException {
      Rectangle shapeMBR = shape.getMBR();
      if (shapeMBR == null)
        return;
      // Check if this shape can be skipped using the gradual fade option
      if (gradualFade && !(shape instanceof Point)) {
        double areaInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale;
        if (areaInPixels < 1.0 && Math.round(areaInPixels * 255) < 1.0) {
          // This shape can be safely skipped as it is too small to be plotted
          return;
        }
      }
      if (adaptiveSample && shape instanceof Point) {
        if (Math.random() > adaptiveSampleRatio)
          return;
      }

      // Skip shapes outside query range if query range is set
      if (queryRange != null && !shapeMBR.isIntersected(queryRange))
        return;
      for (CellInfo cell : cells) {
        if (cell.isIntersected(shapeMBR)) {
          cellNumber.set(cell.cellId);
          output.collect(cellNumber, shape);
        }
      }
    }
  }


  /**
   * Draws an image for each cell in cells configured in the job.
   * @author Ahmed Eldawy
   *
   */
  public static class SkewedPartitionReduce extends MapReduceBase
  implements Reducer<IntWritable, Shape, Rectangle, ImageWritable> {

    private CellInfo[] cells;
    private Rectangle fileMbr;
    private int imageWidth, imageHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2, scale;
    private int strokeColor;
    private boolean fade;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      try {
        this.cells = SpatialSite.getCells(job);
      } catch (IOException e) {
        throw new RuntimeException("Cannot get cells for the job", e);
      }
      this.fileMbr = ImageOutputFormat.getFileMBR(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.strokeColor = job.getInt("color", 0);
      this.fade = job.getBoolean("fade", false);

      this.scale2 = (double)imageWidth * imageHeight /
          (this.fileMbr.getWidth() * this.fileMbr.getHeight());
      this.scale = Math.sqrt(scale2);

      String valueRangeStr = job.get("valuerange");
      if (valueRangeStr != null) {
        String[] parts = valueRangeStr.contains("..") ? valueRangeStr.split("\\.\\.", 2) : valueRangeStr.split(",", 2);
        NASAPoint.minValue = Integer.parseInt(parts[0]);
        NASAPoint.maxValue = Integer.parseInt(parts[1]);
      }

      NASAPoint.setColor1(OperationsParams.getColor(job, "color1", Color.BLUE));
      NASAPoint.setColor2(OperationsParams.getColor(job, "color2", Color.RED));
      NASAPoint.gradientType = OperationsParams.getGradientType(job, "gradient", NASAPoint.GradientType.GT_HUE);
    }

    @Override
    public void reduce(IntWritable cellNumber, Iterator<Shape> values,
        OutputCollector<Rectangle, ImageWritable> output, Reporter reporter)
            throws IOException {
      try {
        CellInfo cellInfo = null;
        int iCell = 0;
        while (iCell < cells.length && cells[iCell].cellId != cellNumber.get())
          iCell++;
        if (iCell >= cells.length)
          throw new RuntimeException("Cannot find cell: "+cellNumber);
        cellInfo = cells[iCell];
        // Initialize the image
        int image_x1 = (int) Math.floor((cellInfo.x1 - fileMbr.x1) * imageWidth / fileMbr.getWidth());
        int image_y1 = (int) Math.floor((cellInfo.y1 - fileMbr.y1) * imageHeight / fileMbr.getHeight());
        int image_x2 = (int) Math.ceil((cellInfo.x2 - fileMbr.x1) * imageWidth / fileMbr.getWidth());
        int image_y2 = (int) Math.ceil((cellInfo.y2 - fileMbr.y1) * imageHeight / fileMbr.getHeight());
        int tile_width = image_x2 - image_x1;
        int tile_height = image_y2 - image_y1;

        BufferedImage image = new BufferedImage(tile_width, tile_height,
            BufferedImage.TYPE_INT_ARGB);

        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        graphics.setBackground(new Color(0, 0, 0, 0));
        graphics.clearRect(0, 0, tile_width, tile_height);
        Color strokeClr = new Color(strokeColor);
        graphics.setColor(strokeClr);
        graphics.translate(-image_x1, -image_y1);

        while (values.hasNext()) {
          Shape s = values.next();
          if (fade) {
            Rectangle shapeMBR = s.getMBR();
            double areaInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale;
            if (areaInPixels > 1.0) {
              graphics.setColor(strokeClr);
            } else {
              byte alpha = (byte) Math.round(areaInPixels * 255);
              if (alpha == 0) {
                // Skip this shape
                continue;
              } else {
                graphics.setColor(new Color(((int)alpha << 24) | strokeColor, true));
              }
            }
          }
          s.draw(graphics, fileMbr, imageWidth, imageHeight, scale2);
        }

        graphics.dispose();

        sharedValue.setImage(image);
        output.collect(cellInfo, sharedValue);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }
  
  /**
   * If the processed block is already partitioned (via global index), then
   * the output is the same as input (Identity map function). If the input
   * partition is not partitioned (a heap file), then the given shape is output
   * to all overlapping partitions.
   * @author Ahmed Eldawy
   *
   */
  public static class AlreadyPartitionedMap extends MapReduceBase 
  implements Mapper<Rectangle, ArrayWritable, Rectangle, ImageWritable> {

    private Rectangle fileMBR;
    private int imageWidth, imageHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2, scale;
    private int strokeColor;
    private boolean fade;
    private boolean adaptiveSample;
    private float adaptiveSampleRatio;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      this.fileMBR = ImageOutputFormat.getFileMBR(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.strokeColor = job.getInt("color", 0);
      this.fade = job.getBoolean("fade", false);

      this.scale2 = (double)imageWidth * imageHeight /
          (this.fileMBR.getWidth() * this.fileMBR.getHeight());
      this.scale = Math.sqrt(scale2);

      String valueRangeStr = job.get("valuerange");
      if (valueRangeStr != null) {
        String[] parts = valueRangeStr.contains("..") ? valueRangeStr.split("\\.\\.", 2) : valueRangeStr.split(",", 2);
        NASAPoint.minValue = Integer.parseInt(parts[0]);
        NASAPoint.maxValue = Integer.parseInt(parts[1]);
      }

      NASAPoint.setColor1(OperationsParams.getColor(job, "color1", Color.BLUE));
      NASAPoint.setColor2(OperationsParams.getColor(job, "color2", Color.RED));
      NASAPoint.gradientType = OperationsParams.getGradientType(job, "gradient", NASAPoint.GradientType.GT_HUE);
      
      this.adaptiveSample = job.getBoolean("sample", false);
      if (this.adaptiveSample) {
        // Calculate the sample ratio
        this.adaptiveSampleRatio = job.getFloat(AdaptiveSampleRatio, 0.01f);
      }
    }

    @Override
    public void map(Rectangle key, ArrayWritable value,
        OutputCollector<Rectangle, ImageWritable> output, Reporter reporter)
            throws IOException {
      // Initialize the image
      int image_x1 = (int) Math.floor((key.x1 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
      int image_y1 = (int) Math.floor((key.y1 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
      int image_x2 = (int) Math.ceil((key.x2 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
      int image_y2 = (int) Math.ceil((key.y2 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
      int tile_width = image_x2 - image_x1;
      int tile_height = image_y2 - image_y1;

      // Initialize the image
      BufferedImage image = new BufferedImage(tile_width, tile_height, BufferedImage.TYPE_INT_ARGB);
      Graphics2D graphics = image.createGraphics();
      graphics.setBackground(new Color(0, 0, 0, 0));
      graphics.clearRect(0, 0, tile_width, tile_height);
      Color strokeClr = new Color(strokeColor);
      graphics.setColor(strokeClr);
      graphics.translate(-image_x1, -image_y1);

      for (Shape s : (Shape[]) value.get()) {
        if (fade) {
          Rectangle shapeMBR = s.getMBR();
          double areaInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale;
          if (areaInPixels > 1.0) {
            graphics.setColor(strokeClr);
          } else {
            byte alpha = (byte) Math.round(areaInPixels * 255);
            if (alpha == 0) {
              // Skip this shape
              continue;
            } else {
              graphics.setColor(new Color(((int)alpha << 24) | strokeColor, true));
            }
          }
        }
        if (adaptiveSample && s instanceof Point) {
          if (Math.random() > adaptiveSampleRatio)
            return;
        }
        s.draw(graphics, fileMBR, imageWidth, imageHeight, scale2);
      }
      graphics.dispose();
      sharedValue.setImage(image);
      output.collect(key.getMBR(), sharedValue);
    }
  }

  public static class PlotOutputCommitter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);

      JobConf job = context.getJobConf();
      Path outFile = ImageOutputFormat.getOutputPath(job);
      int width = job.getInt("width", 1000);
      int height = job.getInt("height", 1000);
      boolean vflip = job.getBoolean("vflip", false);

      // Combine all images in one file
      // Rename output file
      // Combine all output files into one file as we do with grid files
      FileSystem outFs = outFile.getFileSystem(job);
      Path temp = new Path(outFile.toUri().getPath()+"_temp");
      outFs.rename(outFile, temp);
      FileStatus[] resultFiles = outFs.listStatus(temp, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.toUri().getPath().contains("part-");
        }
      });

      // Merge all images into one image (overlay)
      BufferedImage finalImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
      Graphics2D graphics;
      try {
        graphics = finalImage.createGraphics();
      } catch (Throwable e) {
        graphics = new SimpleGraphics(finalImage);
      }
      Color bgColor = OperationsParams.getColor(job, "bgcolor", new Color(0, 0, 0, 0));
      graphics.setBackground(bgColor);
      graphics.clearRect(0, 0, width, height);

      for (FileStatus resultFile : resultFiles) {
        FSDataInputStream imageFile = outFs.open(resultFile.getPath());
        BufferedImage tileImage = ImageIO.read(imageFile);
        imageFile.close();

        graphics.drawImage(tileImage, 0, 0, null);
      }
      graphics.dispose();

      // Flip image vertically if needed
      if (vflip) {
        AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
        tx.translate(0, -finalImage.getHeight());
        AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        finalImage = op.filter(finalImage, null);
      }

      // Finally, write the resulting image to the given output path
      LOG.info("Writing final image");
      OutputStream outputImage = outFs.create(outFile);
      ImageIO.write(finalImage, "png", outputImage);
      outputImage.close();

      outFs.delete(temp, true);
    }
  }


  /**
   * If the processed block is already partitioned (via global index), then
   * the output is the same as input (Identity map function). If the input
   * partition is not partitioned (a heap file), then the given shape is output
   * to all overlapping partitions.
   * @author Ahmed Eldawy
   *
   */
  public static class DataPartitionMap extends MapReduceBase 
  implements Mapper<Rectangle, ArrayWritable, Rectangle, ImageWritable> {

    /**Only objects inside this query range are drawn*/
    private Shape queryRange;
    private int imageWidth;
    private int imageHeight;
    private Rectangle drawMbr;
    private int strokeColor;
    private double scale2;
    /**Used to output values*/
    private ImageWritable sharedValue = new ImageWritable();

    /**Fade drawn shapes according to their area compared to a pixel area*/
    private boolean fade;
    private double scale;
    private boolean adaptiveSample;
    private float adaptiveSampleRatio;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      this.queryRange = OperationsParams.getShape(job, "rect");
      this.drawMbr = queryRange != null ? queryRange.getMBR() : ImageOutputFormat.getFileMBR(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.strokeColor = job.getInt("color", 0);
      this.fade = job.getBoolean("fade", false);

      this.scale2 = (double)imageWidth * imageHeight /
          (this.drawMbr.getWidth() * this.drawMbr.getHeight());
      this.scale = Math.sqrt(scale2);

      String valueRangeStr = job.get("valuerange");
      if (valueRangeStr != null) {
        String[] parts = valueRangeStr.contains("..") ? valueRangeStr.split("\\.\\.", 2) : valueRangeStr.split(",", 2);
        NASAPoint.minValue = Integer.parseInt(parts[0]);
        NASAPoint.maxValue = Integer.parseInt(parts[1]);
      }
      
      this.adaptiveSample = job.getBoolean("sample", false);
      if (this.adaptiveSample) {
        // Calculate the sample ratio
        this.adaptiveSampleRatio = job.getFloat(AdaptiveSampleRatio, 0.01f);
      }
    }

    @Override
    public void map(Rectangle cell, ArrayWritable value,
        OutputCollector<Rectangle, ImageWritable> output, Reporter reporter)
            throws IOException {
      BufferedImage image = new BufferedImage(imageWidth, imageHeight,
          BufferedImage.TYPE_INT_ARGB);

      Graphics2D graphics;
      try {
        graphics = image.createGraphics();
      } catch (Throwable e) {
        graphics = new SimpleGraphics(image);
      }
      graphics.setBackground(new Color(0, 0, 0, 0));
      graphics.clearRect(0, 0, imageWidth, imageHeight);
      Color storkeClr = new Color(strokeColor);
      graphics.setColor(storkeClr);

      for (Shape shape : (Shape[]) value.get()) {
        if (queryRange == null || queryRange.isIntersected(shape)) {
          if (fade) {
            Rectangle shapeMBR = shape.getMBR();
            // shapeArea represents how many pixels are covered by shapeMBR
            double shapeArea = (shapeMBR.getWidth() + shapeMBR.getHeight()) * this.scale;
            if (shapeArea > 1.0) {
              graphics.setColor(storkeClr);
            } else {
              byte alpha = (byte) Math.round(shapeArea * 255);
              if (alpha == 0) {
                continue;
              } else {
                graphics.setColor(new Color(((int)alpha << 24) | strokeColor, true));
              }
            }
          }
          if (adaptiveSample && shape instanceof Point) {
            if (Math.random() > adaptiveSampleRatio)
              return;
          }
          shape.draw(graphics, drawMbr, imageWidth, imageHeight, scale2);
        }
      }

      graphics.dispose();

      sharedValue.setImage(image);
      output.collect(drawMbr, sharedValue);
    }
  }

  /**
   * The reducer class combines all images into one image by overlaying all of
   * them on top of each other.
   * @author Ahmed Eldawy
   *
   */
  public static class DataPartitionReduce extends MapReduceBase
  implements Reducer<Rectangle, ImageWritable, Rectangle, ImageWritable> {

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
    }

    @Override
    public void reduce(Rectangle rect, Iterator<ImageWritable> values,
        OutputCollector<Rectangle, ImageWritable> output, Reporter reporter)
            throws IOException {
      if (values.hasNext()) {
        ImageWritable mergedImage = values.next();
        BufferedImage image = mergedImage.getImage();
        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        // Overlay all other images on top of it
        while (values.hasNext()) {
          BufferedImage img = values.next().getImage();
          graphics.drawImage(img, 0, 0, null);
        }
        graphics.dispose();

        mergedImage.setImage(image);

        output.collect(rect, mergedImage);
      }
    }
  }

  /**Last submitted Plot job*/
  public static RunningJob lastSubmittedJob;

  private static RunningJob plotMapReduce(Path inFile, Path outFile, OperationsParams params) throws IOException {
    boolean background = params.is("background");

    int width = params.getInt("width", 1000);
    int height = params.getInt("height", 1000);

    String hdfDataset = (String) params.get("dataset");
    Shape shape = hdfDataset != null ? new NASARectangle() : params.getShape("shape", null);
    Shape plotRange = params.getShape("rect", null);

    boolean keepAspectRatio = params.is("keep-ratio", true);

    String valueRangeStr = (String) params.get("valuerange");
    MinMax valueRange;
    if (valueRangeStr == null) {
      valueRange = null;
    } else {
      String[] parts = valueRangeStr.split(",");
      valueRange = new MinMax(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    JobConf job = new JobConf(params, GeometricPlot.class);
    job.setJobName("Plot");

    Rectangle fileMBR;
    // Collects some stats about the file to plot it correctly
    if (hdfDataset != null) {
      // Input is HDF
      job.set(HDFRecordReader.DatasetName, hdfDataset);
      if (job.get("shape") == null)
        job.setClass("shape", NASARectangle.class, Shape.class);
      // Determine the range of values by opening one of the HDF files
      if (valueRange == null)
        valueRange = Aggregate.aggregate(new Path[] {inFile}, params);
      job.set("valuerange", valueRange.minValue+".."+valueRange.maxValue);
      fileMBR = plotRange != null?
          plotRange.getMBR() : new Rectangle(-180, -140, 180, 169);
          //      job.setClass(HDFRecordReader.ProjectorClass, MercatorProjector.class,
          //          GeoProjector.class);
    } else {
      // Run MBR operation in synchronous mode
      OperationsParams mbrArgs = new OperationsParams(params);
      mbrArgs.setBoolean("background", false);
      fileMBR = plotRange != null ? plotRange.getMBR() :
        FileMBR.fileMBR(inFile, mbrArgs);
    }
    LOG.info("File MBR: "+fileMBR);

    if (shape instanceof Point && job.getBoolean("sample", false)) {
      // Need to set the sample ratio
      int imageWidth = job.getInt("width", 1000);
      int imageHeight = job.getInt("height", 1000);
      long recordCount = FileMBR.fileMBR(inFile, params).recordCount;
      float sampleRatio = params.getFloat(AdaptiveSampleFactor, 1.0f) *
          imageWidth * imageHeight / recordCount;
      job.setFloat(AdaptiveSampleRatio, sampleRatio);
    }
    
    if (keepAspectRatio) {
      // Adjust width and height to maintain aspect ratio
      if (fileMBR.getWidth() / fileMBR.getHeight() > (double) width / height) {
        // Fix width and change height
        height = (int) (fileMBR.getHeight() * width / fileMBR.getWidth());
        // Make divisible by two for compatibility with ffmpeg
        height &= 0xfffffffe;
        job.setInt("height", height);
      } else {
        width = (int) (fileMBR.getWidth() * height / fileMBR.getHeight());
        job.setInt("width", width);
      }
    }

    String partition = job.get("partition", "data").toLowerCase();
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);

    FileSystem inFs = inFile.getFileSystem(params);
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(inFs, inFile);

    if (partition.equals("space") || partition.equals("grid")) {
      if (gindex != null && gindex.isReplicated()) {
        LOG.info("Partitioned plot with an already partitioned file");
        job.setMapperClass(AlreadyPartitionedMap.class);
        job.setNumMapTasks(0); // No reducer for this job
        job.setInputFormat(ShapeArrayInputFormat.class);
        job.setMapOutputKeyClass(Rectangle.class);
        job.setMapOutputValueClass(ImageWritable.class);
      } else if (partition.equals("grid")) {
        LOG.info("Grid partition a file then plot");
        job.setMapperClass(GridPartitionMap.class);
        job.setReducerClass(GridPartitionReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(shape.getClass());

        GridInfo partitionGrid = new GridInfo(fileMBR.x1, fileMBR.y1, fileMBR.x2,
            fileMBR.y2);
        partitionGrid.calculateCellDimensions(
            (int) Math.max(1, clusterStatus.getMaxReduceTasks()));
        OperationsParams.setShape(job, PartitionGrid, partitionGrid);
        job.setInputFormat(ShapeInputFormat.class);
      } else if (partition.equals("space")) {
        // Use Skewed partitioning
        LOG.info("Use skewed partitioning then plot");
        job.setMapperClass(SkewedPartitionMap.class);
        job.setReducerClass(SkewedPartitionReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(shape.getClass());
        
        // Pack in rectangles using an RTree
        CellInfo[] cellInfos = Repartition.packInRectangles(inFile, outFile, params, fileMBR);
        SpatialSite.setCells(job, cellInfos);
        job.setInputFormat(ShapeInputFormat.class);
      }
    } else if (partition.equals("data")) {
      LOG.info("Plot using data partitioning");
      job.setMapperClass(DataPartitionMap.class);
      job.setCombinerClass(DataPartitionReduce.class);
      job.setReducerClass(DataPartitionReduce.class);
      job.setMapOutputKeyClass(Rectangle.class);
      job.setMapOutputValueClass(ImageWritable.class);
      job.setInputFormat(ShapeArrayInputFormat.class);
    } else {
      throw new RuntimeException("Unknown partition scheme '"+job.get("partition")+"'");
    }

    LOG.info("Creating an image of size "+width+"x"+height);
    ImageOutputFormat.setFileMBR(job, fileMBR);
    if (plotRange != null) {
      job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    }

    FileInputFormat.addInputPath(job, inFile);
    // Set output committer which will stitch images together after all reducers
    // finish
    job.setOutputCommitter(PlotOutputCommitter.class);

    job.setOutputFormat(ImageOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outFile);

    if (background) {
      JobClient jc = new JobClient(job);
      return lastSubmittedJob = jc.submitJob(job);
    } else {
      return lastSubmittedJob = JobClient.runJob(job);
    }
  }

  private static <S extends Shape> void plotLocal(Path inFile, Path outFile,
      OperationsParams params) throws IOException {
    int width = params.getInt("width", 1000);
    int height = params.getInt("height", 1000);

    Color strokeColor = params.getColor("color", Color.BLACK);
    int color = strokeColor.getRGB();

    String hdfDataset = (String) params.get("dataset");
    Shape shape = hdfDataset != null ? new NASARectangle() : (Shape) params.getShape("shape", null);
    Shape plotRange = params.getShape("rect", null);

    boolean keepAspectRatio = params.is("keep-ratio", true);

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

    Rectangle fileMbr;
    if (plotRange != null) {
      fileMbr = plotRange.getMBR();
    } else if (hdfDataset != null) {
      // Plotting a NASA file
      fileMbr = new Rectangle(-180, -90, 180, 90);
    } else {
      fileMbr = FileMBR.fileMBR(inFile, params);
    }

    if (keepAspectRatio) {
      // Adjust width and height to maintain aspect ratio
      if (fileMbr.getWidth() / fileMbr.getHeight() > (double) width / height) {
        // Fix width and change height
        height = (int) (fileMbr.getHeight() * width / fileMbr.getWidth());
      } else {
        width = (int) (fileMbr.getWidth() * height / fileMbr.getHeight());
      }
    }
    
    boolean adaptiveSample = shape instanceof Point && params.getBoolean("sample", false);
    float adaptiveSampleRatio = 0.0f;
    if (adaptiveSample) {
      // Calculate the sample ratio
      long recordCount = FileMBR.fileMBR(inFile, params).recordCount;
      adaptiveSampleRatio = params.getFloat(AdaptiveSampleFactor, 1.0f) *
          width * height / recordCount;
    }
    
    boolean gradualFade = !(shape instanceof Point) && params.getBoolean("fade", false);

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

    double scale2 = (double) width * height
        / (fileMbr.getWidth() * fileMbr.getHeight());
    double scale = Math.sqrt(scale2);

    // Create an image
    BufferedImage image = new BufferedImage(width, height,
        BufferedImage.TYPE_INT_ARGB);
    Graphics2D graphics = image.createGraphics();
    Color bg_color = params.getColor("bgcolor", new Color(0, 0, 0, 0));
    graphics.setBackground(bg_color);
    graphics.clearRect(0, 0, width, height);
    graphics.setColor(strokeColor);

    for (InputSplit split : splits) {
      if (hdfDataset != null) {
        // Read points from the HDF file
        RecordReader<NASADataset, NASAShape> reader = new HDFRecordReader(params,
            (FileSplit)split, hdfDataset, true);
        NASADataset dataset = reader.createKey();

        while (reader.next(dataset, (NASAShape)shape)) {
          // Skip with a fixed ratio if adaptive sample is set
          if (adaptiveSample && Math.random() > adaptiveSampleRatio)
            continue;
          if (plotRange == null || shape.isIntersected(shape)) {
            shape.draw(graphics, fileMbr, width, height, 0.0);
          }
        }
        reader.close();
      } else {
        RecordReader<Rectangle, Shape> reader = new ShapeRecordReader<Shape>(params,
            (FileSplit)split);
        Rectangle cell = reader.createKey();
        while (reader.next(cell, shape)) {
          // Skip with a fixed ratio if adaptive sample is set
          if (adaptiveSample && Math.random() > adaptiveSampleRatio)
            continue;
          Rectangle shapeMBR = shape.getMBR();
          if (shapeMBR != null) {
            if (plotRange == null || shapeMBR.isIntersected(plotRange)) {
              if (gradualFade) {
                double sizeInPixels = (shapeMBR.getWidth() + shapeMBR.getHeight()) * scale;
                if (sizeInPixels < 1.0 && Math.round(sizeInPixels * 255) < 1.0) {
                  // This shape can be safely skipped as it is too small to be plotted
                  continue;
                } else {
                  int alpha = (int)Math.round(sizeInPixels * 255);
                  graphics.setColor(new Color((alpha << 24) | color, true));
                }
              }
              shape.draw(graphics, fileMbr, width, height, scale2);
            }
          }
        }
        reader.close();
      }      
    }
    // Write image to output
    graphics.dispose();
    if (vflip) {
      AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
      tx.translate(0, -image.getHeight());
      AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
      image = op.filter(image, null);
    }
    FileSystem outFs = outFile.getFileSystem(params);
    OutputStream out = outFs.create(outFile, true);
    ImageIO.write(image, "png", out);
    out.close();

  }

  public static RunningJob plot(Path inFile, Path outFile, OperationsParams params) throws IOException {
    // Determine the size of input which needs to be processed in order to determine
    // whether to plot the file locally or using MapReduce
    boolean isLocal;
    if (params.get("local") == null) {
      JobConf job = new JobConf(params);
      ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();
      ShapeInputFormat.addInputPath(job, inFile);
      Shape plotRange = params.getShape("rect");
      if (plotRange != null) {
        job.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
      }
      InputSplit[] splits = inputFormat.getSplits(job, 1);
      boolean autoLocal = splits.length <= 3;

      isLocal = params.is("local", autoLocal);
    } else {
      isLocal = params.is("local");
    }

    if (isLocal) {
      plotLocal(inFile, outFile, params);
      return null;
    } else {
      return plotMapReduce(inFile, outFile, params);
    }
  }

  /**
   * Draws an image that can be used as a scale for heat maps generated using
   * Plot or PlotPyramid.
   * @param output - Output path
   * @param valueRange - Range of values of interest
   * @param width - Width of the generated image
   * @param height - Height of the generated image
   * @throws IOException
   */
  public static void drawScale(Path output, MinMax valueRange, int width, int height) throws IOException {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    Graphics2D g = image.createGraphics();
    g.setBackground(Color.BLACK);
    g.clearRect(0, 0, width, height);

    // fix this part to work according to color1, color2 and gradient type
    for (int y = 0; y < height; y++) {
      Color color = NASARectangle.calculateColor(y);
      g.setColor(color);
      g.drawRect(width * 3 / 4, y, width / 4, 1);
    }

    int fontSize = 24;
    g.setFont(new Font("Arial", Font.BOLD, fontSize));
    int step = (valueRange.maxValue - valueRange.minValue) * fontSize * 10 / height;
    step = (int) Math.pow(10, Math.round(Math.log10(step)));
    int min_value = valueRange.minValue / step * step;
    int max_value = valueRange.maxValue / step * step;

    for (int value = min_value; value <= max_value; value += step) {
      int y = fontSize + (height - fontSize) - value * (height - fontSize) / (valueRange.maxValue - valueRange.minValue);
      g.setColor(Color.WHITE);
      g.drawString(String.valueOf(value), 5, y);
    }

    g.dispose();

    FileSystem fs = output.getFileSystem(new Configuration());
    FSDataOutputStream outStream = fs.create(output, true);
    ImageIO.write(image, "png", outStream);
    outStream.close();
  }

  /**
   * Combines images of different datasets into one image that is displayed
   * to users.
   * This method is called from the web interface to display one image for
   * multiple selected datasets.
   * @param fs The file system that contains the datasets and images
   * @param files Paths to directories which contains the datasets
   * @param includeBoundaries Also plot the indexing boundaries of datasets
   * @return An image that is the combination of all datasets images
   * @throws IOException
   */
  public static BufferedImage combineImages(Configuration conf, Path[] files,
      boolean includeBoundaries, int width, int height) throws IOException {
    BufferedImage result = null;
    // Retrieve the MBRs of all datasets
    Rectangle allMbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Path file : files) {
      Rectangle mbr = FileMBR.fileMBR(file, new OperationsParams(conf));
      allMbr.expand(mbr);
    }

    // Adjust width and height to maintain aspect ratio
    if ((allMbr.x2 - allMbr.x1) / (allMbr.y2 - allMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((allMbr.y2 - allMbr.y1) * width / (allMbr.x2 - allMbr.x1));
    } else {
      width = (int) ((allMbr.x2 - allMbr.x1) * height / (allMbr.y2 - allMbr.y1));
    }
    result = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);


    for (Path file : files) {
      FileSystem fs = file.getFileSystem(conf);
      if (fs.getFileStatus(file).isDir()) {
        // Retrieve the MBR of this dataset
        Rectangle mbr = FileMBR.fileMBR(file, new OperationsParams(conf));
        // Compute the coordinates of this image in the whole picture
        mbr.x1 = (mbr.x1 - allMbr.x1) * width / allMbr.getWidth();
        mbr.x2 = (mbr.x2 - allMbr.x1) * width / allMbr.getWidth();
        mbr.y1 = (mbr.y1 - allMbr.y1) * height / allMbr.getHeight();
        mbr.y2 = (mbr.y2 - allMbr.y1) * height / allMbr.getHeight();
        // Retrieve the image of this dataset
        Path imagePath = new Path(file, "_data.png");
        if (!fs.exists(imagePath))
          throw new RuntimeException("Image "+imagePath+" not ready");
        FSDataInputStream imageFile = fs.open(imagePath);
        BufferedImage image = ImageIO.read(imageFile);
        imageFile.close();
        // Draw the image
        Graphics graphics = result.getGraphics();
        graphics.drawImage(image, (int) mbr.x1, (int) mbr.y1,
            (int) mbr.getWidth(), (int) mbr.getHeight(), null);
        graphics.dispose();

        if (includeBoundaries) {
          // Plot also the image of the boundaries
          // Retrieve the image of the dataset boundaries
          imagePath = new Path(file, "_partitions.png");
          if (fs.exists(imagePath)) {
            imageFile = fs.open(imagePath);
            image = ImageIO.read(imageFile);
            imageFile.close();
            // Draw the image
            graphics = result.getGraphics();
            graphics.drawImage(image, (int) mbr.x1, (int) mbr.y1,
                (int) mbr.getWidth(), (int) mbr.getHeight(), null);
            graphics.dispose();
          }
        }
      }
    }

    return result;
  }


  private static void printUsage() {
    System.out.println("Plots all shapes to an image");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("width:<w> - Maximum width of the image (1000)");
    System.out.println("height:<h> - Maximum height of the image (1000)");
    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("partition:<data|space> - whether to use data partitioning (default) or space partitioning");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
    System.out.println("-fade: Use the gradual fade option");
    System.out.println("-sample: Use the daptive sample option");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path inFile = params.getInputPath();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    plot(inFile, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Plot finished in "+(t2-t1)+" millis");
  }

}
