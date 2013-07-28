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
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.ImageWritable;
import edu.umn.cs.spatialHadoop.SimpleGraphics;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.OGCShape;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

public class Plot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(Plot.class);
  
  /**Whether or not to show partition borders (boundaries) in generated image*/
  private static final String ShowBorders = "plot.show_borders";
  private static final String ShowBlockCount = "plot.show_block_count";
  private static final String ShowRecordCount = "plot.show_record_count";
  private static final String CompactCells = "plot.compact_cells";

  /**
   * If the processed block is already partitioned (via global index), then
   * the output is the same as input (Identity map function). If the input
   * partition is not partitioned (a heap file), then the given shape is output
   * to all overlapping partitions.
   * @author Ahmed Eldawy
   *
   */
  public static class PlotMap extends MapReduceBase 
    implements Mapper<Rectangle, Shape, Rectangle, Shape> {
    
    private CellInfo[] cellInfos;
    
    @Override
    public void configure(JobConf job) {
      try {
        super.configure(job);
        cellInfos = SpatialSite.getCells(job);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    public void map(Rectangle cell, Shape shape,
        OutputCollector<Rectangle, Shape> output, Reporter reporter)
        throws IOException {
      if (cell.isValid()) {
        // Output shape to all overlapping cells
        for (CellInfo cellInfo : cellInfos)
          if (cellInfo.isIntersected(shape))
            output.collect(cellInfo, shape);
      } else {
        // Input is already partitioned
        // Output shape to containing cell only
        output.collect(cell, shape);
      }
    }
  }
  
  /**
   * The reducer class draws an image for contents (shapes) in each cell info
   * @author Ahmed Eldawy
   *
   */
  public static class PlotReduce extends MapReduceBase
      implements Reducer<Rectangle, Shape, Rectangle, ImageWritable> {
    
    private Rectangle fileMbr;
    private int imageWidth, imageHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2;
    private boolean compactCells;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      fileMbr = ImageOutputFormat.getFileMBR(job);
      imageWidth = ImageOutputFormat.getImageWidth(job);
      imageHeight = ImageOutputFormat.getImageHeight(job);
      compactCells = job.getBoolean(CompactCells, false);
      
      this.scale2 = (double)imageWidth * imageHeight /
          ((double)(fileMbr.x2 - fileMbr.x1) * (fileMbr.y2 - fileMbr.y1));
    }

    @Override
    public void reduce(Rectangle cellInfo, Iterator<Shape> values,
        OutputCollector<Rectangle, ImageWritable> output, Reporter reporter)
        throws IOException {
      try {
        // Initialize the image
        int image_x1 = (int) ((cellInfo.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
        int image_y1 = (int) ((cellInfo.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
        int image_x2 = (int) (((cellInfo.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
        int image_y2 = (int) (((cellInfo.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
        int tile_width = image_x2 - image_x1;
        int tile_height = image_y2 - image_y1;
        if (tile_width == 0 || tile_height == 0)
          return;
        BufferedImage image = new BufferedImage(tile_width, tile_height,
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
        graphics.clearRect(0, 0, tile_width, tile_height);
        graphics.setColor(stroke_color);
        graphics.translate(-image_x1, -image_y1);

        double data_x1 = cellInfo.x1, data_y1 = cellInfo.y1,
            data_x2 = cellInfo.x2, data_y2 = cellInfo.y2;
        if (values.hasNext()) {
          Shape s = values.next();
          drawShape(graphics, s, fileMbr, imageWidth, imageHeight, scale2);
          if (compactCells) {
            Rectangle mbr = s.getMBR();
            data_x1 = mbr.x1;
            data_y1 = mbr.y1;
            data_x2 = mbr.x2;
            data_y2 = mbr.y2;
          }
          
          while (values.hasNext()) {
            s = values.next();
            drawShape(graphics, s, fileMbr, imageWidth, imageHeight, scale2);

            if (compactCells) {
              Rectangle mbr = s.getMBR();
              if (mbr.x1 < data_x1)
                data_x1 = mbr.x1;
              if (mbr.y1 < data_y1)
                data_y1 = mbr.y1;
              if (mbr.x2 > data_x2)
                data_x2 = mbr.x2;
              if (mbr.y2 > data_y2)
                data_y2 = mbr.y2;
            }
          }
        }
        
        if (compactCells) {
          // Ensure that the drawn rectangle fits in the partition
          if (data_x1 < cellInfo.x1)
            data_x1 = cellInfo.x1;
          if (data_y1 < cellInfo.y1)
            data_y1 = cellInfo.y1;
          if (data_x2 > cellInfo.x2)
            data_x2 = cellInfo.x2;
          if (data_y2 > cellInfo.y2)
            data_y2 = cellInfo.y2;
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
  
  public static void drawShape(Graphics2D graphics, Shape s, Rectangle fileMbr,
      int imageWidth, int imageHeight, double scale) {
    if (s instanceof Point) {
      Point pt = (Point) s;
      int x = (int) ((pt.x - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int y = (int) ((pt.y - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      
      if (x >= 0 && x < imageWidth && y >= 0 && y < imageHeight)
        graphics.fillRect(x, y, 1, 1);
    } else if (s instanceof Rectangle) {
      Rectangle r = (Rectangle) s;
      int s_x1 = (int) ((r.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y1 = (int) ((r.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      int s_x2 = (int) (((r.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y2 = (int) (((r.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      graphics.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
    } else if (s instanceof OGCShape) {
      OGCShape ogc_shape = (OGCShape) s;
      OGCGeometry geom = ogc_shape.geom;
      Color shape_color = graphics.getColor();
      if (geom instanceof OGCGeometryCollection) {
        OGCGeometryCollection geom_coll = (OGCGeometryCollection) geom;
        for (int i = 0; i < geom_coll.numGeometries(); i++) {
          OGCGeometry sub_geom = geom_coll.geometryN(i);
          // Recursive call to draw each geometry
          drawShape(graphics, new OGCShape(sub_geom), fileMbr, imageWidth, imageHeight, scale);
        }
      } else if (geom.getEsriGeometry() instanceof MultiPath) {
        MultiPath path = (MultiPath) geom.getEsriGeometry();
        double sub_geom_alpha = path.calculateLength2D() * scale;
        int color_alpha = sub_geom_alpha > 1.0 ? 255 : (int) Math.round(sub_geom_alpha * 255);

        if (color_alpha == 0)
          return;

        int[] xpoints = new int[path.getPointCount()];
        int[] ypoints = new int[path.getPointCount()];
        
        for (int i = 0; i < path.getPointCount(); i++) {
          double px = path.getPoint(i).getX();
          double py = path.getPoint(i).getY();
          
          // Transform a point in the polygon to image coordinates
          xpoints[i] = (int) Math.round((px - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
          ypoints[i] = (int) Math.round((py - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
        }
        
        // Draw the polygon
        graphics.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
        if (path instanceof Polygon)
          graphics.drawPolygon(xpoints, ypoints, path.getPointCount());
        else if (path instanceof Polyline)
          graphics.drawPolyline(xpoints, ypoints, path.getPointCount());
      }
    } else {
      LOG.warn("Cannot draw a shape of type: "+s.getClass());
      Rectangle r = s.getMBR();
      int s_x1 = (int) ((r.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y1 = (int) ((r.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      int s_x2 = (int) (((r.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y2 = (int) (((r.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      if (s_x1 >= 0 && s_x1 < imageWidth && s_y1 >= 0 && s_y1 < imageHeight)
        graphics.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
    }
  }
  
  public static <S extends Shape> void plotMapReduce(Path inFile, Path outFile,
      Shape shape, int width, int height, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount, boolean compactCells)  throws IOException {
    JobConf job = new JobConf(Plot.class);
    job.setJobName("Plot");
    
    job.setMapperClass(PlotMap.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setReducerClass(PlotReduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
    job.setMapOutputKeyClass(CellInfo.class);
    SpatialSite.setShapeClass(job, shape.getClass());
    job.setMapOutputValueClass(shape.getClass());
    
    FileSystem inFs = inFile.getFileSystem(job);
    Rectangle fileMbr = FileMBR.fileMBRMapReduce(inFs, inFile, shape);
    FileStatus inFileStatus = inFs.getFileStatus(inFile);
    
    CellInfo[] cellInfos;
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(inFs, inFile);
    if (gindex == null) {
      // A heap file. The map function should partition the file
      GridInfo gridInfo = new GridInfo(fileMbr.x1, fileMbr.y1, fileMbr.x2,
          fileMbr.y2);
      gridInfo.calculateCellDimensions(inFileStatus.getLen(),
          inFileStatus.getBlockSize());
      cellInfos = gridInfo.getAllCells();
      // Doesn't make sense to show any partition information in a heap file
      showBorders = showBlockCount = showRecordCount = false;
    } else {
      cellInfos = SpatialSite.cellsOf(inFs, inFile);
    }
    
    // Set cell information in the job configuration to be used by the mapper
    SpatialSite.setCells(job, cellInfos);
    
    // Adjust width and height to maintain aspect ratio
    if ((fileMbr.x2 - fileMbr.x1) / (fileMbr.y2 - fileMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((fileMbr.y2 - fileMbr.y1) * width / (fileMbr.x2 - fileMbr.x1));
    } else {
      width = (int) ((fileMbr.x2 - fileMbr.x1) * height / (fileMbr.y2 - fileMbr.y1));
    }
    ImageOutputFormat.setFileMBR(job, fileMbr);
    ImageOutputFormat.setImageWidth(job, width);
    ImageOutputFormat.setImageHeight(job, height);
    job.setBoolean(ShowBorders, showBorders);
    job.setBoolean(ShowBlockCount, showBlockCount);
    job.setBoolean(ShowRecordCount, showRecordCount);
    job.setBoolean(CompactCells, compactCells);
    
    // Set input and output
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    
    job.setOutputFormat(ImageOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outFile);
    
    JobClient.runJob(job);
    
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
    
    if (resultFiles.length == 1) {
      // Only one output file
      outFs.rename(resultFiles[0].getPath(), outFile);
    } else {
      // Merge all images into one image (overlay)
      BufferedImage finalImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
      for (FileStatus resultFile : resultFiles) {
        FSDataInputStream imageFile = outFs.open(resultFile.getPath());
        BufferedImage tileImage = ImageIO.read(imageFile);
        imageFile.close();

        Graphics2D graphics;
        try {
          graphics = finalImage.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(finalImage);
        }
        graphics.drawImage(tileImage, 0, 0, null);
        graphics.dispose();
      }
      
      // Finally, write the resulting image to the given output path
      OutputStream outputImage = outFs.create(outFile);
      ImageIO.write(finalImage, "png", outputImage);
    }
    
    outFs.delete(temp, true);
  }
  
  public static <S extends Shape> void plotLocal(Path inFile, Path outFile,
      S shape, int width, int height, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount, boolean compactCells)
          throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    Rectangle fileMbr = FileMBR.fileMBRLocal(inFs, inFile, shape);
    
    // Adjust width and height to maintain aspect ratio
    if ((fileMbr.x2 - fileMbr.x1) / (fileMbr.y2 - fileMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((fileMbr.y2 - fileMbr.y1) * width / (fileMbr.x2 - fileMbr.x1));
    } else {
      width = (int) ((fileMbr.x2 - fileMbr.x1) * height / (fileMbr.y2 - fileMbr.y1));
    }
    
    double scale2 = (double) width * height
        / ((double) (fileMbr.x2 - fileMbr.x1) * (fileMbr.y2 - fileMbr.y1));

    // Create an image
    BufferedImage image = new BufferedImage(width, height,
        BufferedImage.TYPE_INT_ARGB);
    Graphics2D graphics = image.createGraphics();
    Color bg_color = new Color(0,0,0,0);
    bg_color = Color.WHITE;
    graphics.setBackground(bg_color);
    graphics.clearRect(0, 0, width, height);
    Color stroke_color = Color.BLACK;
    graphics.setColor(stroke_color);

    long fileLength = inFs.getFileStatus(inFile).getLen();
    ShapeRecordReader<S> reader =
      new ShapeRecordReader<S>(inFs.open(inFile), 0, fileLength);
    
    Rectangle cell = reader.createKey();
    while (reader.next(cell, shape)) {
      drawShape(graphics, shape, fileMbr, width, height, scale2);
    }
    
    reader.close();
    graphics.dispose();
    FileSystem outFs = outFile.getFileSystem(new Configuration());
    OutputStream out = outFs.create(outFile, true);
    ImageIO.write(image, "png", out);
    out.close();
  }
  
  public static <S extends Shape> void plot(Path inFile, Path outFile,
      S shape, int width, int height, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount, boolean compactCells)
          throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFStatus.isDir() || inFs.getFileBlockLocations(inFStatus, 0, inFStatus.getLen()).length > 3) {
      plotMapReduce(inFile, outFile, shape, width, height, showBorders, showBlockCount, showRecordCount, compactCells);
    } else {
      plotLocal(inFile, outFile, shape, width, height, showBorders, showBlockCount, showRecordCount, compactCells);
    }
  }

  
  private static void printUsage() {
    System.out.println("Plots all shapes to an image");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("width:<w> - Maximum width of the image (1000,1000)");
    System.out.println("height:<h> - Maximum height of the image (1000,1000)");
//    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("-borders: For globally indexed files, draws the borders of partitions");
    System.out.println("-showblockcount: For globally indexed files, draws the borders of partitions");
    System.out.println("-showrecordcount: Show number of records in each partition");
    System.out.println("-compact: Shrink partition boundaries to fit contents");
    System.out.println("-overwrite: Override output file without notice");
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Plot.class);
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

    boolean showBorders = cla.is("borders");
    boolean showBlockCount = cla.is("showblockcount");
    boolean showRecordCount = cla.is("showrecordcount");
    boolean compactCells = cla.is("compact");
    Shape shape = cla.getShape(true);
    
    int width = cla.getWidth(1000);
    int height = cla.getHeight(1000);

    plot(inFile, outFile, shape, width, height, showBorders, showBlockCount, showRecordCount, compactCells);
  }

}
