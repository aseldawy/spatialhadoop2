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
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Vector;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot.HDFRasterizer;

/**
 * Plots all datasets from NASA satisfying the following criteria:
 * 1- Spatial criteria: Falls in the spatial range provided in 'rect' parameter
 * 2- Time critieria: Falls the in the time range provided in 'time' parameter
 * 3- Dataset: Selects the exact dataset given in 'dataset' parameter
 * If overwrite flag is set, all datasets are plotted and overwritten if exists.
 * If the flag is not set, only time instances that are not generated are
 * generated.
 * @author Ahmed Eldawy
 *
 */
public class MultiHDFPlot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(MultiHDFPlot.class);

  private static void printUsage() {
    System.out.println("Plots all NASA datasets matching user criteria");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to NASA repository of all datasets");
    System.out.println("<output file> - (*) Path to output images");
    System.out.println("-pyramid - Draw a multilevel image");
    System.out.println("width:<w> - Width of the whole image (for single level plot)");
    System.out.println("height:<w> - Height of the whole image (for single level plot)");
    System.out.println("tilewidth:<w> - Width of each tile in pixels (for pyramid plot)");
    System.out.println("tileheight:<h> - Height of each tile in pixels (for pyramid plot)");
    System.out.println("numlevels:<n> - Number of levels in the pyrmaid (7)");
    System.out.println("color1:<c1> - The color associated with v1");
    System.out.println("color2:<c2> - The color associated with v2");
    System.out.println("gradient:<rgb|hsb> - Type of gradient to use");
    System.out.println("recover:<read|write|none> - How to recover holes in the data (none)");
    System.out.println("dataset:<d> - Dataset to plot from HDF files");
    System.out.println("time:<from..to> - Time range each formatted as yyyy.mm.dd");
    System.out.println("rect:<x1,y1,x2,y2> - Limit drawing to the selected area");
    System.out.println("-adddate - Write the date on each generated image (false)");
    System.out.println("dateformat:<df> - The format of the date to write on each image (dd-MM-yyyy)");
    System.out.println("combine:<c> - Number of frames to combine in each image (1)");
    System.out.println("-overwrite: Overwrite output file without notice");
  }

  public static boolean multiplot(Path[] input, Path output,
      OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException, ParseException {
    String timeRange = params.get("time");
    final Date dateFrom, dateTo;
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
    try {
      String[] parts = timeRange.split("\\.\\.");
      dateFrom = dateFormat.parse(parts[0]);
      dateTo = dateFormat.parse(parts[1]);
    } catch (ArrayIndexOutOfBoundsException e) {
      System.err.println("Use the seperator two periods '..' to seperate from and to dates");
      return false; // To avoid an error that causes dateFrom to be uninitialized
    } catch (ParseException e) {
      System.err.println("Illegal date format in "+timeRange);
      return false;
    }
    // Number of frames to combine in each image
    int combine = params.getInt("combine", 1);
    // Retrieve all matching input directories based on date range
    Vector<Path> matchingPathsV = new Vector<Path>();
    for (Path inputFile : input) {
      FileSystem inFs = inputFile.getFileSystem(params);
      FileStatus[] matchingDirs = inFs.listStatus(input, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          String dirName = p.getName();
          try {
            Date date = dateFormat.parse(dirName);
            return date.compareTo(dateFrom) >= 0 && date.compareTo(dateTo) <= 0;
          } catch (ParseException e) {
            LOG.warn("Cannot parse directory name: " + dirName);
            return false;
          }
        }
      });
      for (FileStatus matchingDir : matchingDirs)
        matchingPathsV.add(new Path(matchingDir.getPath(), "*.hdf"));
    }
    if (matchingPathsV.isEmpty()) {
      LOG.warn("No matching directories to given input");
      return false;
    }
    
    Path[] matchingPaths = matchingPathsV.toArray(new Path[matchingPathsV.size()]);
    Arrays.sort(matchingPaths);
    
    // Clear all paths to ensure we set our own paths for each job
    params.clearAllPaths();
  
    // Create a water mask if we need to recover holes on write
    if (params.get("recover", "none").equals("write")) {
      // Recover images on write requires a water mask image to be generated first
      OperationsParams wmParams = new OperationsParams(params);
      wmParams.setBoolean("background", false);
      Path wmImage = new Path(output, new Path("water_mask"));
      HDFPlot.generateWaterMask(wmImage, wmParams);
      params.set(HDFPlot.PREPROCESSED_WATERMARK, wmImage.toString());
    }
    // Start a job for each path
    int imageWidth = -1;
    int imageHeight = -1;
    boolean overwrite = params.getBoolean("overwrite", false);
    boolean pyramid = params.getBoolean("pyramid", false);
    FileSystem outFs = output.getFileSystem(params);
    Vector<Job> jobs = new Vector<Job>();
    boolean background = params.getBoolean("background", false);
    Rectangle mbr = new Rectangle(-180, -90, 180, 90);
    for (int i = 0; i < matchingPaths.length; i += combine) {
      Path[] inputPaths = new Path[Math.min(combine, matchingPaths.length - i)];
      System.arraycopy(matchingPaths, i, inputPaths, 0, inputPaths.length);
      Path outputPath = new Path(output,
          inputPaths[0].getParent().getName()+ (pyramid? "" : ".png"));
      if (overwrite || !outFs.exists(outputPath)) {
        // Need to plot
        Job rj = HDFPlot.plotHeatMap(inputPaths, outputPath, params);
        if (imageHeight == -1 || imageWidth == -1) {
          if (rj != null) {
            imageHeight = rj.getConfiguration().getInt("height", 1000);
            imageWidth = rj.getConfiguration().getInt("width", 1000);
            mbr = (Rectangle) OperationsParams.getShape(rj.getConfiguration(), "mbr");
          } else {
            imageHeight = params.getInt("height", 1000);
            imageWidth = params.getInt("width", 1000);
            mbr = (Rectangle) OperationsParams.getShape(params, "mbr");
          }
        }
        if (background && rj != null)
          jobs.add(rj);
      }
    }
    // Wait until all jobs are done
    while (!jobs.isEmpty()) {
      Job firstJob = jobs.firstElement();
      firstJob.waitForCompletion(false);
      if (!firstJob.isSuccessful()) {
        System.err.println("Error running job "+firstJob.getJobID());
        System.err.println("Killing all remaining jobs");
        for (int j = 1; j < jobs.size(); j++)
          jobs.get(j).killJob();
        throw new RuntimeException("Error running job "+firstJob.getJobID());
      }
      jobs.remove(0);
    }
    
    // Draw the scale in the output path if needed
    String scalerange = params.get("scalerange");
    if (scalerange != null) {
      String[] parts = scalerange.split("\\.\\.");
      double min = Double.parseDouble(parts[0]);
      double max = Double.parseDouble(parts[1]);
      String scale = params.get("scale", "none").toLowerCase();
      if (scale.equals("vertical")) {
        MultiHDFPlot.drawVerticalScale(new Path(output, "scale.png"), min, max, 64,
            imageHeight, params);
      } else if (scale.equals("horizontal")) {
        MultiHDFPlot.drawHorizontalScale(new Path(output, "scale.png"), min, max, imageWidth,
            64, params);
      }
    }
    // Add the KML file
    createKML(outFs, output, mbr, params);
    return true;
  }

  private static void createKML(FileSystem outFs, Path output, Rectangle mbr,
      OperationsParams params) throws IOException, ParseException {
    FileStatus[] all_images = outFs.listStatus(output, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().matches("\\d+\\.\\d+\\.\\d+\\.png");
      }
    });
    
    Path kmlPath = new Path(output, "index.kml");
    PrintStream ps = new PrintStream(outFs.create(kmlPath));
    ps.println("<?xml version='1.0' encoding='UTF-8'?>");
    ps.println("<kml xmlns='http://www.opengis.net/kml/2.2'>");
    ps.println("<Folder>");
    String mbrStr = String.format("<LatLonBox><west>%f</west><south>%f</south><east>%f</east><north>%f</north></LatLonBox>", mbr.x1, mbr.y1, mbr.x2, mbr.y2);
    for (FileStatus image : all_images) {
      SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyy.MM.dd");
      SimpleDateFormat kmlDateFormat = new SimpleDateFormat("yyyy-MM-dd");
      String name = image.getPath().getName();
      int dotIndex = name.lastIndexOf('.');
      name = name.substring(0, dotIndex);
      Date date = fileDateFormat.parse(name);
      String kmlDate = kmlDateFormat.format(date);
      ps.println("<GroundOverlay>");
      ps.println("<name>"+kmlDate+"</name>");
      ps.println("<TimeStamp><when>"+kmlDate+"</when></TimeStamp>");
      ps.println("<Icon><href>"+image.getPath().getName()+"</href></Icon>");
      ps.println(mbrStr);
      ps.println("</GroundOverlay>");
    }
    String scale = params.get("scale", "none").toLowerCase();
    if (scale.equals("vertical")) {
      ps.println("<ScreenOverlay>");
      ps.println("<name>Scale</name>");
      ps.println("<Icon><href>scale.png</href></Icon>");
      ps.println("<overlayXY x='1' y='0.5' xunits='fraction' yunits='fraction'/>");
      ps.println("<screenXY x='1' y='0.5' xunits='fraction' yunits='fraction'/>");
      ps.println("<rotationXY x='0' y='0' xunits='fraction' yunits='fraction'/>");
      ps.println("<size x='0' y='0.7' xunits='fraction' yunits='fraction'/>");
      ps.println("</ScreenOverlay>");
    } else if (scale.equals("horizontal")) {
      ps.println("<ScreenOverlay>");
      ps.println("<name>Scale</name>");
      ps.println("<Icon><href>scale.png</href></Icon>");
      ps.println("<overlayXY x='0.5' y='0' xunits='fraction' yunits='fraction'/>");
      ps.println("<screenXY x='0.5' y='0' xunits='fraction' yunits='fraction'/>");
      ps.println("<rotationXY x='0' y='0' xunits='fraction' yunits='fraction'/>");
      ps.println("<size x='0.7' y='0' xunits='fraction' yunits='fraction'/>");
      ps.println("</ScreenOverlay>");
    }
    ps.println("</Folder>");
    ps.println("</kml>");
    ps.close();
  }

  private static void createVideo(FileSystem outFs, Path output, boolean addLogo)
      throws IOException {
    // Rename all generated files to be day_%3d.png
    // Rename files to be ready to use with ffmpeg
    FileStatus[] all_images = outFs.listStatus(output, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().matches("\\d+\\.\\d+\\.\\d+\\.png");
      }
    });
    
    Arrays.sort(all_images, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus f1, FileStatus f2) {
        return f1.getPath().getName().compareTo(f2.getPath().getName());
      }
    });
    
    int day = 1;
    for (FileStatus image : all_images) {
      String newFileName = String.format("day_%03d.png", day++);
      outFs.rename(image.getPath(), new Path(output, newFileName));
    }
    
    String videoCommand;
    if (addLogo) {
      // Puts frames together into a video
      videoCommand = "avconv -r 4 -i day_%3d.png -vf "
          + "\"movie=gistic_logo.png [watermark]; "
          + "movie=scale.png [scale]; "
          + "[in][watermark] overlay=main_w-overlay_w-10:10 [mid]; "
          + "[mid] pad=iw+64:ih [mid2]; "
          + "[mid2][scale] overlay=main_w-overlay_w:0 [out]\" "
          + "-r 4 -pix_fmt yuv420p output.mp4 ";
    } else {
      videoCommand = "avconv -r 4 -i day_%3d.png -vf \""
          + "movie=scale.png [scale]; "
          + "[in] pad=iw+64:ih [mid2]; "
          + "[mid2][scale] overlay=main_w-overlay_w:0 [out]\" "
          + "-r 4 -pix_fmt yuv420p output.mp4 ";
    }
    System.out.println("Run the following command to generate the video");
    System.out.println(videoCommand);
  }

  /**
   * Draws a scale used with the heat map
   * @param output
   * @param valueRange
   * @param width
   * @param height
   * @throws IOException
   */
  private static void drawVerticalScale(Path output, double min, double max, int width, int height, OperationsParams params) throws IOException {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    Graphics2D g = image.createGraphics();
    g.setBackground(Color.BLACK);
    g.clearRect(0, 0, width, height);
  
    // fix this part to work according to color1, color2 and gradient type
    HDFPlot.HDFRasterizer gradient = new HDFPlot.HDFRasterizer();
    gradient.configure(params);
    HDFRasterLayer gradientLayer = (HDFRasterLayer) gradient.createCanvas(0, 0, new Rectangle());
    for (int y = 0; y < height; y++) {
      Color color = gradientLayer.calculateColor(height - y, 0, height);
      g.setColor(color);
      g.drawRect(width * 3 / 4, y, width / 4, 1);
    }
  
    int fontSize = 24;
    g.setFont(new Font("Arial", Font.BOLD, fontSize));
    double step = (max - min) * fontSize * 5 / height;
    step = (int)(Math.pow(10.0, Math.round(Math.log10(step))));
    double min_value = Math.floor(min / step) * step;
    double max_value = Math.floor(max / step) * step;
  
    g.setColor(Color.WHITE);
    for (double value = min_value; value <= max_value; value += step) {
      double y = ((value - min) + (max - value) * (height - fontSize))/(max - min);
      g.drawString(String.valueOf((int)value), 5, (int)y);
    }
  
    g.dispose();
  
    FileSystem fs = output.getFileSystem(new Configuration());
    FSDataOutputStream outStream = fs.create(output, true);
    ImageIO.write(image, "png", outStream);
    outStream.close();
  }

  /**
   * Draws a scale used with the heat map
   * @param output
   * @param valueRange
   * @param width
   * @param height
   * @throws IOException
   */
  private static void drawHorizontalScale(Path output, double min, double max,
      int width, int height, OperationsParams params) throws IOException {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    Graphics2D g = image.createGraphics();
    g.setBackground(Color.BLACK);
    g.clearRect(0, 0, width, height);
  
    int fontSize = 24;
    // fix this part to work according to color1, color2 and gradient type
    HDFPlot.HDFRasterizer gradient = new HDFPlot.HDFRasterizer();
    gradient.configure(params);
    HDFRasterLayer gradientLayer = (HDFRasterLayer) gradient.createCanvas(0, 0, new Rectangle());
    for (int x = 0; x < width; x++) {
      Color color = gradientLayer.calculateColor(x, 0, width);
      g.setColor(color);
      g.drawRect(x, height - (fontSize - 5), 1, fontSize - 5);
    }
  
    g.setFont(new Font("Arial", Font.BOLD, fontSize));
    double step = (max - min) * fontSize * 5 / width;
    step = (int)(Math.pow(10.0, Math.round(Math.log10(step))));
    double min_value = Math.floor(min / step) * step;
    double max_value = Math.floor(max / step) * step;
  
    g.setColor(Color.WHITE);
    for (double value = min_value; value <= max_value; value += step) {
      double x = ((value - min) * (width - fontSize) + (max - value))/(max - min);
      g.drawString(String.valueOf((int)value), (int)x, fontSize);
    }
  
    g.dispose();
  
    FileSystem fs = output.getFileSystem(new Configuration());
    FSDataOutputStream outStream = fs.create(output, true);
    ImageIO.write(image, "png", outStream);
    outStream.close();
  }

  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   * @throws ParseException 
   */
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    if (!params.checkInputOutput()) {
      System.err.println("Output directly already exists and overwrite flag is not set");
      printUsage();
      System.exit(1);
    }
    String timeRange = params.get("time");
    if (timeRange == null) {
      System.err.println("time range must be specified");
      printUsage();
      System.exit(1);
    }
    Path[] input = params.getInputPaths();
    Path output = params.getOutputPath();
    long t1 = System.currentTimeMillis();
    multiplot(input, output, params);
    long t2 = System.currentTimeMillis();
    System.out.println("MultiHDFPlot finished in "+(t2-t1)+" millis");
  }
}
