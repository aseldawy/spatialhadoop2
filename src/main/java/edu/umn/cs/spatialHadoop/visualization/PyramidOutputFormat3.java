/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;

import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * An output format that is used to write either image tiles or data tiles.
 * @author Ahmed Eldawy
 *
 */
public class PyramidOutputFormat3 extends FileOutputFormat<LongWritable, Writable> {

  public static final String DataExt = ".txt";

  static class ImageRecordWriter extends RecordWriter<LongWritable, Writable> {

    private Plotter plotter;
    private final FileSystem outFS;
    private final Path outPath;
    private boolean vflip;
    /**Used to indicate progress to Hadoop*/
    private TaskAttemptContext task;
    /**Extension of output images*/
    private String imgExt;

    private Map<Long, FSDataOutputStream> dataFiles;

    /**A temporary Text to store a shape before writing it to the output*/
    private Text tempLine;

    /**New line characters as a byte array to append to Text*/
    private static final byte[] NewLineChars = "\n".getBytes();

    /**A temporary tile index to decocde the tile ID*/
    private TileIndex tempTileIndex;
    
    ImageRecordWriter(FileSystem outFs, Path taskOutPath, TaskAttemptContext task) {
      this.task = task;
      System.setProperty("java.awt.headless", "true");
      this.plotter = Plotter.getPlotter(task.getConfiguration());
      this.outPath = taskOutPath;
      this.outFS = outFs;
      this.vflip = task.getConfiguration().getBoolean("vflip", true);
      String outFName = outPath.getName();
      int extensionStart = outFName.lastIndexOf('.');
      imgExt = extensionStart == -1 ? ".png"
          : outFName.substring(extensionStart);
      tempLine = new Text2();
      dataFiles = new HashMap<Long, FSDataOutputStream>();
    }

    private final Path getTilePath(int z, int x, int y, String ext) {
      if (vflip)
        y = ((1 << z) - 1) - y;
      return new Path(outPath, "tile-"+z +"-"+x+"-"+y+ext);
    }

    @Override
    public void write(LongWritable encodedTileID, Writable w) throws IOException {
      tempTileIndex = TileIndex.decode(encodedTileID.get(), tempTileIndex);
      if (w instanceof Canvas) {
        Path imagePath = getTilePath(tempTileIndex.z, tempTileIndex.x, tempTileIndex.y, imgExt);
    	  // Write this tile to an image
    	  FSDataOutputStream outFile = outFS.create(imagePath);
    	  plotter.writeImage((Canvas) w, outFile, this.vflip);
    	  outFile.close();
      } else if (w instanceof Shape) {
        // Write the shape to a text file
        Shape s = (Shape) w;
        FSDataOutputStream outFile = dataFiles.get(encodedTileID.get());
        if (outFile == null) {
          Path filePath = getTilePath(tempTileIndex.z, tempTileIndex.x, tempTileIndex.y, DataExt);
          outFile = outFS.create(filePath);
          dataFiles.put(encodedTileID.get(), outFile);
        }
        tempLine.clear();
        s.toText(tempLine);
        tempLine.append(NewLineChars, 0, NewLineChars.length);
        outFile.write(tempLine.getBytes(), 0, tempLine.getLength());
      }
      task.progress();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      // Close all open data files
      for (Map.Entry<Long, FSDataOutputStream> entry : dataFiles.entrySet()) {
        entry.getValue().close();
      }
      dataFiles.clear();
    }
  }
  
  @Override
  public RecordWriter<LongWritable, Writable> getRecordWriter(
      TaskAttemptContext task) throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(task, "").getParent();
    FileSystem fs = file.getFileSystem(task.getConfiguration());
    return new ImageRecordWriter(fs, file, task);
  }
  
  /**
   * Finalizes the job by adding a default empty tile and writing
   * an HTML file to navigate generated images
   * folder.
   * @author Ahmed Eldawy
   *
   */
  public static class MultiLevelOutputCommitter extends FileOutputCommitter {
    
    /**Job output path*/
    private Path outPath;

    public MultiLevelOutputCommitter(Path outputPath, TaskAttemptContext context)
        throws IOException {
      super(outputPath, context);
      this.outPath = outputPath;
    }
    
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      Configuration conf = context.getConfiguration();
      FileSystem outFs = outPath.getFileSystem(conf);

      // Write a default empty image to be displayed for non-generated tiles
      int tileWidth = conf.getInt("tilewidth", 256);
      int tileHeight = conf.getInt("tileheight", 256);
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
      String[] strLevels = conf.get("levels", "7").split("\\.\\.");
      int minLevel, maxLevel;
      if (strLevels.length == 1) {
        minLevel = 0;
        maxLevel = Integer.parseInt(strLevels[0]) - 1;
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
        lineStr = lineStr.replace("#{TILE_URL}", "'tile-' + zoom + '-' + coord.x + '-' + coord.y + '.png'");

        htmlOut.println(lineStr);
      }
      templateFileReader.close();
      htmlOut.close();
      
      PrintStream confOut = new PrintStream(outFs.create(new Path(outPath,
              "Configuration.txt")));
      confOut.println("DirectoryName="+outPath.getName());
      Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
      confOut.println("x1="+inputMBR.x1);
      confOut.println("x2="+inputMBR.x2);
      confOut.println("y1="+inputMBR.y1);
      confOut.println("y2="+inputMBR.y2);
      confOut.println("vflip="+conf.getBoolean("vflip", true));
      confOut.println("Shape="+conf.get("shape"));
      confOut.println("plotter="+conf.getClass(Plotter.PlotterClass, Plotter.class).getName());
      confOut.close();
    }
  }
  
  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException {
    Path jobOutputPath = getOutputPath(context);
    return new MultiLevelOutputCommitter(jobOutputPath, context);
  }
  
}
