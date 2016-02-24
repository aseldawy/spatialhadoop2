/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.Parallel;

/**
 * Writes canvases to a binary output file
 * @author Ahmed Eldawy
 *
 */
public class CanvasOutputFormat extends FileOutputFormat<Object, Canvas> {
  
  private static final String InputMBR = "mbr";

  /**
   * Writes canvases to a file
   * @author Ahmed Eldawy
   *
   */
  class CanvasRecordWriter extends RecordWriter<Object, Canvas> {
    /**The output file where all canvases are written*/
    private FSDataOutputStream outFile;
    /**Plotter used to merge intermediate canvases*/
    private Plotter plotter;
    /**The canvas resulting of merging all written canvases*/
    private Canvas mergedCanvas;
    /**Associated task context to report progress*/
    private TaskAttemptContext task;
    
    public CanvasRecordWriter(FileSystem fs, Path taskOutputPath,
        TaskAttemptContext task) throws IOException {
      Configuration conf = task.getConfiguration();
      this.task = task;
      this.outFile = fs.create(taskOutputPath);
      this.plotter = Plotter.getPlotter(conf);
      int imageWidth = conf.getInt("width", 1000);
      int imageHeight = conf.getInt("height", 1000);
      Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
      this.mergedCanvas = plotter.createCanvas(imageWidth, imageHeight, inputMBR);
    }

    @Override
    public void write(Object dummy, Canvas r) throws IOException {
      plotter.merge(mergedCanvas, r);
      task.progress();
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      // Write the merged canvas
      mergedCanvas.write(outFile);
      outFile.close();
    }
  }
  
  @Override
  public RecordWriter<Object, Canvas> getRecordWriter(
      TaskAttemptContext task) throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(task, "");
    FileSystem fs = file.getFileSystem(task.getConfiguration());
    return new CanvasRecordWriter(fs, file, task);
  }

  
  protected static void mergeImages(final Configuration conf, final Path outPath)
      throws IOException, InterruptedException {
    final int width = conf.getInt("width", 1000);
    final int height = conf.getInt("height", 1000);
    final Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, InputMBR);
    
    final boolean vflip = conf.getBoolean("vflip", true);
    
    // List all output files resulting from reducers
    final FileSystem outFs = outPath.getFileSystem(conf);
    final FileStatus[] resultFiles = outFs.listStatus(outPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toUri().getPath().contains("part-");
      }
    });
    
    if (resultFiles.length == 0) {
      System.err.println("Error! Couldn't find any partial output. Exiting!");
      return;
    }
    System.out.println(System.currentTimeMillis()+": Merging "+resultFiles.length+" layers into one");
    List<Canvas> intermediateLayers = Parallel.forEach(resultFiles.length, new Parallel.RunnableRange<Canvas>() {
      @Override
      public Canvas run(int i1, int i2) {
        Plotter plotter = Plotter.getPlotter(conf);
        // The canvas that contains the merge of all assigned layers
        Canvas finalLayer = null;
        Canvas tempLayer = plotter.createCanvas(1, 1, new Rectangle());
        for (int i = i1; i < i2; i++) {
          FileStatus resultFile = resultFiles[i];
          try {
            FSDataInputStream inputStream = outFs.open(resultFile.getPath());
            while (inputStream.getPos() < resultFile.getLen()) {
              if (tempLayer == finalLayer) {
                // More than one layer. Create a separate final layer to merge
                finalLayer = plotter.createCanvas(width, height, inputMBR);
                plotter.merge(finalLayer, tempLayer);
              }
              tempLayer.readFields(inputStream);
              
              if (finalLayer == null) {
                // First layer. Treat it as a final layer to avoid merging
                // if it is the only layer
                finalLayer = tempLayer;
              } else {
                // More than only layer. Merge into the final layer
                plotter.merge(finalLayer, tempLayer);
              }
            }
            inputStream.close();
          } catch (IOException e) {
            System.err.println("Error reading "+resultFile);
            e.printStackTrace();
          }
        }
        return finalLayer;
      }
    }, conf.getInt("parallel", Runtime.getRuntime().availableProcessors()));
    
    // Merge all intermediate layers into one final layer
    Plotter plotter = Plotter.getPlotter(conf);
    Canvas finalLayer;
    if (intermediateLayers.size() == 1) {
      finalLayer = intermediateLayers.get(0);
    } else {
      finalLayer = plotter.createCanvas(width, height, inputMBR);
      for (Canvas intermediateLayer : intermediateLayers) {
        plotter.merge(finalLayer, intermediateLayer);
      }
    }
    
    // Finally, write the resulting image to the given output path
    System.out.println(System.currentTimeMillis()+": Writing final image");
    outFs.delete(outPath, true); // Delete old (non-combined) images
    FSDataOutputStream outputFile = outFs.create(outPath);
    plotter.writeImage(finalLayer, outputFile, vflip);
    outputFile.close();
  }
  
  
  
  /**
   * Writes the final image by doing any remaining merges then generating
   * the final image
   * @author Ahmed Eldawy
   *
   */
  public static class ImageWriter extends FileOutputCommitter {
    /**Job output path*/
    private Path outPath;

    public ImageWriter(Path outputPath, TaskAttemptContext context)
        throws IOException {
      super(outputPath, context);
      this.outPath = outputPath;
    }
    
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      final Configuration conf = context.getConfiguration();
      
      try {
        mergeImages(conf, outPath);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * An alternative output committer that uses the old MapReduce API. I found
   * that I need this one when working with LocalJopRunner in 1.2.1 as it
   * still calls this output committer
   * @author Ahmed Eldawy
   *
   */
  public static class ImageWriterOld extends org.apache.hadoop.mapred.FileOutputCommitter {
    
    @Override
    public void commitJob(org.apache.hadoop.mapred.JobContext context)
        throws IOException {
      super.commitJob(context);
      Path outPath = CanvasOutputFormat.getOutputPath(context);
      try {
        mergeImages(context.getConfiguration(), outPath);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException {
    context.setStatus("Merging images");
    Path jobOutputPath = getOutputPath(context);
    return new ImageWriter(jobOutputPath, context);
  }
}
