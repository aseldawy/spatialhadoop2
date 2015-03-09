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
import java.util.Vector;

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
 * Writes raster layers to a binary output file
 * @author Ahmed Eldawy
 *
 */
public class RasterOutputFormat extends FileOutputFormat<Object, RasterLayer> {
  
  private static final String InputMBR = "mbr";

  /**
   * Writes raster layers to a file
   * @author Ahmed Eldawy
   *
   */
  class RasterRecordWriter extends RecordWriter<Object, RasterLayer> {
    /**The output file where all raster layers are written*/
    private FSDataOutputStream outFile;
    /**Rasterizer used to merge intermediate raster layers*/
    private Rasterizer rasterizer;
    /**The raster layer resulting of merging all written raster layers*/
    private RasterLayer mergedRasterLayer;
    /**Associated task context to report progress*/
    private TaskAttemptContext task;
    
    public RasterRecordWriter(FileSystem fs, Path taskOutputPath,
        TaskAttemptContext task) throws IOException {
      Configuration conf = task.getConfiguration();
      this.task = task;
      this.outFile = fs.create(taskOutputPath);
      this.rasterizer = Rasterizer.getRasterizer(conf);
      int imageWidth = conf.getInt("width", 1000);
      int imageHeight = conf.getInt("height", 1000);
      Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
      this.mergedRasterLayer = rasterizer.createRaster(imageWidth, imageHeight, inputMBR);
    }

    @Override
    public void write(Object dummy, RasterLayer r) throws IOException {
      rasterizer.merge(mergedRasterLayer, r);
      task.progress();
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      // Write the merged raster layer
      mergedRasterLayer.write(outFile);
      outFile.close();
    }
  }
  
  @Override
  public RecordWriter<Object, RasterLayer> getRecordWriter(
      TaskAttemptContext task) throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(task, "");
    FileSystem fs = file.getFileSystem(task.getConfiguration());
    return new RasterRecordWriter(fs, file, task);
  }

  
  protected static void mergeImages(final Configuration conf, final Path outPath)
      throws IOException {
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
    Vector<RasterLayer> intermediateLayers = Parallel.forEach(resultFiles.length, new Parallel.RunnableRange<RasterLayer>() {
      @Override
      public RasterLayer run(int i1, int i2) {
        Rasterizer rasterizer = Rasterizer.getRasterizer(conf);
        // The raster layer that contains the merge of all assigned layers
        RasterLayer finalLayer = null;
        RasterLayer tempLayer = rasterizer.createRaster(1, 1, new Rectangle());
        for (int i = i1; i < i2; i++) {
          FileStatus resultFile = resultFiles[i];
          try {
            FSDataInputStream inputStream = outFs.open(resultFile.getPath());
            while (inputStream.getPos() < resultFile.getLen()) {
              if (tempLayer == finalLayer) {
                // More than one layer. Create a separate final layer to merge
                finalLayer = rasterizer.createRaster(width, height, inputMBR);
                rasterizer.merge(finalLayer, tempLayer);
              }
              tempLayer.readFields(inputStream);
              
              if (finalLayer == null) {
                // First layer. Treat it as a final layer to avoid merging
                // if it is the only layer
                finalLayer = tempLayer;
              } else {
                // More than only layer. Merge into the final layer
                rasterizer.merge(finalLayer, tempLayer);
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
    });
    
    // Merge all intermediate layers into one final layer
    Rasterizer rasterizer = Rasterizer.getRasterizer(conf);
    RasterLayer finalLayer;
    if (intermediateLayers.size() == 1) {
      finalLayer = intermediateLayers.elementAt(0);
    } else {
      finalLayer = rasterizer.createRaster(width, height, inputMBR);
      for (RasterLayer intermediateLayer : intermediateLayers) {
        rasterizer.merge(finalLayer, intermediateLayer);
      }
    }
    
    // Finally, write the resulting image to the given output path
    System.out.println(System.currentTimeMillis()+": Writing final image");
    outFs.delete(outPath, true); // Delete old (non-combined) images
    FSDataOutputStream outputFile = outFs.create(outPath);
    rasterizer.writeImage(finalLayer, outputFile, vflip);
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
      
      mergeImages(conf, outPath);
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
      Path outPath = RasterOutputFormat.getOutputPath(context);
      mergeImages(context.getConfiguration(), outPath);
    }
  }
  
  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException {
    Path jobOutputPath = getOutputPath(context);
    return new ImageWriter(jobOutputPath, context);
  }
}
