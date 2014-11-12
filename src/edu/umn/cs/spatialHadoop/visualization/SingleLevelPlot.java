/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.operations.FileMBR;


/**
 * @author Ahmed Eldawy
 *
 */
public class SingleLevelPlot {
  private static final Log LOG = LogFactory.getLog(SingleLevelPlot.class);

  /**Configuration line for input file MBR*/
  private static final String InputMBR = "mbr";

  /**Configuration line for the rasterizer class*/
  private static final String RasterizerClass = "SingleLevelPlot.Rasterizer";
  
  static Rasterizer getRasterizer(Configuration job) {
    try {
      Class<? extends Rasterizer> rasterizerClass =
          job.getClass(RasterizerClass, null, Rasterizer.class);
      if (rasterizerClass == null)
        throw new RuntimeException("Rasterizer class not set in job");
      return rasterizerClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rasterizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error constructing rasterizer", e);
    }
  }


  /**
   * Mapper of the plot for the case when the data is not repartitioned.
   * The input key is the MBR of the input partition
   * The input value is the list of shapes to draw
   * The output key is an integer to allow combining intermediate layers
   * The output value is the raster layer that represents that partition
   * @author Ahmed Eldawy
   *
   */
  public static class NoPartitionRasterizer extends MapReduceBase 
    implements Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, RasterLayer> {
    
    /**The MBR of the input file*/
    private Rectangle inputMBR;
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;
    /**The component that rasterizes the shapes*/
    private Rasterizer rasterizer;
    /**Value for the output*/
    private IntWritable outputValue;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      this.outputValue = new IntWritable(0);
      this.rasterizer = getRasterizer(job);
      this.rasterizer.configure(job);
    }
    
    @Override
    public void map(Rectangle partitionMBR, Iterable<? extends Shape> shapes,
        OutputCollector<IntWritable, RasterLayer> output, Reporter reporter)
        throws IOException {
      // If input is not spatially partitioned, the MBR is taken from input
      if (!partitionMBR.isValid())
        partitionMBR.set(inputMBR);
      
      // Calculate the dimensions of the generated raster layer by calculating
      // the MBR in the image space
      // Note: Do not calculate from the width and height of partitionMBR
      // because it will cause round-off errors between adjacent partitions
      // which might leave gaps in the final generated image
      int rasterLayerX1 = (int) Math.floor((partitionMBR.x1 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerX2 = (int) Math.ceil((partitionMBR.x2 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerY1 = (int) Math.floor((partitionMBR.y1 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      int rasterLayerY2 = (int) Math.ceil((partitionMBR.y2 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      RasterLayer rasterLayer = rasterizer.create(rasterLayerX2 - rasterLayerX1, rasterLayerY2 - rasterLayerY1, partitionMBR);
      rasterizer.rasterize(rasterLayer, shapes);
      // If we set the output value to one constant, all intermediate layers
      // will be merged in one machine. Alternatively, We can set it to several values
      // to allow multiple reducers to collaborate in merging intermediate
      // layers. We will need to run a follow up merge process that runs
      // on a single machine in the OutputCommitter function.
      // TODO use different values for multiple reducers and compare the difference
      output.collect(outputValue, rasterLayer);
    }
  }

  /**
   * Merges intermediate raster layers to create final output raster layers 
   * @author Ahmed Eldawy
   *
   */
  public static class NoPartitionMerger extends MapReduceBase 
    implements Reducer<IntWritable, RasterLayer, NullWritable, RasterLayer> {

    /**The component that rasterizes the shapes*/
    private Rasterizer rasterizer;
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;
    private Rectangle inputMBR;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.rasterizer = getRasterizer(job);
      this.rasterizer.configure(job);
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
    }
    
    @Override
    public void reduce(IntWritable key, Iterator<RasterLayer> intermediateLayers,
        OutputCollector<NullWritable, RasterLayer> output, Reporter reporter)
        throws IOException {
      RasterLayer finalLayer = rasterizer.create(imageWidth, imageHeight, inputMBR);
      
      while (intermediateLayers.hasNext()) {
        RasterLayer intermediateLayer = intermediateLayers.next();
        rasterizer.mergeLayers(finalLayer, intermediateLayer);
        reporter.progress(); // Indicate progress to avoid reducer timeout
      }
      
      output.collect(NullWritable.get(), finalLayer);
    }
  }
  
  /**
   * Writes the final image by doing any remaining merges then generating
   * the final image
   * @author Ahmed Eldawy
   *
   */
  public static class ImageWriter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      JobConf job = context.getJobConf();
      Path outFile = ImageOutputFormat.getOutputPath(job);
      Rasterizer rasterizer = getRasterizer(job);
      rasterizer.configure(job);
      // Create any raster layer to be able to deserialize the output files
      RasterLayer intermediateLayer = rasterizer.create(1, 1, new Rectangle());
      int width = job.getInt("width", 1000);
      int height = job.getInt("height", 1000);
      Rectangle inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      RasterLayer finalLayer = rasterizer.create(width, height, inputMBR);

      boolean vflip = job.getBoolean("vflip", true);

      // Combine all images in one file
      // Rename output file
      // Combine all output files into one file as we do with grid files
      FileSystem outFs = outFile.getFileSystem(job);
      Path temp = new Path(outFile.toUri().getPath()+"_"+(int)(Math.random()*1000000)+".tmp");
      outFs.rename(outFile, temp);
      FileStatus[] resultFiles = outFs.listStatus(temp, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.toUri().getPath().contains("part-");
        }
      });
      
      // Read all intermediate layers and merge into one final layer
      for (FileStatus resultFile : resultFiles) {
        FSDataInputStream inputStream = outFs.open(resultFile.getPath());
        while (inputStream.getPos() < resultFile.getLen()) {
          // Read next raster layer in file
          intermediateLayer.readFields(inputStream);
          rasterizer.mergeLayers(finalLayer, intermediateLayer);
        }
        inputStream.close();
      }
      
      // Convert the final layer to image
      BufferedImage finalImage = rasterizer.toImage(finalLayer);
      // Flip image vertically if needed
      if (vflip) {
        AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
        tx.translate(0, -finalImage.getHeight());
        AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        finalImage = op.filter(finalImage, null);
      }
      
      // Finally, write the resulting image to the given output path
      LOG.info("Writing final image");
      OutputStream outputFile = outFs.create(outFile);
      ImageIO.write(finalImage, "png", outputFile);
      outputFile.close();

      outFs.delete(temp, true);
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
    // Set rasterizer
    job.setClass(RasterizerClass, rasterizerClass, Rasterizer.class);
    // Set input file MBR
    Rectangle inputMBR = (Rectangle) params.getShape("rect");
    if (inputMBR == null)
      inputMBR = FileMBR.fileMBR(inFile, params);
    OperationsParams.setShape(job, InputMBR, inputMBR);
    
    // Adjust width and height if aspect ratio is to be kept
    if (params.getBoolean("keepratio", true)) {
      int width = job.getInt("width", 1000);
      int height = job.getInt("height", 1000);
      // Adjust width and height to maintain aspect ratio
      if (inputMBR.getWidth() / inputMBR.getHeight() > (double) width / height) {
        // Fix width and change height
        height = (int) (inputMBR.getHeight() * width / inputMBR.getWidth());
        // Make divisible by two for compatibility with ffmpeg
        height &= 0xfffffffe;
        job.setInt("height", height);
      } else {
        width = (int) (inputMBR.getWidth() * height / inputMBR.getHeight());
        job.setInt("width", width);
      }
    }
    
    // Set input and output
    job.setInputFormat(ShapeIterInputFormat.class);
    ShapeIterInputFormat.setInputPaths(job, inFile);
    job.setOutputFormat(RasterOutputFormat.class);
    RasterOutputFormat.setOutputPath(job, outFile);
    
    // Set mapper, reducer and committer
    job.setMapperClass(NoPartitionRasterizer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(rasterizer.getRasterClass());
    job.setReducerClass(NoPartitionMerger.class);
    job.setOutputCommitter(ImageWriter.class);

    // Start the job
    JobClient.runJob(job);
  }

  public static void plotLocal(Path inFile, Path outFile,
      Class<? extends Rasterizer> rasterizerClass, OperationsParams params) throws IOException {
    Rasterizer rasterizer;
    try {
      rasterizer = rasterizerClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    }
    
    rasterizer.configure(params);
    boolean vflip = params.getBoolean("vflip", true);

    Shape plotRange = params.getShape("rect", null);
    Rectangle inputMBR;
    if (plotRange != null) {
      inputMBR = plotRange.getMBR();
    } else {
      inputMBR = FileMBR.fileMBR(inFile, params);
    }

    // Retrieve desired output image size and keep aspect ratio if needed
    int width = params.getInt("width", 1000);
    int height = params.getInt("height", 1000);
    if (params.getBoolean("keepratio", true)) {
      // Adjust width and height to maintain aspect ratio
      if (inputMBR.getWidth() / inputMBR.getHeight() > (double) width / height) {
        // Fix width and change height
        height = (int) (inputMBR.getHeight() * width / inputMBR.getWidth());
      } else {
        width = (int) (inputMBR.getWidth() * height / inputMBR.getHeight());
      }
    }

    // Create final layer that will contain the image
    RasterLayer finalRaster = rasterizer.create(width, height, inputMBR);

    // Start reading input file
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
      ShapeIterInputFormat inputFormat = new ShapeIterInputFormat();
      ShapeIterInputFormat.addInputPath(job, inFile);
      splits = inputFormat.getSplits(job, 1);
    }

    for (InputSplit split : splits) {
      RecordReader<Rectangle, ShapeIterator> reader =
          new ShapeIterRecordReader(params, (FileSplit)split);
      Rectangle partitionMBR = reader.createKey();
      ShapeIterator shapes = reader.createValue();
      
      while (reader.next(partitionMBR, shapes)) {
        if (!partitionMBR.isValid())
          partitionMBR.set(inputMBR);
        
        // Run the rasterize step
        
        rasterizer.rasterize(finalRaster, shapes);
      }
      reader.close();
    }
    
    BufferedImage finalImage = rasterizer.toImage(finalRaster);

    // Flip image vertically if needed
    if (vflip) {
      AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
      tx.translate(0, -finalImage.getHeight());
      AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
      finalImage = op.filter(finalImage, null);
    }
    
    // Finally, write the resulting image to the given output path
    LOG.info("Writing final image");
    FileSystem outFs = outFile.getFileSystem(params);
    OutputStream outputFile = outFs.create(outFile);
    ImageIO.write(finalImage, "png", outputFile);
    outputFile.close();
  }
}
