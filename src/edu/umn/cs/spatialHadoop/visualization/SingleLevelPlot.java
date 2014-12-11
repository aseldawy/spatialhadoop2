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
import java.util.Random;
import java.util.Vector;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.ImageOutputFormat;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridPartitioner;
import edu.umn.cs.spatialHadoop.core.Partitioner;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;


/**
 * @author Ahmed Eldawy
 *
 */
public class SingleLevelPlot {
  private static final Log LOG = LogFactory.getLog(SingleLevelPlot.class);

  /**Configuration line for input file MBR*/
  private static final String InputMBR = "mbr";

  /**
   * Visualizes a dataset using the existing partitioning of a file.
   * The mapper creates a partial raster layer for each partition while the reducer
   * merges the partial rasters together into the final raster layer.
   * The final raster layer is then written to the output.
   * 
   * @author Ahmed Eldawy
   *
   */
  public static class NoPartitionPlot extends MapReduceBase 
    implements Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, RasterLayer>,
    Reducer<IntWritable, RasterLayer, NullWritable, RasterLayer> {
    
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
    /**Number of reduce jobs*/
    private int numReducers;
    /**Random number generator to send the raster layer to a random reducer*/
    private Random random;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      this.outputValue = new IntWritable(0);
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.numReducers = job.getNumReduceTasks();
      this.random = new Random();
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
      RasterLayer rasterLayer = rasterizer.createRaster(rasterLayerX2 - rasterLayerX1, rasterLayerY2 - rasterLayerY1, partitionMBR);
      rasterizer.rasterize(rasterLayer, shapes);
      // If we set the output value to one constant, all intermediate layers
      // will be merged in one machine. Alternatively, We can set it to several values
      // to allow multiple reducers to collaborate in merging intermediate
      // layers. We will need to run a follow up merge process that runs
      // on a single machine in the OutputCommitter function.
      outputValue.set(random.nextInt(numReducers));
      output.collect(outputValue, rasterLayer);
    }
    
    @Override
    public void reduce(IntWritable key, Iterator<RasterLayer> intermediateLayers,
        OutputCollector<NullWritable, RasterLayer> output, Reporter reporter)
        throws IOException {
      RasterLayer finalLayer = rasterizer.createRaster(imageWidth, imageHeight, inputMBR);
      
      while (intermediateLayers.hasNext()) {
        RasterLayer intermediateLayer = intermediateLayers.next();
        rasterizer.merge(finalLayer, intermediateLayer);
        reporter.progress(); // Indicate progress to avoid reducer timeout
      }
      
      output.collect(NullWritable.get(), finalLayer);
    }
  }

  /**
   * A
   * @author eldawy
   *
   */
  public static class RepartitionPlot extends MapReduceBase 
    implements Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape>,
    Reducer<IntWritable, Shape, NullWritable, RasterLayer> {

    /**The partitioner used to partitioner the data across reducers*/
    private Partitioner partitioner;
    
    /**The component that rasterizes the shapes*/
    private Rasterizer rasterizer;

    /**MBR of the input file*/
    private Rectangle inputMBR;
    
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;


    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.partitioner = Partitioner.getPartitioner(job);
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);

    }
    
    @Override
    public void map(Rectangle dummy, Iterable<? extends Shape> shapes,
        final OutputCollector<IntWritable, Shape> output, Reporter reporter)
        throws IOException {
      final IntWritable partitionID = new IntWritable();
      for (final Shape shape : shapes) {
        partitioner.overlapPartitions(shape, new ResultCollector<Integer>() {
          @Override
          public void collect(Integer r) {
            partitionID.set(r);
            try {
              output.collect(partitionID, shape);
            } catch (IOException e) {
              LOG.warn("Error checking overlapping partitions", e);
            }
          }
        });
      }
    }

    @Override
    public void reduce(IntWritable partitionID, Iterator<Shape> shapes,
        OutputCollector<NullWritable, RasterLayer> output, Reporter reporter)
        throws IOException {
      CellInfo partition = partitioner.getPartition(partitionID.get());
      int rasterLayerX1 = (int) Math.floor((partition.x1 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerX2 = (int) Math.ceil((partition.x2 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerY1 = (int) Math.floor((partition.y1 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      int rasterLayerY2 = (int) Math.ceil((partition.y2 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      RasterLayer rasterLayer = rasterizer.createRaster(rasterLayerX2 - rasterLayerX1, rasterLayerY2 - rasterLayerY1, partition);
      rasterizer.rasterize(rasterLayer, shapes);
      output.collect(NullWritable.get(), rasterLayer);
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
      Rasterizer rasterizer = Rasterizer.getRasterizer(job);
      int width = job.getInt("width", 1000);
      int height = job.getInt("height", 1000);
      Rectangle inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);

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
      // Create any raster layer to be able to deserialize the output files
      RasterLayer intermediateLayer = rasterizer.createRaster(1, 1, new Rectangle());
      // The final raster layer contains the merge of all intermediate layers
      RasterLayer finalLayer = rasterizer.createRaster(width, height, inputMBR);
      LOG.info("Merging "+resultFiles.length+" layers into one");
      for (FileStatus resultFile : resultFiles) {
        FSDataInputStream inputStream = outFs.open(resultFile.getPath());
        while (inputStream.getPos() < resultFile.getLen()) {
          // Read next raster layer in file
          intermediateLayer.readFields(inputStream);
          rasterizer.merge(finalLayer, intermediateLayer);
        }
        inputStream.close();
      }
      
      LOG.info("Converting the final raster layer into an image");
      // Convert the final layer to image
      BufferedImage finalImage = rasterizer.write(finalLayer);
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
    job.setJobName("SingleLevelPlot");
    // Set rasterizer
    Rasterizer.setRasterizer(job, rasterizerClass);
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
    
    // Set mapper and reducer based on the partitioning scheme
    String partition = job.get("partition", "none");
    if (partition.equalsIgnoreCase("none")) {
      job.setMapperClass(NoPartitionPlot.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(rasterizer.getRasterClass());
      job.setReducerClass(NoPartitionPlot.class);
    } else {
      Partitioner partitioner;
      if (partition.equalsIgnoreCase("grid")) {
        partitioner = new GridPartitioner(inFile, job);
      } else {
        throw new RuntimeException("Unknown partitioning scheme '"+partition+"'");
      }
      Shape shape = params.getShape("shape");
      job.setMapperClass(RepartitionPlot.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(shape.getClass());
      job.setReducerClass(RepartitionPlot.class);
      Partitioner.setPartitioner(job, partitioner);
    }
    job.setOutputCommitter(ImageWriter.class);
    
    // Use multithreading in case the job is running locally
    job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());

    // Start the job
    JobClient.runJob(job);
  }

  public static void plotLocal(Path inFile, Path outFile,
      final Class<? extends Rasterizer> rasterizerClass, final OperationsParams params) throws IOException {

    boolean vflip = params.getBoolean("vflip", true);

    Shape plotRange = params.getShape("rect", null);
    final Rectangle inputMBR = plotRange == null ? FileMBR.fileMBR(inFile, params) : plotRange.getMBR();

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
    // Store width and height in final variables to make them accessible in parallel
    final int fwidth = width, fheight = height;

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
    
    // Copy splits to a final array to be used in parallel
    final FileSplit[] fsplits = new FileSplit[splits.length];
    System.arraycopy(splits, 0, fsplits, 0, splits.length);
    
    Vector<RasterLayer> partialRasters = Parallel.forEach(splits.length, new RunnableRange<RasterLayer>() {
      @Override
      public RasterLayer run(int i1, int i2) {
        Rasterizer rasterizer;
        try {
          rasterizer = rasterizerClass.newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException("Error creating rastierizer", e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Error creating rastierizer", e);
        }
        rasterizer.configure(params);
        // Create the partial layer that will contain the rasterization of the assigned partitions
        RasterLayer partialRaster = rasterizer.createRaster(fwidth, fheight, inputMBR);
        
        for (int i = i1; i < i2; i++) {
          try {
            RecordReader<Rectangle, ShapeIterator> reader =
                new ShapeIterRecordReader(params, fsplits[i]);
            Rectangle partitionMBR = reader.createKey();
            ShapeIterator shapes = reader.createValue();
            
            while (reader.next(partitionMBR, shapes)) {
              if (!partitionMBR.isValid())
                partitionMBR.set(inputMBR);
              
              // Run the rasterize step
              rasterizer.rasterize(partialRaster, (Iterable<Shape>)shapes);
            }
            reader.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        return partialRaster;
      }
    });
    Rasterizer rasterizer;
    try {
      rasterizer = rasterizerClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    }
    // Create the final raster layer that will contain the final image
    RasterLayer finalRaster = rasterizer.createRaster(fwidth, fheight, inputMBR);
    for (RasterLayer partialRaster : partialRasters) {
      rasterizer.merge(finalRaster, partialRaster);
    }
    BufferedImage finalImage = rasterizer.write(finalRaster);

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
  
  /**
   * Plots the given file using the provided rasterizer
   * @param inFile
   * @param outFile
   * @param rasterizerClass
   * @param params
   * @throws IOException
   */
  public static void plot(Path inFile, Path outFile,
      final Class<? extends Rasterizer> rasterizerClass, final OperationsParams params) throws IOException {
    if (OperationsParams.isLocal(new JobConf(params), inFile)) {
      plotLocal(inFile, outFile, rasterizerClass, params);
    } else {
      plotMapReduce(inFile, outFile, rasterizerClass, params);
    }
  }
}
