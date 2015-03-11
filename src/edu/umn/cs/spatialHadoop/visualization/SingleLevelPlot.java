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
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Partitioner;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader3;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.operations.Indexer;
import edu.umn.cs.spatialHadoop.operations.RangeFilter;
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
  public static class NoPartitionPlotMap<S extends Shape>
    extends Mapper<Rectangle, Iterable<S>, IntWritable, RasterLayer> {
    
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
    /**Whether the configured rasterizer defines a smooth function or not*/
    private boolean smooth;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.imageWidth = conf.getInt("width", 1000);
      this.imageHeight = conf.getInt("height", 1000);
      this.inputMBR = (Rectangle) OperationsParams.getShape(conf, InputMBR);
      this.outputValue = new IntWritable(0);
      this.rasterizer = Rasterizer.getRasterizer(conf);
      this.smooth = rasterizer.isSmooth();
      this.numReducers = Math.max(1, context.getNumReduceTasks());
      this.random = new Random();
    }

    @Override
    protected void map(Rectangle partitionMBR, Iterable<S> shapes,
        Context context) throws IOException, InterruptedException {
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
      if (smooth)
        shapes = rasterizer.smooth(shapes);
      rasterizer.rasterize(rasterLayer, shapes);
      // If we set the output value to one constant, all intermediate layers
      // will be merged in one machine. Alternatively, We can set it to several values
      // to allow multiple reducers to collaborate in merging intermediate
      // layers. We will need to run a follow up merge process that runs
      // on a single machine in the OutputCommitter function.
      outputValue.set(random.nextInt(numReducers));
      context.write(outputValue, rasterLayer);
    }
  }
  
  public static class NoPartitionPlotReduce<S extends Shape>
    extends Reducer<IntWritable, RasterLayer, NullWritable, RasterLayer> {

    /**The MBR of the input file*/
    private Rectangle inputMBR;
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;
    /**The component that rasterizes the shapes*/
    private Rasterizer rasterizer;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.imageWidth = conf.getInt("width", 1000);
      this.imageHeight = conf.getInt("height", 1000);
      this.inputMBR = (Rectangle) OperationsParams.getShape(conf, InputMBR);
      this.rasterizer = Rasterizer.getRasterizer(conf);
    }
    
    @Override
    protected void reduce(IntWritable dummy,
        Iterable<RasterLayer> intermediateLayers, Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      RasterLayer finalLayer = rasterizer.createRaster(imageWidth, imageHeight, inputMBR);
      
      for (RasterLayer intermediateLayer : intermediateLayers) {
        rasterizer.merge(finalLayer, intermediateLayer);
        context.progress();
      }
      
      context.write(NullWritable.get(), finalLayer);
    }

  }
  
  
  public static class RepartitionPlotMap<S extends Shape>
    extends Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {
    /**The partitioner used to partitioner the data across reducers*/
    private Partitioner partitioner;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.partitioner = Partitioner.getPartitioner(conf);
    }
    
    @Override
    protected void map(Rectangle key, Iterable<? extends Shape> shapes,
        final Context context) throws IOException, InterruptedException {

      final IntWritable partitionID = new IntWritable();
      int i = 0;
      for (final Shape shape : shapes) {
        partitioner.overlapPartitions(shape, new ResultCollector<Integer>() {
          @Override
          public void collect(Integer r) {
            partitionID.set(r);
            try {
              context.write(partitionID, shape);
            } catch (IOException e) {
              LOG.warn("Error checking overlapping partitions", e);
            } catch (InterruptedException e) {
              LOG.warn("Error checking overlapping partitions", e);
            }
          }
        });
        if (((++i) & 0xff) == 0) {
          context.progress();
        }
      }
    
    }
  }
  
  public static class RepartitionPlotReduce
      extends Reducer<IntWritable, Shape, NullWritable, RasterLayer> {
    
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

    /**Whether the configured rasterizer defines a smooth function or not*/
    private boolean smooth;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.partitioner = Partitioner.getPartitioner(conf);
      this.rasterizer = Rasterizer.getRasterizer(conf);
      this.smooth = rasterizer.isSmooth();
      this.inputMBR = (Rectangle) OperationsParams.getShape(conf, InputMBR);
      this.imageWidth = conf.getInt("width", 1000);
      this.imageHeight = conf.getInt("height", 1000);
    }
    
    @Override
    protected void reduce(IntWritable partitionID, Iterable<Shape> shapes,
        Context context) throws IOException, InterruptedException {
      CellInfo partition = partitioner.getPartition(partitionID.get());
      int rasterLayerX1 = (int) Math.floor((partition.x1 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerX2 = (int) Math.ceil((partition.x2 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int rasterLayerY1 = (int) Math.floor((partition.y1 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      int rasterLayerY2 = (int) Math.ceil((partition.y2 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      RasterLayer rasterLayer = rasterizer.createRaster(rasterLayerX2 - rasterLayerX1, rasterLayerY2 - rasterLayerY1, partition);
      if (smooth) {
        shapes = rasterizer.smooth(shapes);
      }

      rasterizer.rasterize(rasterLayer, shapes);
      context.write(NullWritable.get(), rasterLayer);
    }
  }
  
  /**
   * Generates a single level using a MapReduce job and returns the created job.
   * @param inFiles
   * @param outFile
   * @param rasterizerClass
   * @param params
   * @return
   * @throws IOException
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static Job plotMapReduce(Path[] inFiles, Path outFile,
      Class<? extends Rasterizer> rasterizerClass, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    Rasterizer rasterizer;
    try {
      rasterizer = rasterizerClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    }
    
    Job job = new Job(params, "SingleLevelPlot");
    job.setJarByClass(SingleLevelPlot.class);
    job.setJobName("SingleLevelPlot");
    // Set rasterizer
    Configuration conf = job.getConfiguration();
    Rasterizer.setRasterizer(conf, rasterizerClass);
    // Set input file MBR
    Rectangle inputMBR = (Rectangle) params.getShape("mbr");
    Rectangle drawRect = (Rectangle) params.getShape("rect");
    if (inputMBR == null)
      inputMBR = drawRect != null? drawRect : FileMBR.fileMBR(inFiles, params);
    OperationsParams.setShape(conf, InputMBR, inputMBR);
    if (drawRect != null)
      conf.setClass(SpatialSite.FilterClass, RangeFilter.class, BlockFilter.class);
    
    // Adjust width and height if aspect ratio is to be kept
    int width = conf.getInt("width", 1000);
    int height = conf.getInt("height", 1000);
    if (params.getBoolean("keepratio", true)) {
      // Adjust width and height to maintain aspect ratio
      if (inputMBR.getWidth() / inputMBR.getHeight() > (double) width / height) {
        // Fix width and change height
        height = (int) (inputMBR.getHeight() * width / inputMBR.getWidth());
        // Make divisible by two for compatibility with ffmpeg
        height &= 0xfffffffe;
        conf.setInt("height", height);
      } else {
        width = (int) (inputMBR.getWidth() * height / inputMBR.getHeight());
        conf.setInt("width", width);
      }
    }
    
    boolean merge = conf.getBoolean("merge", true);
    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inFiles);
    if (conf.getBoolean("output", true)) {
      if (merge) {
        job.setOutputFormatClass(RasterOutputFormat.class);
        conf.setClass("mapred.output.committer.class",
            RasterOutputFormat.ImageWriterOld.class,
            org.apache.hadoop.mapred.OutputCommitter.class);
      } else {
        job.setOutputFormatClass(ImageOutputFormat.class);
      }
      RasterOutputFormat.setOutputPath(job, outFile);
    } else {
      job.setOutputFormatClass(NullOutputFormat.class);
    }
    
    // Set mapper and reducer based on the partitioning scheme
    String partition = conf.get("partition", "none");
    if (partition.equalsIgnoreCase("none")) {
      LOG.info("Using no-partition plot");
      job.setMapperClass(NoPartitionPlotMap.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(rasterizer.getRasterClass());
      if (merge) {
        job.setReducerClass(NoPartitionPlotReduce.class);
        // Set number of reduce tasks according to cluster status
        job.setNumReduceTasks(Math.max(1, new JobClient(new JobConf())
            .getClusterStatus().getMaxReduceTasks() * 7 / 8));
      } else {
        job.setNumReduceTasks(0);
      }
    } else {
      LOG.info("Using repartition plot");
      Partitioner partitioner = Indexer.createPartitioner(inFiles, outFile, conf, partition);
      Shape shape = params.getShape("shape");
      job.setMapperClass(RepartitionPlotMap.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(shape.getClass());
      job.setReducerClass(RepartitionPlotReduce.class);
      // TODO set number of reducers according to cluster size
      //job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));        
      Partitioner.setPartitioner(conf, partitioner);
    }
    
    // Use multithreading in case the job is running locally
    conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    
    // Start the job
    if (params.getBoolean("background", false)) {
      // Run in background
      job.submit();
    } else {
      job.waitForCompletion(false);
    }
    return job;
  }

  public static void plotLocal(Path[] inFiles, Path outFile,
      final Class<? extends Rasterizer> rasterizerClass, final OperationsParams params) throws IOException, InterruptedException {

    boolean vflip = params.getBoolean("vflip", true);

    
    final Rectangle inputMBR = params.get("mbr") != null ?
        params.getShape("mbr").getMBR() : FileMBR.fileMBR(inFiles, params);
    OperationsParams.setShape(params, InputMBR, inputMBR);

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
    Vector<InputSplit> splits = new Vector<InputSplit>();
    final SpatialInputFormat3<Rectangle, Shape> inputFormat =
        new SpatialInputFormat3<Rectangle, Shape>();
    for (Path inFile : inFiles) {
      FileSystem inFs = inFile.getFileSystem(params);
      if (inFs.exists(inFile) && !inFs.isDirectory(inFile)) {
        // One file, retrieve it immediately.
        // This is useful if the input is a hidden file which is automatically
        // skipped by FileInputFormat. We need to plot a hidden file for the case
        // of plotting partition boundaries of a spatial index
        splits.add(new FileSplit(inFile, 0,
            inFs.getFileStatus(inFile).getLen(), new String[0]));
      } else {
        Job job = Job.getInstance(params);
        SpatialInputFormat3.addInputPath(job, inFile);
        splits.addAll(inputFormat.getSplits(job));
      }
      
    }
    
    // Copy splits to a final array to be used in parallel
    final FileSplit[] fsplits = splits.toArray(new FileSplit[splits.size()]);
    
    Vector<RasterLayer> partialRasters = Parallel.forEach(fsplits.length, new RunnableRange<RasterLayer>() {
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
            RecordReader<Rectangle, Iterable<Shape>> reader =
                inputFormat.createRecordReader(fsplits[i], null);
            if (reader instanceof SpatialRecordReader3) {
              ((SpatialRecordReader3)reader).initialize(fsplits[i], params);
            } else if (reader instanceof RTreeRecordReader3) {
              ((RTreeRecordReader3)reader).initialize(fsplits[i], params);
            } else if (reader instanceof HDFRecordReader3) {
              ((HDFRecordReader3)reader).initialize(fsplits[i], params);
            } else {
              throw new RuntimeException("Unknown record reader");
            }

            while (reader.nextKeyValue()) {
              Rectangle partition = reader.getCurrentKey();
              if (!partition.isValid())
                partition.set(inputMBR);

              Iterable<Shape> shapes = reader.getCurrentValue();
              // Run the rasterize step
              rasterizer.rasterize(partialRaster,
                  rasterizer.isSmooth() ? rasterizer.smooth(shapes) : shapes);
            }
            reader.close();
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        return partialRaster;
      }
    });
    boolean merge = params.getBoolean("merge", true);
    Rasterizer rasterizer;
    try {
      rasterizer = rasterizerClass.newInstance();
      rasterizer.configure(params);
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    }
    if (merge) {
      // Create the final raster layer that will contain the final image
      RasterLayer finalRaster = rasterizer.createRaster(fwidth, fheight, inputMBR);
      for (RasterLayer partialRaster : partialRasters)
        rasterizer.merge(finalRaster, partialRaster);
      
      // Finally, write the resulting image to the given output path
      LOG.info("Writing final image");
      FileSystem outFs = outFile.getFileSystem(params);
      FSDataOutputStream outputFile = outFs.create(outFile);
      
      rasterizer.writeImage(finalRaster, outputFile, vflip);
      outputFile.close();
    } else {
      // No merge
      LOG.info("Writing partial images");
      FileSystem outFs = outFile.getFileSystem(params);
      for (int i = 0; i < partialRasters.size(); i++) {
        Path filename = new Path(outFile, String.format("part-%05d.png", i));
        FSDataOutputStream outputFile = outFs.create(filename);
        
        rasterizer.writeImage(partialRasters.get(i), outputFile, vflip);
        outputFile.close();
      }
    }
  }
  
  /**
   * Plots the given file using the provided rasterizer
   * @param inFile
   * @param outFile
   * @param rasterizerClass
   * @param params
   * @throws IOException
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static Job plot(Path[] inFiles, Path outFile,
      final Class<? extends Rasterizer> rasterizerClass,
      final OperationsParams params)
          throws IOException, InterruptedException, ClassNotFoundException {
    if (OperationsParams.isLocal(params, inFiles)) {
      plotLocal(inFiles, outFile, rasterizerClass, params);
      return null;
    } else {
      return plotMapReduce(inFiles, outFile, rasterizerClass, params);
    }
  }
}
