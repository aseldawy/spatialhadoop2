/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
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

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridPartitioner;
import edu.umn.cs.spatialHadoop.core.Partitioner;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat;
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
    /**Whether the configured rasterizer defines a smooth function or not*/
    private boolean smooth;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      this.outputValue = new IntWritable(0);
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.smooth = rasterizer.isSmooth();
      this.numReducers = Math.max(1, job.getNumReduceTasks());
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
      if (smooth)
        shapes = rasterizer.smooth(shapes);
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

    /**Whether the configured rasterizer defines a smooth function or not*/
    private boolean smooth;


    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.partitioner = Partitioner.getPartitioner(job);
      this.rasterizer = Rasterizer.getRasterizer(job);
      this.smooth = rasterizer.isSmooth();
      this.inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);
      this.imageWidth = job.getInt("width", 1000);
      this.imageHeight = job.getInt("height", 1000);
    }
    
    @Override
    public void map(Rectangle dummy, Iterable<? extends Shape> shapes,
        final OutputCollector<IntWritable, Shape> output, Reporter reporter)
        throws IOException {
      final IntWritable partitionID = new IntWritable();
      int i = 0;
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
        if (((++i) & 0xffff) == 0) {
          reporter.progress();
        }
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
      if (smooth) {
        final Iterator<Shape> inputShapes = shapes;
        shapes = rasterizer.smooth(new Iterable<Shape>() {
          @Override
          public Iterator<Shape> iterator() {
            return inputShapes;
          }
        }).iterator();
      }

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
      final JobConf job = context.getJobConf();
      Path outPath = RasterOutputFormat.getOutputPath(job);
      
      final int width = job.getInt("width", 1000);
      final int height = job.getInt("height", 1000);
      final Rectangle inputMBR = (Rectangle) OperationsParams.getShape(job, InputMBR);

      boolean vflip = job.getBoolean("vflip", true);

      // List all output files resulting from reducers
      final FileSystem outFs = outPath.getFileSystem(job);
      final FileStatus[] resultFiles = outFs.listStatus(outPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.toUri().getPath().contains("part-");
        }
      });
      
      System.out.println(System.currentTimeMillis()+": Merging "+resultFiles.length+" layers into one");
      Vector<RasterLayer> intermediateLayers = Parallel.forEach(resultFiles.length, new Parallel.RunnableRange<RasterLayer>() {
        @Override
        public RasterLayer run(int i1, int i2) {
          Rasterizer rasterizer = Rasterizer.getRasterizer(job);
          int layerCount = 0;
          // The raster layer that contains the merge of all assigned layers
          RasterLayer finalLayer = null;
          RasterLayer tempLayer = rasterizer.createRaster(1, 1, new Rectangle());
          for (int i = i1; i < i2; i++) {
            FileStatus resultFile = resultFiles[i];
            try {
              FSDataInputStream inputStream = outFs.open(resultFile.getPath());
              while (inputStream.getPos() < resultFile.getLen()) {
                tempLayer.readFields(inputStream);
                if (layerCount == 0) {
                  // Assign this layer as a final layer
                  finalLayer = tempLayer;
                } else if (layerCount == 1) {
                  // Only one layer. Create a new final layer and merge both
                  RasterLayer newFinalLayer = rasterizer.createRaster(width, height, inputMBR);
                  rasterizer.merge(newFinalLayer, finalLayer);
                  rasterizer.merge(newFinalLayer, tempLayer);
                  finalLayer = newFinalLayer;
                } else {
                  rasterizer.merge(finalLayer, tempLayer);
                }
                layerCount++;
                System.out.println(System.currentTimeMillis()+": Merged "+layerCount);
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
      Rasterizer rasterizer = Rasterizer.getRasterizer(job);
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
    Rectangle inputMBR = (Rectangle) params.getShape("mbr");
    if (inputMBR == null)
      inputMBR = FileMBR.fileMBR(inFile, params);
    OperationsParams.setShape(job, InputMBR, inputMBR);
    
    // Adjust width and height if aspect ratio is to be kept
    int width = job.getInt("width", 1000);
    int height = job.getInt("height", 1000);
    if (params.getBoolean("keepratio", true)) {
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
    
    boolean merge = job.getBoolean("merge", true);
    // Set input and output
    job.setInputFormat(ShapeIterInputFormat.class);
    ShapeIterInputFormat.setInputPaths(job, inFile);
    if (merge)
      job.setOutputFormat(RasterOutputFormat.class);
    else
      job.setOutputFormat(ImageOutputFormat.class);
    RasterOutputFormat.setOutputPath(job, outFile);
    
    // Set mapper and reducer based on the partitioning scheme
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));
    String partition = job.get("partition", "none");
    if (partition.equalsIgnoreCase("none")) {
      LOG.info("Using no-partition plot");
      job.setMapperClass(NoPartitionPlot.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(rasterizer.getRasterClass());
      if (merge) {
        job.setReducerClass(NoPartitionPlot.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));        
      } else {
        job.setNumReduceTasks(0);
      }
    } else {
      LOG.info("Using repartition plot");
      Partitioner partitioner;
      if (partition.equalsIgnoreCase("grid")) {
        partitioner = new GridPartitioner(inFile, job);
      } else if (partition.equalsIgnoreCase("pixel")) {
        // Use pixel level partitioning (one partition per pixel)
        partitioner = new GridPartitioner(inFile, job, width, height);
      } else {
        throw new RuntimeException("Unknown partitioning scheme '"+partition+"'");
      }
      Shape shape = params.getShape("shape");
      job.setMapperClass(RepartitionPlot.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(shape.getClass());
      job.setReducerClass(RepartitionPlot.class);
      job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));        
      Partitioner.setPartitioner(job, partitioner);
    }
    if (merge)
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
    InputSplit[] splits;
    FileSystem inFs = inFile.getFileSystem(params);
    final ShapeIterInputFormat inputFormat = new ShapeIterInputFormat();
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFStatus != null && !inFStatus.isDir()) {
      // One file, retrieve it immediately.
      // This is useful if the input is a hidden file which is automatically
      // skipped by FileInputFormat. We need to plot a hidden file for the case
      // of plotting partition boundaries of a spatial index
      splits = new InputSplit[] {new FileSplit(inFile, 0, inFStatus.getLen(), new String[0])};
    } else {
      JobConf job = new JobConf(params);
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
            RecordReader<Rectangle, Iterable<? extends Shape>> reader =
                inputFormat.getRecordReader(fsplits[i], new JobConf(params), null);
            Rectangle partitionMBR = reader.createKey();
            Iterable<? extends Shape> shapes = reader.createValue();
            
            while (reader.next(partitionMBR, shapes)) {
              if (!partitionMBR.isValid())
                partitionMBR.set(inputMBR);
              
              // Run the rasterize step
              rasterizer.rasterize(partialRaster,
                  rasterizer.isSmooth() ? rasterizer.smooth(shapes) : shapes);
            }
            reader.close();
          } catch (IOException e) {
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
