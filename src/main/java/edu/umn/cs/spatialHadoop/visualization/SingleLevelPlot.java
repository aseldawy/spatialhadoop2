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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
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
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GridPartitioner;
import edu.umn.cs.spatialHadoop.indexing.Indexer;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.util.FileUtil;
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
   * The mapper creates a partial canvas for each partition while the reducer
   * merges the partial canvases together into the final canvas.
   * The final canvas is then written to the output.
   * 
   * @author Ahmed Eldawy
   *
   */
  public static class NoPartitionPlotMap<S extends Shape>
    extends Mapper<Rectangle, Iterable<S>, IntWritable, Canvas> {
    
    /**The MBR of the input file*/
    private Rectangle inputMBR;
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;
    /**The component that plots the shapes*/
    private Plotter plotter;
    /**Value for the output*/
    private IntWritable outputValue;
    /**Number of reduce jobs*/
    private int numReducers;
    /**Random number generator to send the canvas to a random reducer*/
    private Random random;
    /**Whether the configured canvas defines a smooth function or not*/
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
      this.plotter = Plotter.getPlotter(conf);
      this.smooth = plotter.isSmooth();
      this.numReducers = Math.max(1, context.getNumReduceTasks());
      this.random = new Random();
    }

    @Override
    protected void map(Rectangle partitionMBR, Iterable<S> shapes,
        Context context) throws IOException, InterruptedException {
      // If input is not spatially partitioned, the MBR is taken from input
      if (!partitionMBR.isValid())
        partitionMBR.set(inputMBR);
      
      // Calculate the dimensions of the generated canvas by calculating
      // the MBR in the image space
      // Note: Do not calculate from the width and height of partitionMBR
      // because it will cause round-off errors between adjacent partitions
      // which might leave gaps in the final generated image
      int canvasX1 = (int) Math.floor((partitionMBR.x1 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int canvasX2 = (int) Math.ceil((partitionMBR.x2 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int canvasY1 = (int) Math.floor((partitionMBR.y1 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      int canvasY2 = (int) Math.ceil((partitionMBR.y2 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      Canvas canvasLayer = plotter.createCanvas(canvasX2 - canvasX1, canvasY2 - canvasY1, partitionMBR);
      if (smooth) {
        shapes = plotter.smooth(shapes);
        context.progress();
      }
      int i = 0;
      for (Shape shape : shapes) {
        plotter.plot(canvasLayer, shape);
        if (((++i) & 0xff) == 0)
          context.progress();
      }
      // If we set the output value to one constant, all intermediate layers
      // will be merged in one machine. Alternatively, We can set it to several values
      // to allow multiple reducers to collaborate in merging intermediate
      // layers. We will need to run a follow up merge process that runs
      // on a single machine in the OutputCommitter function.
      outputValue.set(random.nextInt(numReducers));
      outputValue.set(0);
      context.write(outputValue, canvasLayer);
    }
  }
  
  public static class NoPartitionPlotCombine<S extends Shape>
  extends Reducer<IntWritable, Canvas, IntWritable, Canvas> {
    
    /**The MBR of the input file*/
    private Rectangle inputMBR;
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;
    /**The component that plots the shapes*/
    private Plotter plotter;
    /**Number of reduce jobs*/
    private int numReducers;
    /**Random number generator to send the canvas to a random reducer*/
    private Random random;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.imageWidth = conf.getInt("width", 1000);
      this.imageHeight = conf.getInt("height", 1000);
      this.inputMBR = (Rectangle) OperationsParams.getShape(conf, InputMBR);
      this.plotter = Plotter.getPlotter(conf);
      this.numReducers = Math.max(1, context.getNumReduceTasks());
      this.random = new Random();
    }
    
    @Override
    protected void reduce(IntWritable key,
        Iterable<Canvas> intermediateLayers, Context context)
            throws IOException, InterruptedException {
      Iterator<Canvas> iLayers = intermediateLayers.iterator();
      if (iLayers.hasNext()) {
        Canvas layer = iLayers.next();
        if (!iLayers.hasNext()) {
          // Only one layer in the input. Output it as-is
          key.set(random.nextInt(numReducers));
          context.write(key, layer);
        } else {
          Canvas finalLayer = plotter.createCanvas(imageWidth, imageHeight, inputMBR);
          plotter.merge(finalLayer, layer);
          while (iLayers.hasNext()) {
            layer = iLayers.next();
            plotter.merge(finalLayer, layer);
          }
          key.set(random.nextInt(numReducers));
          context.write(key, finalLayer);
        }
      }
    }
  }
  
  public static class NoPartitionPlotReduce<S extends Shape>
    extends Reducer<IntWritable, Canvas, NullWritable, Canvas> {

    /**The MBR of the input file*/
    private Rectangle inputMBR;
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;
    /**The component that plots the shapes*/
    private Plotter plotter;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.imageWidth = conf.getInt("width", 1000);
      this.imageHeight = conf.getInt("height", 1000);
      this.inputMBR = (Rectangle) OperationsParams.getShape(conf, InputMBR);
      this.plotter = Plotter.getPlotter(conf);
    }
    
    @Override
    protected void reduce(IntWritable dummy,
        Iterable<Canvas> intermediateLayers, Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      Canvas finalLayer = plotter.createCanvas(imageWidth, imageHeight, inputMBR);
      
      for (Canvas intermediateLayer : intermediateLayers) {
        plotter.merge(finalLayer, intermediateLayer);
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
        if (((++i) & 0xff) == 0)
          context.progress();
      }
    
    }
  }
  
  public static class RepartitionPlotReduce
      extends Reducer<IntWritable, Shape, NullWritable, Canvas> {
    
    /**The partitioner used to partitioner the data across reducers*/
    private Partitioner partitioner;
    
    /**The component that plots the shapes*/
    private Plotter plotter;

    /**MBR of the input file*/
    private Rectangle inputMBR;
    
    /**Generated image width in pixels*/
    private int imageWidth;
    /**Generated image height in pixels*/
    private int imageHeight;

    /**Whether the configured plotter defines a smooth function or not*/
    private boolean smooth;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.partitioner = Partitioner.getPartitioner(conf);
      this.plotter = Plotter.getPlotter(conf);
      this.smooth = plotter.isSmooth();
      this.inputMBR = (Rectangle) OperationsParams.getShape(conf, InputMBR);
      this.imageWidth = conf.getInt("width", 1000);
      this.imageHeight = conf.getInt("height", 1000);
    }
    
    @Override
    protected void reduce(IntWritable partitionID, Iterable<Shape> shapes,
        Context context) throws IOException, InterruptedException {
      CellInfo partition = partitioner.getPartition(partitionID.get());
      int canvasX1 = (int) Math.floor((partition.x1 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int canvasX2 = (int) Math.ceil((partition.x2 - inputMBR.x1) * imageWidth / inputMBR.getWidth());
      int canvasY1 = (int) Math.floor((partition.y1 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      int canvasY2 = (int) Math.ceil((partition.y2 - inputMBR.y1) * imageHeight / inputMBR.getHeight());
      Canvas canvasLayer = plotter.createCanvas(canvasX2 - canvasX1, canvasY2 - canvasY1, partition);
      if (smooth) {
        shapes = plotter.smooth(shapes);
        context.progress();
      }

      int i = 0;
      for (Shape shape : shapes) {
        plotter.plot(canvasLayer, shape);
        if (((++i) & 0xff) == 0)
          context.progress();
      }
      
      context.write(NullWritable.get(), canvasLayer);
    }
  }
  
  /**
   * Generates a single level using a MapReduce job and returns the created job.
   * @param inFiles
   * @param outFile
   * @param plotterClass
   * @param params
   * @return
   * @throws IOException
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static Job plotMapReduce(Path[] inFiles, Path outFile,
      Class<? extends Plotter> plotterClass, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    Plotter plotter;
    try {
      plotter = plotterClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating rastierizer", e);
    }
    
    Job job = new Job(params, "SingleLevelPlot");
    job.setJarByClass(SingleLevelPlot.class);
    job.setJobName("SingleLevelPlot");
    // Set plotter
    Configuration conf = job.getConfiguration();
    Plotter.setPlotter(conf, plotterClass);
    // Set input file MBR
    Rectangle inputMBR = (Rectangle) params.getShape("mbr");
    Rectangle drawRect = (Rectangle) params.getShape("rect");
    if (inputMBR == null)
      inputMBR = drawRect != null? drawRect : FileMBR.fileMBR(inFiles, params);
    OperationsParams.setShape(conf, InputMBR, inputMBR);
    if (drawRect != null)
      OperationsParams.setShape(conf, SpatialInputFormat3.InputQueryRange, drawRect);
    
    // Adjust width and height if aspect ratio is to be kept
    int imageWidth = conf.getInt("width", 1000);
    int imageHeight = conf.getInt("height", 1000);
    if (params.getBoolean("keepratio", true)) {
      // Adjust width and height to maintain aspect ratio
      if (inputMBR.getWidth() / inputMBR.getHeight() > (double) imageWidth / imageHeight) {
        // Fix width and change height
        imageHeight = (int) (inputMBR.getHeight() * imageWidth / inputMBR.getWidth());
        // Make divisible by two for compatibility with ffmpeg
        if (imageHeight % 2 == 1)
          imageHeight--;
        conf.setInt("height", imageHeight);
      } else {
        imageWidth = (int) (inputMBR.getWidth() * imageHeight / inputMBR.getHeight());
        conf.setInt("width", imageWidth);
      }
    }
    
    boolean merge = conf.getBoolean("merge", true);
    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inFiles);
    if (conf.getBoolean("output", true)) {
      if (merge) {
        job.setOutputFormatClass(CanvasOutputFormat.class);
        conf.setClass("mapred.output.committer.class",
            CanvasOutputFormat.ImageWriterOld.class,
            org.apache.hadoop.mapred.OutputCommitter.class);
      } else {
        job.setOutputFormatClass(ImageOutputFormat.class);
      }
      CanvasOutputFormat.setOutputPath(job, outFile);
    } else {
      job.setOutputFormatClass(NullOutputFormat.class);
    }
    
    // Set mapper and reducer based on the partitioning scheme
    String partition = conf.get("partition", "none");
    ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
    if (partition.equalsIgnoreCase("none")) {
      LOG.info("Using no-partition plot");
      job.setMapperClass(NoPartitionPlotMap.class);
      job.setCombinerClass(NoPartitionPlotCombine.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(plotter.getCanvasClass());
      if (merge) {
        int numSplits = new SpatialInputFormat3().getSplits(job).size();
        job.setReducerClass(NoPartitionPlotReduce.class);
        // Set number of reduce tasks according to cluster status
        int maxReduce = Math.max(1, clusterStatus.getMaxReduceTasks() * 7 / 8);
        job.setNumReduceTasks(Math.max(1, Math.min(maxReduce, numSplits / maxReduce)));
      } else {
        job.setNumReduceTasks(0);
      }
    } else {
      LOG.info("Using repartition plot");
      Partitioner partitioner;
      if (partition.equals("pixel")) {
        // Special case for pixel level partitioning as it depends on the
        // visualization parameters
        partitioner = new GridPartitioner(inputMBR, imageWidth, imageHeight);
      } else if (partition.equals("grid")) {
        int numBlocks = 0;
        for (Path in : inFiles) {
          FileSystem fs = in.getFileSystem(params);
          long size = FileUtil.getPathSize(fs, in);
          long blockSize = fs.getDefaultBlockSize(in);
          numBlocks += Math.ceil(size / (double) blockSize);
        }
        int numPartitions = numBlocks * 1000;
        int gridSize = (int) Math.ceil(Math.sqrt(numPartitions));
        partitioner = new GridPartitioner(inputMBR, gridSize, gridSize);
      } else {
        // Use a standard partitioner as created by the indexer
        partitioner = Indexer.createPartitioner(inFiles, outFile, conf, partition);
      }
      Shape shape = params.getShape("shape");
      job.setMapperClass(RepartitionPlotMap.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(shape.getClass());
      job.setReducerClass(RepartitionPlotReduce.class);
      // Set number of reducers according to cluster size
      job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks() * 9 / 10));        
      Partitioner.setPartitioner(conf, partitioner);
    }
    
    // Use multithreading in case the job is running locally
    conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    
    // Start the job
    if (params.getBoolean("background", false)) {
      // Run in background
      job.submit();
    } else {
      job.waitForCompletion(params.getBoolean("verbose", false));
    }
    return job;
  }

  public static void plotLocal(Path[] inFiles, Path outFile,
      final Class<? extends Plotter> plotterClass,
      final OperationsParams params) throws IOException, InterruptedException {
    OperationsParams mbrParams = new OperationsParams(params);
    mbrParams.setBoolean("background", false);
    final Rectangle inputMBR = params.get(InputMBR) != null ?
        params.getShape("mbr").getMBR() : FileMBR.fileMBR(inFiles, mbrParams);
    if (params.get(InputMBR) == null)
      OperationsParams.setShape(params, InputMBR, inputMBR);

    // Retrieve desired output image size and keep aspect ratio if needed
    int width = params.getInt("width", 1000);
    int height = params.getInt("height", 1000);
    if (params.getBoolean("keepratio", true)) {
      // Adjust width and height to maintain aspect ratio and store the adjusted
      // values back in params in case the caller needs to retrieve them
      if (inputMBR.getWidth() / inputMBR.getHeight() > (double) width / height)
        params.setInt("height", height = (int) (inputMBR.getHeight() * width / inputMBR.getWidth()));
      else
        params.setInt("width", width = (int) (inputMBR.getWidth() * height / inputMBR.getHeight()));
    }
    // Store width and height in final variables to make them accessible in parallel
    final int fwidth = width, fheight = height;

    // Start reading input file
    List<InputSplit> splits = new ArrayList<InputSplit>();
    final SpatialInputFormat3<Rectangle, Shape> inputFormat =
        new SpatialInputFormat3<Rectangle, Shape>();
    for (Path inFile : inFiles) {
      FileSystem inFs = inFile.getFileSystem(params);
      if (!OperationsParams.isWildcard(inFile) && inFs.exists(inFile) && !inFs.isDirectory(inFile)) {
        if (SpatialSite.NonHiddenFileFilter.accept(inFile)) {
          // Use the normal input format splitter to add this non-hidden file
          Job job = Job.getInstance(params);
          SpatialInputFormat3.addInputPath(job, inFile);
          splits.addAll(inputFormat.getSplits(job));
        } else {
          // A hidden file, add it immediately as one split
          // This is useful if the input is a hidden file which is automatically
          // skipped by FileInputFormat. We need to plot a hidden file for the case
          // of plotting partition boundaries of a spatial index
          splits.add(new FileSplit(inFile, 0,
              inFs.getFileStatus(inFile).getLen(), new String[0]));
        }
      } else {
        // Use the normal input format splitter to add this non-hidden file
        Job job = Job.getInstance(params);
        SpatialInputFormat3.addInputPath(job, inFile);
        splits.addAll(inputFormat.getSplits(job));
      }
    }
    
    // Copy splits to a final array to be used in parallel
    final FileSplit[] fsplits = splits.toArray(new FileSplit[splits.size()]);
    int parallelism = params.getInt("parallel",
        Runtime.getRuntime().availableProcessors());
    List<Canvas> partialCanvases = Parallel.forEach(fsplits.length, new RunnableRange<Canvas>() {
      @Override
      public Canvas run(int i1, int i2) {
        Plotter plotter;
        try {
          plotter = plotterClass.newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException("Error creating rastierizer", e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Error creating rastierizer", e);
        }
        plotter.configure(params);
        // Create the partial layer that will contain the plot of the assigned partitions
        Canvas partialCanvas = plotter.createCanvas(fwidth, fheight, inputMBR);
        
        for (int i = i1; i < i2; i++) {
          try {
            RecordReader<Rectangle, Iterable<Shape>> reader =
                inputFormat.createRecordReader(fsplits[i], null);
            if (reader instanceof SpatialRecordReader3) {
              ((SpatialRecordReader3)reader).initialize(fsplits[i], params);
            } else if (reader instanceof RTreeRecordReader3) {
              ((RTreeRecordReader3)reader).initialize(fsplits[i], params);
            } else if (reader instanceof HDFRecordReader) {
              ((HDFRecordReader)reader).initialize(fsplits[i], params);
            } else {
              throw new RuntimeException("Unknown record reader");
            }

            while (reader.nextKeyValue()) {
              Rectangle partition = reader.getCurrentKey();
              if (!partition.isValid())
                partition.set(inputMBR);

              Iterable<Shape> shapes = reader.getCurrentValue();
              // Run the plot step
              plotter.plot(partialCanvas,
                  plotter.isSmooth() ? plotter.smooth(shapes) : shapes);
            }
            reader.close();
          } catch (IOException e) {
            throw new RuntimeException("Error reading the file ", e);
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupt error ", e);
          }
        }
        return partialCanvas;
      }
    }, parallelism);
    boolean merge = params.getBoolean("merge", true);
    Plotter plotter;
    try {
      plotter = plotterClass.newInstance();
      plotter.configure(params);
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating plotter", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating plotter", e);
    }
    
    // Whether we should vertically flip the final image or not
    boolean vflip = params.getBoolean("vflip", true);
    if (merge) {
      LOG.info("Merging "+partialCanvases.size()+" partial canvases");
      // Create the final canvas that will contain the final image
      Canvas finalCanvas = plotter.createCanvas(fwidth, fheight, inputMBR);
      for (Canvas partialCanvas : partialCanvases)
        plotter.merge(finalCanvas, partialCanvas);
      
      // Finally, write the resulting image to the given output path
      LOG.info("Writing final image");
      FileSystem outFs = outFile.getFileSystem(params);
      FSDataOutputStream outputFile = outFs.create(outFile);
      
      plotter.writeImage(finalCanvas, outputFile, vflip);
      outputFile.close();
    } else {
      // No merge
      LOG.info("Writing partial images");
      FileSystem outFs = outFile.getFileSystem(params);
      for (int i = 0; i < partialCanvases.size(); i++) {
        Path filename = new Path(outFile, String.format("part-%05d.png", i));
        FSDataOutputStream outputFile = outFs.create(filename);
        
        plotter.writeImage(partialCanvases.get(i), outputFile, vflip);
        outputFile.close();
      }
    }
  }
  
  /**
   * Plots the given file using the provided plotter
   * @param inFiles
   * @param outFile
   * @param plotterClass
   * @param params
   * @return
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public static Job plot(Path[] inFiles, Path outFile,
      final Class<? extends Plotter> plotterClass,
      final OperationsParams params)
          throws IOException, InterruptedException, ClassNotFoundException {
    if (OperationsParams.isLocal(params, inFiles)) {
      plotLocal(inFiles, outFile, plotterClass, params);
      return null;
    } else {
      return plotMapReduce(inFiles, outFile, plotterClass, params);
    }
  }
}
