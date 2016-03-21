/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.IndexOutputFormat.IndexRecordWriter;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * @author Ahmed Eldawy
 *
 */
public class Indexer {
  private static final Log LOG = LogFactory.getLog(Indexer.class);
  
  private static final Map<String, Class<? extends Partitioner>> PartitionerClasses;
  private static final Map<String, Class<? extends LocalIndexer>> LocalIndexes;
  private static final Map<String, Boolean> PartitionerReplicate;
  
  static {
    PartitionerClasses = new HashMap<String, Class<? extends Partitioner>>();
    PartitionerClasses.put("grid", GridPartitioner.class);
    PartitionerClasses.put("str", STRPartitioner.class);
    PartitionerClasses.put("str+", STRPartitioner.class);
    PartitionerClasses.put("rtree", STRPartitioner.class);
    PartitionerClasses.put("r+tree", STRPartitioner.class);
    PartitionerClasses.put("quadtree", QuadTreePartitioner.class);
    PartitionerClasses.put("zcurve", ZCurvePartitioner.class);
    PartitionerClasses.put("hilbert", HilbertCurvePartitioner.class);
    PartitionerClasses.put("kdtree", KdTreePartitioner.class);
    
    PartitionerReplicate = new HashMap<String, Boolean>();
    PartitionerReplicate.put("grid", true);
    PartitionerReplicate.put("str", false);
    PartitionerReplicate.put("str+", true);
    PartitionerReplicate.put("rtree", false);
    PartitionerReplicate.put("r+tree", true);
    PartitionerReplicate.put("quadtree", true);
    PartitionerReplicate.put("zcurve", false);
    PartitionerReplicate.put("hilbert", false);
    PartitionerReplicate.put("kdtree", true);
    
    LocalIndexes = new HashMap<String, Class<? extends LocalIndexer>>();
    LocalIndexes.put("rtree", RTreeLocalIndexer.class);
    LocalIndexes.put("r+tree", RTreeLocalIndexer.class);
  }


  /**
   * The map function that partitions the data using the configured partitioner
   * @author Eldawy
   *
   */
  public static class PartitionerMap extends
    Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {

    /**The partitioner used to partitioner the data across reducers*/
    private Partitioner partitioner;
    /**
     * Whether to replicate a record to all overlapping partitions or to assign
     * it to only one partition
     */
    private boolean replicate;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      this.partitioner = Partitioner.getPartitioner(context.getConfiguration());
      this.replicate = context.getConfiguration().getBoolean("replicate", false);
    }
    
    @Override
    protected void map(Rectangle key, Iterable<? extends Shape> shapes,
        final Context context) throws IOException,
        InterruptedException {
      final IntWritable partitionID = new IntWritable();
      for (final Shape shape : shapes) {
        if (replicate) {
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
        } else {
          partitionID.set(partitioner.overlapPartition(shape));
          if (partitionID.get() >= 0)
            context.write(partitionID, shape);
        }
        context.progress();
      }
    }
  }

  
  public static class PartitionerReduce<S extends Shape>
    extends Reducer<IntWritable, Shape, IntWritable, Shape> {

    @Override
    protected void reduce(IntWritable partitionID, Iterable<Shape> shapes,
        Context context) throws IOException, InterruptedException {
      LOG.info("Working on partition #"+partitionID);
      for (Shape shape : shapes) {
        context.write(partitionID, shape);
        context.progress();
      }
      // Indicate end of partition to close the file
      context.write(new IntWritable(-partitionID.get()-1), null);
      LOG.info("Done with partition #"+partitionID);
    }
  }
    
  private static Job indexMapReduce(Path inPath, Path outPath,
      OperationsParams paramss) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = new Job(paramss, "Indexer");
    Configuration conf = job.getConfiguration();
    job.setJarByClass(Indexer.class);
    
    // Set input file MBR if not already set
    Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
    if (inputMBR == null) {
      inputMBR = FileMBR.fileMBR(inPath, new OperationsParams(conf));
      OperationsParams.setShape(conf, "mbr", inputMBR);
    }
    
    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inPath);
    job.setOutputFormatClass(IndexOutputFormat.class);
    IndexOutputFormat.setOutputPath(job, outPath);

    // Set the correct partitioner according to index type
    String index = conf.get("sindex");
    if (index == null)
      throw new RuntimeException("Index type is not set");
    long t1 = System.currentTimeMillis();
    setLocalIndexer(conf, index);
    Partitioner partitioner = createPartitioner(inPath, outPath, conf, index);
    Partitioner.setPartitioner(conf, partitioner);
    
    long t2 = System.currentTimeMillis();
    System.out.println("Total time for space subdivision in millis: "+(t2-t1));
    
    // Set mapper and reducer
    Shape shape = OperationsParams.getShape(conf, "shape");
    job.setMapperClass(PartitionerMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setReducerClass(PartitionerReduce.class);
    // Set number of reduce tasks according to cluster status
    ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
    job.setNumReduceTasks(Math.max(1, Math.min(partitioner.getPartitionCount(),
        (clusterStatus.getMaxReduceTasks() * 9) / 10)));

    // Use multithreading in case the job is running locally
    conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    
    // Start the job
    if (conf.getBoolean("background", false)) {
      // Run in background
      job.submit();
    } else {
      job.waitForCompletion(conf.getBoolean("verbose", false));
    }
    return job;
  }

  /**
   * Set the local indexer for the given job configuration.
   * @param job
   * @param sindex
   */
  private static void setLocalIndexer(Configuration conf, String sindex) {
    Class<? extends LocalIndexer> localIndexerClass = LocalIndexes.get(sindex);
    if (localIndexerClass != null)
      conf.setClass(LocalIndexer.LocalIndexerClass, localIndexerClass, LocalIndexer.class);
  }

  public static Partitioner createPartitioner(Path in, Path out,
      Configuration job, String partitionerName) throws IOException {
    return createPartitioner(new Path[] {in}, out, job, partitionerName);
  }

  /**
   * Create a partitioner for a particular job
   * @param ins
   * @param out
   * @param job
   * @param partitionerName
   * @return
   * @throws IOException
   */
  public static Partitioner createPartitioner(Path[] ins, Path out,
      Configuration job, String partitionerName) throws IOException {
    try {
      Partitioner partitioner;
      Class<? extends Partitioner> partitionerClass =
          PartitionerClasses.get(partitionerName.toLowerCase());
      if (partitionerClass == null) {
        // Try to parse the name as a class name
        try {
          partitionerClass = Class.forName(partitionerName).asSubclass(Partitioner.class);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Unknown index type '"+partitionerName+"'");
        }
      }
      
      if (PartitionerReplicate.containsKey(partitionerName.toLowerCase())) {
        boolean replicate = PartitionerReplicate.get(partitionerName.toLowerCase());
        job.setBoolean("replicate", replicate);
      }
      partitioner = partitionerClass.newInstance();
      
      long t1 = System.currentTimeMillis();
      final Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
      // Determine number of partitions
      long inSize = 0;
      for (Path in : ins) {
        inSize += FileUtil.getPathSize(in.getFileSystem(job), in);
      }
      long estimatedOutSize = (long) (inSize * (1.0 + job.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f)));
      FileSystem outFS = out.getFileSystem(job);
      long outBlockSize = outFS.getDefaultBlockSize(out);

      final List<Point> sample = new ArrayList<Point>();
      float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
      long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);

      LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
      ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
        @Override
        public void collect(Point p) {
          sample.add(p.clone());
        }
      };
      OperationsParams params2 = new OperationsParams();
      params2.setFloat("ratio", sample_ratio);
      params2.setLong("size", sample_size);
      if (job.get("shape") != null)
      params2.set("shape", job.get("shape"));
      if (job.get("local") != null)
      params2.set("local", job.get("local"));
      params2.setClass("outshape", Point.class, Shape.class);
      Sampler.sample(ins, resultCollector, params2);
      long t2 = System.currentTimeMillis();
      System.out.println("Total time for sampling in millis: "+(t2-t1));
      LOG.info("Finished reading a sample of "+sample.size()+" records");
      
      int partitionCapacity = (int) Math.max(1, Math.floor((double)sample.size() * outBlockSize / estimatedOutSize));
      int numPartitions = Math.max(1, (int) Math.ceil((float)estimatedOutSize / outBlockSize));
      LOG.info("Partitioning the space into "+numPartitions+" partitions with capacity of "+partitionCapacity);

      partitioner.createFromPoints(inMBR, sample.toArray(new Point[sample.size()]), partitionCapacity);
      
      return partitioner;
    } catch (InstantiationException e) {
      e.printStackTrace();
      return null;
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static void indexLocal(Path inPath, final Path outPath,
      OperationsParams params) throws IOException, InterruptedException {
    Job job = Job.getInstance(params);
    final Configuration conf = job.getConfiguration();
    
    final String sindex = conf.get("sindex");
    
    // Start reading input file
    List<InputSplit> splits = new ArrayList<InputSplit>();
    final SpatialInputFormat3<Rectangle, Shape> inputFormat = new SpatialInputFormat3<Rectangle, Shape>();
    FileSystem inFs = inPath.getFileSystem(conf);
    FileStatus inFStatus = inFs.getFileStatus(inPath);
    if (inFStatus != null && !inFStatus.isDir()) {
      // One file, retrieve it immediately.
      // This is useful if the input is a hidden file which is automatically
      // skipped by FileInputFormat. We need to plot a hidden file for the case
      // of plotting partition boundaries of a spatial index
      splits.add(new FileSplit(inPath, 0, inFStatus.getLen(), new String[0]));
    } else {
      SpatialInputFormat3.setInputPaths(job, inPath);
      for (InputSplit s : inputFormat.getSplits(job))
        splits.add(s);
    }
    
    // Copy splits to a final array to be used in parallel
    final FileSplit[] fsplits = splits.toArray(new FileSplit[splits.size()]);
    boolean replicate = PartitionerReplicate.get(sindex);
    
    // Set input file MBR if not already set
    Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
    if (inputMBR == null) {
      inputMBR = FileMBR.fileMBR(inPath, new OperationsParams(conf));
      OperationsParams.setShape(conf, "mbr", inputMBR);
    }
    
    setLocalIndexer(conf, sindex);
    final Partitioner partitioner = createPartitioner(inPath, outPath, conf, sindex);

    final IndexRecordWriter<Shape> recordWriter = new IndexRecordWriter<Shape>(
        partitioner, replicate, sindex, outPath, conf);
    for (FileSplit fsplit : fsplits) {
      RecordReader<Rectangle, Iterable<Shape>> reader = inputFormat.createRecordReader(fsplit, null);
      if (reader instanceof SpatialRecordReader3) {
        ((SpatialRecordReader3)reader).initialize(fsplit, conf);
      } else if (reader instanceof RTreeRecordReader3) {
        ((RTreeRecordReader3)reader).initialize(fsplit, conf);
      } else if (reader instanceof HDFRecordReader) {
        ((HDFRecordReader)reader).initialize(fsplit, conf);
      } else {
        throw new RuntimeException("Unknown record reader");
      }

      final IntWritable partitionID = new IntWritable();

      while (reader.nextKeyValue()) {
        Iterable<Shape> shapes = reader.getCurrentValue();
        if (replicate) {
          for (final Shape s : shapes) {
            partitioner.overlapPartitions(s, new ResultCollector<Integer>() {
              @Override
              public void collect(Integer id) {
                partitionID.set(id);
                try {
                  recordWriter.write(partitionID, s);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            });
          }
        } else {
          for (final Shape s : shapes) {
            int pid = partitioner.overlapPartition(s);
            if (pid != -1) {
              partitionID.set(pid);
              recordWriter.write(partitionID, s);
            }
          }
        }
      }
      reader.close();
    }
    recordWriter.close(null);
    
    // Write the WKT formatted master file
    Path masterPath = new Path(outPath, "_master." + sindex);
    FileSystem outFs = outPath.getFileSystem(params);
    Path wktPath = new Path(outPath, "_"+sindex+".wkt");
    PrintStream wktOut = new PrintStream(outFs.create(wktPath));
    wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
    Text tempLine = new Text2();
    Partition tempPartition = new Partition();
    LineReader in = new LineReader(outFs.open(masterPath));
    while (in.readLine(tempLine) > 0) {
      tempPartition.fromText(tempLine);
      wktOut.println(tempPartition.toWKT());
    }
    in.close();
    wktOut.close();
  }
  
  public static Job index(Path inPath, Path outPath, OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    if (OperationsParams.isLocal(new JobConf(params), inPath)) {
      indexLocal(inPath, outPath, params);
      return null;
    } else {
      return indexMapReduce(inPath, outPath, params);
    }
  }

  protected static void printUsage() {
    System.out.println("Builds a spatial index on an input file");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon> - (*) Type of shapes stored in input file");
    System.out.println("sindex:<index> - (*) Type of spatial index (grid|str|str+|quadtree|zcurve|kdtree)");
    System.out.println("-overwrite - Overwrite output file without noitce");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * Entry point to the indexing operation.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    
    if (!params.checkInputOutput(true)) {
      printUsage();
      return;
    }
    if (params.get("sindex") == null) {
      System.err.println("Please specify type of index to build (grid, rtree, r+tree, str, str+)");
      printUsage();
      return;
    }
    Path inputPath = params.getInputPath();
    Path outputPath = params.getOutputPath();

    // The spatial index to use
    long t1 = System.currentTimeMillis();
    index(inputPath, outputPath, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total indexing time in millis "+(t2-t1));
  }

}
