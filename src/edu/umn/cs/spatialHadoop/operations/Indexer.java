/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GridPartitioner;
import edu.umn.cs.spatialHadoop.core.HilbertCurvePartitioner;
import edu.umn.cs.spatialHadoop.core.KdTreePartitioner;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Partitioner;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.QuadTreePartitioner;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.STRPartitioner;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.core.ZCurvePartitioner;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.IndexOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.IndexOutputFormat.IndexRecordWriter;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * @author Ahmed Eldawy
 *
 */
public class Indexer {
  private static final Log LOG = LogFactory.getLog(Indexer.class);
  
  private static final Map<String, Class<? extends Partitioner>> PartitionerClasses;
  private static final Map<String, Boolean> PartitionerReplicate;
  
  static {
    PartitionerClasses = new HashMap<String, Class<? extends Partitioner>>();
    PartitionerClasses.put("grid", GridPartitioner.class);
    PartitionerClasses.put("str", STRPartitioner.class);
    PartitionerClasses.put("str+", STRPartitioner.class);
    PartitionerClasses.put("quadtree", QuadTreePartitioner.class);
    PartitionerClasses.put("zcurve", ZCurvePartitioner.class);
    PartitionerClasses.put("hilbert", HilbertCurvePartitioner.class);
    PartitionerClasses.put("kdtree", KdTreePartitioner.class);
    
    PartitionerReplicate = new HashMap<String, Boolean>();
    PartitionerReplicate.put("grid", true);
    PartitionerReplicate.put("str", false);
    PartitionerReplicate.put("str+", true);
    PartitionerReplicate.put("quadtree", true);
    PartitionerReplicate.put("zcurve", false);
    PartitionerReplicate.put("hilbert", false);
    PartitionerReplicate.put("kdtree", true);
  }
  
  /**
   * The map and reduce functions for the repartition
   * @author Ahmed Eldawy
   *
   */
  public static class IndexMethods extends MapReduceBase 
    implements Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape>,
    Reducer<IntWritable, Shape, IntWritable, Shape> {

    /**The partitioner used to partitioner the data across reducers*/
    private Partitioner partitioner;
    /**
     * Whether to replicate a record to all overlapping partitions or to assign
     * it to only one partition
     */
    private boolean replicate;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      this.partitioner = Partitioner.getPartitioner(job);
      this.replicate = job.getBoolean("replicate", false);
    }
    
    @Override
    public void map(Rectangle dummy, Iterable<? extends Shape> shapes,
        final OutputCollector<IntWritable, Shape> output, Reporter reporter)
        throws IOException {
      final IntWritable partitionID = new IntWritable();
      int i = 0;
      for (final Shape shape : shapes) {
        if (replicate) {
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
        } else {
          partitionID.set(partitioner.overlapPartition(shape));
          if (partitionID.get() >= 0)
            output.collect(partitionID, shape);
        }
        if (((++i) & 0xff) == 0) {
          reporter.progress();
        }
      }
    }

    @Override
    public void reduce(IntWritable partitionID, Iterator<Shape> shapes,
        OutputCollector<IntWritable, Shape> output, Reporter reporter)
        throws IOException {
      while (shapes.hasNext()) {
        output.collect(partitionID, shapes.next());
      }
      // Indicate end of partition to close the file
      partitionID.set(-(partitionID.get()+1));
      output.collect(partitionID, null);
    }
  }
  
  /**
   * Output committer that concatenates all master files into one master file.
   * @author Ahmed Eldawy
   *
   */
  public static class IndexerOutputCommitter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      JobConf job = context.getJobConf();
      Path outPath = GridOutputFormat.getOutputPath(job);
      FileSystem outFs = outPath.getFileSystem(job);

      // Concatenate all master files into one file
      FileStatus[] resultFiles = outFs.listStatus(outPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().contains("_master");
        }
      });
      
      if (resultFiles.length == 0) {
        LOG.warn("No _master files were written by reducers");
      } else {
        String sindex = job.get("sindex");
        Path masterPath = new Path(outPath, "_master." + sindex);
        OutputStream destOut = outFs.create(masterPath);
        Path wktPath = new Path(outPath, "_"+sindex+".wkt");
        PrintStream wktOut = new PrintStream(outFs.create(wktPath));
        wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
        Text tempLine = new Text2();
        Partition tempPartition = new Partition();
        final byte[] NewLine = new byte[] {'\n'};
        for (FileStatus f : resultFiles) {
          LineReader in = new LineReader(outFs.open(f.getPath()));
          while (in.readLine(tempLine) > 0) {
            destOut.write(tempLine.getBytes(), 0, tempLine.getLength());
            destOut.write(NewLine);
            tempPartition.fromText(tempLine);
            wktOut.println(tempPartition.toWKT());
          }
          in.close();
          outFs.delete(f.getPath(), false); // Delete the copied file
        }
        destOut.close();
      }
    }
  }
  
  private static RunningJob indexMapReduce(Path inPath, Path outPath,
      OperationsParams params) throws IOException, InterruptedException {
    JobConf job = new JobConf(params, Indexer.class);
    job.setJobName("Indexer");
    
    // Set input file MBR if not already set
    Rectangle inputMBR = (Rectangle) params.getShape("mbr");
    if (inputMBR == null)
      inputMBR = FileMBR.fileMBR(inPath, params);
    OperationsParams.setShape(job, "mbr", inputMBR);
    
    // Set input and output
    job.setInputFormat(ShapeIterInputFormat.class);
    ShapeIterInputFormat.setInputPaths(job, inPath);
    job.setOutputFormat(IndexOutputFormat.class);
    GridOutputFormat.setOutputPath(job, outPath);

    // Set the correct partitioner according to index type
    String index = job.get("sindex");
    if (index == null)
      throw new RuntimeException("Index type is not set");
    long t1 = System.currentTimeMillis();
    Partitioner partitioner = createPartitioner(inPath, outPath, job, index);
    Partitioner.setPartitioner(job, partitioner);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time for space subdivision in millis: "+(t2-t1));
    
    // Set mapper and reducer
    Shape shape = params.getShape("shape");
    job.setMapperClass(IndexMethods.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setReducerClass(IndexMethods.class);
    job.setOutputCommitter(IndexerOutputCommitter.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

    // Use multithreading in case the job is running locally
    job.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
    
    // Start the job
    if (params.getBoolean("background", false)) {
      // Run in background
      JobClient jc = new JobClient(job);
      return jc.submitJob(job);
    } else {
      // Run and block until it is finished
      return JobClient.runJob(job);
    }
  }

  public static Partitioner createPartitioner(Path in, Path out,
      Configuration job, String partitionerName) throws IOException {
    return createPartitioner(new Path[] {in}, out, job, partitionerName);
  }

  /***
   * Create a partitioner for a particular job
   * @param in
   * @param out
   * @param job
   * @param partitionerName
   * @return
   * @throws IOException
   */
  public static Partitioner createPartitioner(Path[] ins, Path out,
      Configuration job, String partitionerName) throws IOException {
    try {
      Partitioner partitioner = null;
      Class<? extends Partitioner> partitionerClass =
          PartitionerClasses.get(partitionerName.toLowerCase());
      if (partitionerClass == null) {
        throw new RuntimeException("Unknown index type '"+partitionerName+"'");
      }
      boolean replicate = PartitionerReplicate.get(partitionerName.toLowerCase());
      job.setBoolean("replicate", replicate);
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
      int numPartitions = Math.max(1, (int) Math.ceil((float)estimatedOutSize / outBlockSize));
      LOG.info("Partitioning the space into "+numPartitions+" partitions");

      final Vector<Point> sample = new Vector<Point>();
      float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
      long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);

      LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
      ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
        @Override
        public void collect(Point p) {
          sample.add(p.clone());
        }
      };
      OperationsParams params2 = new OperationsParams(job);
      params2.setFloat("ratio", sample_ratio);
      params2.setLong("size", sample_size);
      params2.setClass("outshape", Point.class, Shape.class);
      Sampler.sample(ins, resultCollector, params2);
      long t2 = System.currentTimeMillis();
      System.out.println("Total time for sampling in millis: "+(t2-t1));
      LOG.info("Finished reading a sample of "+sample.size()+" records");
      
      partitioner.createFromPoints(inMBR, sample.toArray(new Point[sample.size()]), numPartitions);
      
      return partitioner;
    } catch (InstantiationException e) {
      e.printStackTrace();
      return null;
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static void indexLocal(Path inPath, Path outPath,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params);
    String sindex = params.get("sindex");
    Partitioner partitioner = createPartitioner(inPath, outPath, job, sindex);
    
    // Start reading input file
    Vector<InputSplit> splits = new Vector<InputSplit>();
    final ShapeIterInputFormat inputFormat = new ShapeIterInputFormat();
    FileSystem inFs = inPath.getFileSystem(params);
    FileStatus inFStatus = inFs.getFileStatus(inPath);
    if (inFStatus != null && !inFStatus.isDir()) {
      // One file, retrieve it immediately.
      // This is useful if the input is a hidden file which is automatically
      // skipped by FileInputFormat. We need to plot a hidden file for the case
      // of plotting partition boundaries of a spatial index
      splits.add(new FileSplit(inPath, 0, inFStatus.getLen(), new String[0]));
    } else {
      ShapeIterInputFormat.addInputPath(job, inPath);
      for (InputSplit s : inputFormat.getSplits(job, 1))
        splits.add(s);
    }
    
    // Copy splits to a final array to be used in parallel
    final FileSplit[] fsplits = splits.toArray(new FileSplit[splits.size()]);
    boolean replicate = job.getBoolean("replicate", false);
    
    final IndexRecordWriter<Shape> recordWriter = new IndexRecordWriter<Shape>(
        partitioner, replicate, sindex, outPath, params);
    
    for (FileSplit fsplit : fsplits) {
      RecordReader<Rectangle, Iterable<? extends Shape>> reader =
          inputFormat.getRecordReader(fsplit, job, null);
      Rectangle partitionMBR = reader.createKey();
      Iterable<? extends Shape> shapes = reader.createValue();
      
      final IntWritable partitionID = new IntWritable();
      
      while (reader.next(partitionMBR, shapes)) {
        if (replicate) {
          // Replicate each shape to all overlapping partitions
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
          for (Shape s : shapes) {
            partitionID.set(partitioner.overlapPartition(s));
            recordWriter.write(partitionID, s);
          }
        }
      }
      reader.close();
    }
    
    recordWriter.close(null);
  }
  
  public static RunningJob index(Path inPath, Path outPath,
      OperationsParams params) throws IOException, InterruptedException {
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
