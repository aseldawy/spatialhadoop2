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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.RTreeGridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.ShapeRecordWriter;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.indexing.RTree;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RTreeGridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeRecordReader;

/**
 * Creates an index on an input file
 * @author Ahmed Eldawy
 *
 */
public class Repartition {
  static final Log LOG = LogFactory.getLog(Repartition.class);
  
  /**
   * The map class maps each object to every cell it overlaps with.
   * @author Ahmed Eldawy
   *
   */
  public static class RepartitionMap<T extends Shape> extends MapReduceBase
      implements Mapper<Rectangle, T, IntWritable, T> {
    /**List of cells used by the mapper*/
    private CellInfo[] cellInfos;
    
    /**Used to output intermediate records*/
    private IntWritable cellId = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      try {
        cellInfos = SpatialSite.getCells(job);
        super.configure(job);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    public void map(Rectangle cellMbr, T shape,
        OutputCollector<IntWritable, T> output, Reporter reporter)
        throws IOException {
      Rectangle shape_mbr = shape.getMBR();
      if (shape_mbr == null)
        return;
      // Only send shape to output if its lowest corner lies in the cellMBR
      // This ensures that a replicated shape in an already partitioned file
      // doesn't get send to output from all partitions
      if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {
        for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
          if (cellInfos[cellIndex].isIntersected(shape_mbr)) {
            cellId.set((int) cellInfos[cellIndex].cellId);
            output.collect(cellId, shape);
          }
        }
      }
    }
  }
  
  /**
   * The map class maps each object to the cell with maximum overlap.
   * @author Ahmed Eldawy
   *
   */
  public static class RepartitionMapNoReplication<T extends Shape> extends MapReduceBase
      implements Mapper<Rectangle, T, IntWritable, T> {
    /**List of cells used by the mapper*/
    private CellInfo[] cellInfos;
    
    /**Used to output intermediate records*/
    private IntWritable cellId = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      try {
        cellInfos = SpatialSite.getCells(job);
        super.configure(job);
      } catch (IOException e) {
        throw new RuntimeException("Error loading cells", e);
      }
    }

    public void map(Rectangle cellMbr, T shape,
        OutputCollector<IntWritable, T> output, Reporter reporter)
        throws IOException {
      Rectangle shape_mbr = shape.getMBR();
      if (shape_mbr == null)
        return;
      double maxOverlap = -1.0;
      int bestCell = -1;
      // Only send shape to output if its lowest corner lies in the cellMBR
      // This ensures that a replicated shape in an already partitioned file
      // doesn't get send to output from all partitions
      if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {
        for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
          Rectangle overlap = cellInfos[cellIndex].getIntersection(shape_mbr);
          if (overlap != null) {
            double overlapArea = overlap.getWidth() * overlap.getHeight();
            if (bestCell == -1 || overlapArea > maxOverlap) {
              maxOverlap = overlapArea;
              bestCell = cellIndex;
            }
          }
        }
      }
      if (bestCell != -1) {
        cellId.set((int) cellInfos[bestCell].cellId);
        output.collect(cellId, shape);
      } else {
        LOG.warn("Shape: "+shape+" doesn't overlap any partitions");
      }
    }
  }
  
  public static class RepartitionReduce<T extends Shape> extends MapReduceBase
  implements Reducer<IntWritable, T, IntWritable, T> {

    @Override
    public void reduce(IntWritable cellIndex, Iterator<T> shapes,
        OutputCollector<IntWritable, T> output, Reporter reporter)
        throws IOException {
      T shape = null;
      LOG.info("Closing partition #"+cellIndex);
      while (shapes.hasNext()) {
        shape = shapes.next();
        output.collect(cellIndex, shape);
      }
      LOG.info("Done with all records in #"+cellIndex);
      // Close cell
      output.collect(new IntWritable(-cellIndex.get()), shape);
      LOG.info("Done with cell #"+cellIndex);
    }
    
  }
  
  public static class RepartitionOutputCommitter extends FileOutputCommitter {
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
          String ext = resultFiles[0].getPath().getName()
                .substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
          Path masterPath = new Path(outPath, "_master" + ext);
          OutputStream destOut = outFs.create(masterPath);
          byte[] buffer = new byte[4096];
          for (FileStatus f : resultFiles) {
            InputStream in = outFs.open(f.getPath());
            int bytes_read;
            do {
              bytes_read = in.read(buffer);
              if (bytes_read > 0)
                destOut.write(buffer, 0, bytes_read);
            } while (bytes_read > 0);
            in.close();
            outFs.delete(f.getPath(), false);
          }
          destOut.close();
        }
        
        // Plot an image for the partitions used in file
  /*      
        CellInfo[] cellInfos = SpatialSite.getCells(job);
        Path imagePath = new Path(outPath, "_partitions.png");
        int imageSize = (int) (Math.sqrt(cellInfos.length) * 300);
        Plot.plotLocal(masterPath, imagePath, new Partition(), imageSize, imageSize, Color.BLACK, false, false, false);
  */      
      }
    }

  /**
   * Calculates number of partitions required to index the given file.
   * @param conf The current configuration which can contain user-defined parameters
   * @param inFileSize The size of the input file in bytes
   * @param outFs The output file system where the index will be written
   * @param outFile The path of the output file which is used to get the output block size.
   * @param blockSize If set, this will override the default output block size.
   * @return The number of blocks needed to write the index file
   */
  public static int calculateNumberOfPartitions(Configuration conf, long inFileSize,
      FileSystem outFs, Path outFile, long blockSize) {
    final float IndexingOverhead =
        conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f);
    long indexedFileSize = (long) (inFileSize * (1 + IndexingOverhead));
    if (blockSize == 0)
      blockSize = outFs.getDefaultBlockSize(outFile);
    return (int)Math.ceil((float)indexedFileSize / blockSize);
  }
	
  /**
   * Repartitions a file that is already in HDFS. It runs a MapReduce job
   * that partitions the file into cells, and writes each cell separately.
   * @param inFile The input raw file that needs to be indexed.
   * @param outPath The output path where the index will be written.
   * @param stockShape An instance of the shapes stored in the input file.
   * @param blockSize The block size for the constructed index.
   * @param sindex The type of index to build.
   * @param overwrite Whether to overwrite the output or not.
   * @throws IOException If an exception happens while preparing the job.
   * @throws InterruptedException If the underlying MapReduce job was interrupted.
   */
  @Deprecated
  public static void repartitionMapReduce(Path inFile, Path outPath,
      Shape stockShape, long blockSize, String sindex,
      boolean overwrite) throws IOException, InterruptedException {
    OperationsParams params = new OperationsParams();
    if (stockShape != null)
      params.setClass("shape", stockShape.getClass(), Shape.class);
    params.setLong("blocksize", blockSize);
    params.set("sindex", sindex);
    params.setBoolean("overwrite", overwrite);
    repartitionMapReduce(inFile, outPath, null, params);
  }
  
  public static void repartitionMapReduce(Path inFile, Path outPath, CellInfo[] cellInfos, 
      OperationsParams params) throws IOException, InterruptedException {
    String sindex = params.get("sindex");
    boolean overwrite = params.getBoolean("overwrite", false);
    Shape stockShape = params.getShape("shape");
    
    FileSystem outFs = outPath.getFileSystem(params);

    // Calculate number of partitions in output file
    // Copy blocksize from source file if it's globally indexed
    @SuppressWarnings("deprecation")
	final long blockSize = outFs.getDefaultBlockSize();
    
    // Calculate the dimensions of each partition based on gindex type
    if(cellInfos == null){
    		if (sindex.equals("grid")) {
    	      Rectangle input_mbr = FileMBR.fileMBR(inFile, params);
    	      long inFileSize = FileMBR.sizeOfLastProcessedFile;
    	      int num_partitions = calculateNumberOfPartitions(new Configuration(),
    	          inFileSize, outFs, outPath, blockSize);

    	      GridInfo gridInfo = new GridInfo(input_mbr.x1, input_mbr.y1,
    	          input_mbr.x2, input_mbr.y2);
    	      gridInfo.calculateCellDimensions(num_partitions);
    	      cellInfos = gridInfo.getAllCells();
    	    } else if (sindex.equals("rtree") || sindex.equals("r+tree") ||
    	        sindex.equals("str") || sindex.equals("str+")) {
    	      // Pack in rectangles using an RTree
    	      cellInfos = packInRectangles(inFile, outPath, params);
    	    } else {
    	      throw new RuntimeException("Unsupported spatial index: "+sindex);
    	    }	
    }
        
    JobConf job = new JobConf(params, Repartition.class);

    job.setJobName("Repartition");
    
    // Overwrite output file
    if (outFs.exists(outPath)) {
      if (overwrite)
        outFs.delete(outPath, true);
      else
        throw new RuntimeException("Output file '" + outPath
            + "' already exists and overwrite flag is not set");
    }
    
    // Decide which map function to use depending on the type of global index
    if (sindex.equals("rtree") || sindex.equals("str")) {
      // Repartition without replication
      job.setMapperClass(RepartitionMapNoReplication.class);
    } else {
      // Repartition with replication (grid, str+, and r+tree)
      job.setMapperClass(RepartitionMap.class);
    }
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(stockShape.getClass());
    ShapeInputFormat.setInputPaths(job, inFile);
    job.setInputFormat(ShapeInputFormat.class);

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));

    FileOutputFormat.setOutputPath(job,outPath);
    if (sindex.equals("grid") || sindex.equals("str") || sindex.equals("str+")) {
      job.setOutputFormat(GridOutputFormat.class);
    } else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
      // For now, the two types of local index are the same
      job.setOutputFormat(RTreeGridOutputFormat.class);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }

    SpatialSite.setCells(job, cellInfos);
    job.setBoolean(SpatialSite.OVERWRITE, overwrite);

    // Set reduce function
    job.setReducerClass(RepartitionReduce.class);
    job.setNumReduceTasks(Math.max(1, Math.min(cellInfos.length,
        (clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));

    // Set output committer that combines output files together
    job.setOutputCommitter(RepartitionOutputCommitter.class);
    
    JobClient.runJob(job);
  
  }
  
  /**
   * Repartitions an input file according to the given list of cells.
   * @param inFile The input raw file that needs to be indexed.
   * @param outPath The output path where the index will be written.
   * @param stockShape An instance of the shapes stored in the input file.
   * @param blockSize The block size for the constructed index.
   * @param cellInfos A predefined set of cells to use as a global index
   * @param sindex The type of index to build.
   * @param overwrite Whether to overwrite the output or not.
   * @throws IOException If an exception happens while preparing the job.
   */
  public static void repartitionMapReduce(Path inFile, Path outPath,
      Shape stockShape, long blockSize, CellInfo[] cellInfos, String sindex,
      boolean overwrite) throws IOException {
    JobConf job = new JobConf(Repartition.class);

    job.setJobName("Repartition");
    FileSystem outFs = outPath.getFileSystem(job);
    
    // Overwrite output file
    if (outFs.exists(outPath)) {
      if (overwrite)
        outFs.delete(outPath, true);
      else
        throw new RuntimeException("Output file '" + outPath
            + "' already exists and overwrite flag is not set");
    }
    
    // Decide which map function to use depending on the type of global index
    if (sindex.equals("rtree") || sindex.equals("str")) {
      // Repartition without replication
      job.setMapperClass(RepartitionMapNoReplication.class);
    } else {
      // Repartition with replication (grid and r+tree)
      job.setMapperClass(RepartitionMap.class);
    }
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(stockShape.getClass());
    ShapeInputFormat.setInputPaths(job, inFile);
    job.setInputFormat(ShapeInputFormat.class);

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));

    FileOutputFormat.setOutputPath(job,outPath);
    if (sindex.equals("grid") || sindex.equals("str") || sindex.equals("str+")) {
      job.setOutputFormat(GridOutputFormat.class);
    } else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
      // For now, the two types of local index are the same
      job.setOutputFormat(RTreeGridOutputFormat.class);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    SpatialSite.setCells(job, cellInfos);
    job.setBoolean(SpatialSite.OVERWRITE, overwrite);

    // Set reduce function
    job.setReducerClass(RepartitionReduce.class);
    job.setNumReduceTasks(Math.max(1, Math.min(cellInfos.length,
        (clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));

    // Set output committer that combines output files together
    job.setOutputCommitter(RepartitionOutputCommitter.class);
    
    if (blockSize != 0) {
      job.setLong("dfs.block.size", blockSize);
      job.setLong("fs.local.block.size", blockSize);
    }
    
    JobClient.runJob(job);
  }
  
  public static <S extends Shape> CellInfo[] packInRectangles(Path inFile,
      Path outFile, OperationsParams params)
      throws IOException {
    return packInRectangles(new Path[] { inFile }, outFile, params, null);
  }


  public static <S extends Shape> CellInfo[] packInRectangles(Path inFile,
      Path outFile, OperationsParams params, Rectangle fileMBR)
      throws IOException {
    return packInRectangles(new Path[] { inFile }, outFile, params, fileMBR);
  }

  public static CellInfo[] packInRectangles(Path[] files,
      Path outFile, OperationsParams params, Rectangle fileMBR)
      throws IOException {
    final Vector<Point> sample = new Vector<Point>();
    
    float sample_ratio =
        params.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    long sample_size =
      params.getLong(SpatialSite.SAMPLE_SIZE, 100*1024*1024);
    
    LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
    ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
      @Override
      public void collect(Point value) {
        sample.add(value.clone());
      }
    };
    OperationsParams params2 = new OperationsParams(params);
    params2.setFloat("ratio", sample_ratio);
    params2.setLong("size", sample_size);
    params2.setClass("outshape", Point.class, TextSerializable.class);
    Sampler.sample(files, resultCollector, params2);
    LOG.info("Finished reading a sample of size: "+sample.size()+" records");
    
    long inFileSize = Sampler.sizeOfLastProcessedFile;

    // Compute an approximate MBR to determine the desired number of rows
    // and columns
    Rectangle approxMBR;
    if (fileMBR == null) {
      approxMBR = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
      for (Point pt : sample)
        approxMBR.expand(pt);
    } else {
      approxMBR = fileMBR;
    }
    GridInfo gridInfo = new GridInfo(approxMBR.x1, approxMBR.y1, approxMBR.x2, approxMBR.y2);
    FileSystem outFs = outFile.getFileSystem(params);
    @SuppressWarnings("deprecation")
	long blocksize = outFs.getDefaultBlockSize();
    gridInfo.calculateCellDimensions(Math.max(1, (int)((inFileSize + blocksize / 2) / blocksize)));
    if (fileMBR == null)
      gridInfo.set(-Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
    else
      gridInfo.set(fileMBR);
    
    Rectangle[] rectangles = RTree.packInRectangles(gridInfo,
            sample.toArray(new Point[sample.size()]));
    CellInfo[] cellsInfo = new CellInfo[rectangles.length];
    for (int i = 0; i < rectangles.length; i++)
      cellsInfo[i] = new CellInfo(i + 1, rectangles[i]);
    
    return cellsInfo;
  }
  
  /**
   * 
   * @param inFile The input raw file that needs to be indexed.
   * @param outFile The output path where the index will be written.
   * @param stockShape An instance of the shapes stored in the input file.
   * @param blockSize The block size for the constructed index.
   * @param sindex The type of index to build.
   * @param overwrite Whether to overwrite the output or not.
   * @throws IOException If an exception happens while preparing the job.
   * @throws InterruptedException If the underlying MapReduce job was interrupted.
   * @deprecated this method is replaced with
   *   {@link #repartitionLocal(Path, Path, OperationsParams)}
   */
  @Deprecated
  public static <S extends Shape> void repartitionLocal(Path inFile,
      Path outFile, S stockShape, long blockSize, String sindex,
      boolean overwrite)
          throws IOException, InterruptedException {
    OperationsParams params = new OperationsParams();
    if (stockShape != null)
      params.setClass("shape", stockShape.getClass(), Shape.class);
    params.setLong("blocksize", blockSize);
    params.set("sindex", sindex);
    params.setBoolean("overwrite", overwrite);
    repartitionLocal(inFile, outFile, params);
  }
  
  @SuppressWarnings("deprecation")
  public static <S extends Shape> void repartitionLocal(Path inFile,
      Path outFile, OperationsParams params) throws IOException, InterruptedException {
    String sindex = params.get("sindex");
    long blockSize = params.getSize("blocksize");

    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileSystem outFs = outFile.getFileSystem(new Configuration());
    
    // Calculate number of partitions in output file
    if (blockSize == 0) {
      GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, inFile);
      if (globalIndex != null) {
        // Copy blocksize from source file if it's globally indexed
        blockSize = inFs.getFileStatus(new Path(inFile, globalIndex.iterator().next().filename)).getBlockSize();
      } else {
        // Use default block size for output file system
        blockSize = outFs.getDefaultBlockSize();
      }
    }

    // Calculate the dimensions of each partition based on gindex type
    CellInfo[] cells;
    if (sindex.equals("grid")) {
      Rectangle input_mbr = FileMBR.fileMBR(inFile, params);
      long inFileSize = FileMBR.sizeOfLastProcessedFile;
      int num_partitions = calculateNumberOfPartitions(new Configuration(),
          inFileSize, outFs, outFile, blockSize);

      GridInfo gridInfo = new GridInfo(input_mbr.x1, input_mbr.y1,
          input_mbr.x2, input_mbr.y2);
      gridInfo.calculateCellDimensions(num_partitions);
      cells = gridInfo.getAllCells();
    } else if (sindex.equals("rtree") || sindex.equals("r+tree") ||
        sindex.equals("str") || sindex.equals("str+")) {
      cells = packInRectangles(inFile, outFile, params);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    repartitionLocal(inFile, outFile, cells, params);
  }

  /**
   * Repartitions a file on local machine without MapReduce jobs.
   * @param in The input raw file that needs to be indexed.
   * @param out The output path where the index will be written.
   * @param stockShape An instance of the shapes stored in the input file.
   * @param blockSize The block size for the constructed index.
   * @param cells A predefined set of cells to use as a global index
   * @param sindex The type of index to build.
   * @param overwrite Whether to overwrite the output or not.
   * @throws IOException If an exception happens while preparing the job.
   * This method is @deprecated. Please use the method
   * {@link #repartitionLocal(Path, Path, CellInfo[], OperationsParams)}
   */
  @Deprecated
  public static <S extends Shape> void repartitionLocal(Path in, Path out,
      S stockShape, long blockSize, CellInfo[] cells, String sindex,
      boolean overwrite) throws IOException {
    OperationsParams params = new OperationsParams();
    params.setClass("shape", stockShape.getClass(), Shape.class);
    params.setLong("blocksize", blockSize);
    params.set("sindex", sindex);
    params.setBoolean("overwrite", overwrite);
    repartitionLocal(in, out, cells, params);
  }

  public static <S extends Shape> void repartitionLocal(Path inFile, Path outFile,
      CellInfo[] cells, OperationsParams params) throws IOException {
    String sindex = params.get("sindex");
    Shape shape = params.getShape("shape");
    JobConf job = new JobConf(params, Repartition.class);

    ShapeRecordWriter<Shape> writer;
    if (sindex.equals("grid") ||
      sindex.equals("str") || sindex.equals("str+")) {
      writer = new GridRecordWriter<Shape>(outFile, job, null, cells);
    } else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
      writer = new RTreeGridRecordWriter<Shape>(outFile, job, null, cells);
      writer.setStockObject(shape);
    } else {
      throw new RuntimeException("Unupoorted spatial idnex: "+sindex);
    }
    
    // Read input file(s)
    FileInputFormat.addInputPath(job, inFile);
    ShapeInputFormat<S> inputFormat = new ShapeInputFormat<S>();
    InputSplit[] splits = inputFormat.getSplits(job, 1);
    for (InputSplit split : splits) {
      ShapeRecordReader<Shape> reader = new ShapeRecordReader<Shape>(params, (FileSplit) split);
      Rectangle c = reader.createKey();
      
      while (reader.next(c, shape)) {
        if (shape.getMBR() != null)
          writer.write(NullWritable.get(), shape);
      }
      reader.close();
    }
    writer.close(null);
  }

  
  /**
   * @param inFile The input raw file that needs to be indexed.
   * @param outputPath The output path where the index will be written.
   * @param params The parameters and configuration of the underlying job
   * @throws IOException If an exception happens while preparing the job.
   * @throws InterruptedException If the underlying MapReduce job was interrupted.
   */
  public static void repartition(Path inFile, Path outputPath,
      OperationsParams params) throws IOException, InterruptedException {
    JobConf job = new JobConf(params, FileMBR.class);
    FileInputFormat.addInputPath(job, inFile);
    ShapeInputFormat<Shape> inputFormat = new ShapeInputFormat<Shape>();

    boolean autoLocal = inputFormat.getSplits(job, 1).length <= 3;
    boolean isLocal = params.getBoolean("local", autoLocal);
    
    if (isLocal)
      repartitionLocal(inFile, outputPath, params);
    else
      repartitionMapReduce(inFile, outputPath, null, params);
  }

  protected static void printUsage() {
    System.out.println("Builds a spatial index on an input file");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|rectangle|polygon> - (*) Type of shapes stored in input file");
    System.out.println("sindex:<index> - (*) Type of spatial index (grid|rtree|r+tree|str|str+)");
    System.out.println("-overwrite - Overwrite output file without noitce");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
	 * Entry point to the operation.
	 * shape:&lt;s&gt; the shape to use. Automatically inferred from input file if not set.
	 * sindex&lt;index&gt; Type of spatial index to build
	 * cells-of:&lt;filename&gt; Use the cells of the given file for the global index.
	 * blocksize:&lt;size&gt; Size of each block in indexed file in bytes.
	 * -local: If set, the index is built on the local machine.
	 * input filename: Input file in HDFS
	 * output filename: Output file in HDFS
	 * @param args The command line arguments
	 * @throws Exception If any exception happens while the job is running
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
    repartition(inputPath, outputPath, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total indexing time in millis "+(t2-t1));
	}
}
