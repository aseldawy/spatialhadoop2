/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import edu.umn.cs.spatialHadoop.util.Progressable;

/**
 * An implementation of Spatial Join MapReduce as described in
 * S. Zhang, J. Han, Z. Liu, K. Wang, and Z. Xu. SJMR:
 * Parallelizing spatial join with MapReduce on clusters. In
 * CLUSTER, pages 1–8, New Orleans, LA, Aug. 2009.
 * The map function partitions data into grid cells and the reduce function
 * makes a plane-sweep over each cell.
 * @author eldawy
 *
 */
public class SJMR {
  
  /**Class logger*/
  private static final Log LOG = LogFactory.getLog(SJMR.class);
  private static final String PartitionGrid = "SJMR.PartitionGrid";
  public static final String PartitioiningFactor = "partition-grid-factor";
  private static final String InactiveMode = "SJMR.InactiveMode";
  private static final String isFilterOnlyMode = "DJ.FilterOnlyMode";
  private static final String JoiningThresholdPerOnce = "DJ.JoiningThresholdPerOnce";
  public static boolean isReduceInactive = false;
  public static boolean isSpatialJoinOutputRequired = true;
  public static boolean isFilterOnly = false;
  public static int joiningThresholdPerOnce = 50000;
  
  

  
  public static class IndexedText implements Writable {
    public byte index;
    public Text text;
    
    IndexedText() {
      text = new Text();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeByte(index);
      text.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      index = in.readByte();
      text.readFields(in);
    }
  }
  
  /**
   * Map function for the self join version of SJMR. Instead of associating
   * each record with an index to indicate whether it's left or right, each
   * record is only replicated once and the reduce function will do a self join
   * for all input records.
   *  
   * @author Ahmed Eldawy
   *
   */
  public static class SelfSJMRMap extends MapReduceBase
  implements
  Mapper<Rectangle, Shape, IntWritable, Shape> {
    private GridInfo gridInfo;
    private IntWritable cellId = new IntWritable();
    
    @Override
    public void map(Rectangle key, Shape shape,
        OutputCollector<IntWritable, Shape> output, Reporter reporter)
        throws IOException {
      java.awt.Rectangle cells = gridInfo.getOverlappingCells(shape.getMBR());
      
      for (int col = cells.x; col < cells.x + cells.width; col++) {
        for (int row = cells.y; row < cells.y + cells.height; row++) {
          cellId.set(row * gridInfo.columns + col + 1);
          output.collect(cellId, shape);
        }
      }
    }
  }
  
  /**
   * The map class maps each object to all cells it overlaps with.
   * @author Ahmed Eldawy
   *
   */
  public static class SJMRMap extends MapReduceBase
  implements
  Mapper<Rectangle, Text, IntWritable, IndexedText> {
    private Shape shape;
    private IndexedText outputValue = new IndexedText();
    private GridInfo gridInfo;
    private IntWritable cellId = new IntWritable();
    private Path[] inputFiles;
    private InputSplit currentSplit;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      // Retrieve grid to use for partitioning
      gridInfo = (GridInfo) OperationsParams.getShape(job, PartitionGrid);
      // Create a stock shape for deserializing lines
      shape = SpatialSite.createStockShape(job);
      // Get input paths to determine file index for every record
      inputFiles = FileInputFormat.getInputPaths(job);
    }

    @Override
    public void map(Rectangle cellMbr, Text value,
        OutputCollector<IntWritable, IndexedText> output,
        Reporter reporter) throws IOException {
      if (reporter.getInputSplit() != currentSplit) {
      	FileSplit fsplit = (FileSplit) reporter.getInputSplit();
      	for (int i = 0; i < inputFiles.length; i++) {
      		if (fsplit.getPath().toString().startsWith(inputFiles[i].toString())) {
      			outputValue.index = (byte) i;
      		}
      	}
      	currentSplit = reporter.getInputSplit();
      }
      

      Text tempText = new Text(value);
      outputValue.text = value;
      shape.fromText(tempText);
      Rectangle shape_mbr = shape.getMBR();
      // Do a reference point technique to avoid processing the same record twice
      if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {
        Rectangle shapeMBR = shape.getMBR();
        if (shapeMBR == null)
          return;

        java.awt.Rectangle cells = gridInfo.getOverlappingCells(shapeMBR);
        for (int col = cells.x; col < cells.x + cells.width; col++) {
          for (int row = cells.y; row < cells.y + cells.height; row++) {
            cellId.set(row * gridInfo.columns + col + 1);
            output.collect(cellId, outputValue);
          }
        }
      }
    }
  }
  
  public static class SelfSJMRReduce<S extends Shape> extends MapReduceBase implements
  Reducer<IntWritable, S, S, S> {
    /**List of cells used by the reducer*/
    private GridInfo grid;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      grid = (GridInfo) OperationsParams.getShape(job, PartitionGrid);
    }

    @Override
    public void reduce(IntWritable cellId, Iterator<S> values,
        final OutputCollector<S, S> output, Reporter reporter) throws IOException {
      // Extract CellInfo (MBR) for duplicate avoidance checking
      final CellInfo cellInfo = grid.getCell(cellId.get());
      
      Vector<S> shapes = new Vector<S>();
      
      while (values.hasNext()) {
        S s = values.next();
        shapes.add((S) s.clone());
      }
      
      SpatialAlgorithms.SelfJoin_planeSweep(shapes.toArray(new Shape[shapes.size()]), true, new OutputCollector<Shape, Shape>() {

        @Override
        public void collect(Shape r, Shape s) throws IOException {
          // Perform a reference point duplicate avoidance technique
          Rectangle intersectionMBR = r.getMBR().getIntersection(s.getMBR());
          // Error: intersectionMBR may be null.
          if (intersectionMBR != null) {
            if (cellInfo.contains(intersectionMBR.x1, intersectionMBR.y1)) {
              // Report to the reduce result collector
              output.collect((S)r, (S)s);
            }
          }
        }
      }, new Progressable.ReporterProgressable(reporter));
    }
  }
  
  public static class SJMRReduce<S extends Shape> extends MapReduceBase implements
  Reducer<IntWritable, IndexedText, S, S> {
	 /**Class logger*/
	 private static final Log sjmrReduceLOG = LogFactory.getLog(SJMRReduce.class);
	  
    /**Number of files in the input*/
    private int inputFileCount;
    
    /**List of cells used by the reducer*/
    private GridInfo grid;
    private boolean inactiveMode;
	private boolean isFilterOnly;
	private int shapesThresholdPerOnce;
	
    private S shape;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      grid = (GridInfo) OperationsParams.getShape(job, PartitionGrid);
      shape = (S) SpatialSite.createStockShape(job);
      inputFileCount = FileInputFormat.getInputPaths(job).length;
      inactiveMode = OperationsParams.getInactiveModeFlag(job, InactiveMode);
	  isFilterOnly = OperationsParams.getFilterOnlyModeFlag(job, isFilterOnlyMode);
	  shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
      sjmrReduceLOG.info("configured the reduced task");
    }

    @Override
    public void reduce(IntWritable cellId, Iterator<IndexedText> values,
        final OutputCollector<S, S> output, Reporter reporter)
            throws IOException {
      if(!inactiveMode){
        LOG.info("Start reduce() logic now !!!"); 
        long t1 = System.currentTimeMillis();	

        // Extract CellInfo (MBR) for duplicate avoidance checking
        final CellInfo cellInfo = grid.getCell(cellId.get());

        // Partition retrieved shapes (values) into lists for each file
        List<S>[] shapeLists = new List[inputFileCount];
        for (int i = 0; i < shapeLists.length; i++) {
          shapeLists[i] = new Vector<S>();
        }

        while (values.hasNext()) {
          do{
            IndexedText t = values.next();
            S s = (S) shape.clone();
            s.fromText(t.text);
            shapeLists[t.index].add(s);	
          } while(values.hasNext() && shapeLists[1].size() < shapesThresholdPerOnce);

          // Perform spatial join between the two lists
          sjmrReduceLOG.info("Joining (" + shapeLists[0].size() +" X "+ shapeLists[1].size()+ ")...");
          if(isFilterOnly){
            SpatialAlgorithms.SpatialJoin_planeSweepFilterOnly(shapeLists[0], shapeLists[1], new ResultCollector2<S, S>() {
              @Override
              public void collect(S x, S y) {
                if(isSpatialJoinOutputRequired){
                  try {
                    Rectangle intersectionMBR = x.getMBR().getIntersection(y.getMBR());
                    // Error: intersectionMBR may be null.
                    if (intersectionMBR != null) {
                      if (cellInfo.contains(intersectionMBR.x1, intersectionMBR.y1)) {
                        // Report to the reduce result collector
                        output.collect(x, y);
                      }
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                  }	
                }
              }
            }, reporter);  
          }else{
            SpatialAlgorithms.SpatialJoin_planeSweep(shapeLists[0], shapeLists[1], new ResultCollector2<S, S>() {
              @Override
              public void collect(S x, S y) {
                if(isSpatialJoinOutputRequired){
                  try {
                    Rectangle intersectionMBR = x.getMBR().getIntersection(y.getMBR());
                    // Error: intersectionMBR may be null.
                    if (intersectionMBR != null) {
                      if (cellInfo.contains(intersectionMBR.x1, intersectionMBR.y1)) {
                        // Report to the reduce result collector
                        output.collect(x, y);
                      }
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                  }	
                }
              }
            }, reporter);

          }
          shapeLists[1].clear();
        }

        long t2 = System.currentTimeMillis();
        LOG.info("Reducer finished in: "+(t2-t1)+" millis");

      }else{
        LOG.info("Nothing to do !!!");	
      }
    }
  }

  public static <S extends Shape> long sjmr(Path[] inFiles,
      Path userOutputPath, OperationsParams params) throws IOException, InterruptedException {
    JobConf job = new JobConf(params, SJMR.class);
    
    LOG.info("SJMR journey starts ....");
    FileSystem inFs = inFiles[0].getFileSystem(job);
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      FileSystem outFs = FileSystem.get(job);
      do {
        outputPath = new Path(inFiles[0].getName() + ".sjmr_"
            + (int) (Math.random() * 1000000));
      } while (outFs.exists(outputPath));
    }
    FileSystem outFs = outputPath.getFileSystem(job);
    
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setJobName("SJMR");
    job.setMapperClass(SJMRMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IndexedText.class);
    job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));
    job.setLong("mapred.min.split.size",
        Math.max(inFs.getFileStatus(inFiles[0]).getBlockSize(),
            inFs.getFileStatus(inFiles[1]).getBlockSize()));


    job.setReducerClass(SJMRReduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

    job.setInputFormat(ShapeLineInputFormat.class);
    if (job.getBoolean("output", true))
      job.setOutputFormat(TextOutputFormat.class);
    else
      job.setOutputFormat(NullOutputFormat.class);
    ShapeLineInputFormat.setInputPaths(job, inFiles);
    
    // Calculate and set the dimensions of the grid to use in the map phase
    long total_size = 0;
    Rectangle mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Path file : inFiles) {
      FileSystem fs = file.getFileSystem(params);
      Rectangle file_mbr = FileMBR.fileMBR(file, params);
      mbr.expand(file_mbr);
      total_size += FileUtil.getPathSize(fs, file);
    }
    // If the largest file is globally indexed, use its partitions
    total_size += total_size * job.getFloat(SpatialSite.INDEXING_OVERHEAD,0.2f);
    int sjmrPartitioningGridFactor = params.getInt(PartitioiningFactor, 20);
    int num_cells = (int) Math.max(1, total_size * sjmrPartitioningGridFactor /
        outFs.getDefaultBlockSize(outputPath));
    LOG.info("Number of cells is configured to be " + num_cells);

    OperationsParams.setInactiveModeFlag(job, InactiveMode, isReduceInactive);
    OperationsParams.setJoiningThresholdPerOnce(job, JoiningThresholdPerOnce, joiningThresholdPerOnce);
	OperationsParams.setFilterOnlyModeFlag(job, isFilterOnlyMode, isFilterOnly);
	
    GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
    gridInfo.calculateCellDimensions(num_cells);
    OperationsParams.setShape(job, PartitionGrid, gridInfo);
    
    TextOutputFormat.setOutputPath(job, outputPath);
    
    if (OperationsParams.isLocal(job, inFiles)) {
      // Enforce local execution if explicitly set by user or for small files
      job.set("mapred.job.tracker", "local");
    }
    
    // Start the job
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();

    return resultCount;
  }
  
  private static void printUsage() {
    System.out.println("Performs a spatial join between two files using the distributed join algorithm");
    System.out.println("Parameters: (* marks the required parameters)");
    System.out.println("<input file 1> - (*) Path to the first input file");
    System.out.println("<input file 2> - (*) Path to the second input file");
    System.out.println("<output file> - Path to output file");
    System.out.println("partition-grid-factor:<value> - Patitioning grid factor (its default value is 20)");
    System.out.println("-overwrite - Overwrite output file without notice");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] allFiles = params.getPaths();
    if (allFiles.length < 2) {
      System.err
          .println("This operation requires at least two input files");
      printUsage();
      System.exit(1);
    }
    if (allFiles.length == 2 && !params.checkInput()) {
      // One of the input files does not exist
      printUsage();
      System.exit(1);
    }
    if (allFiles.length > 2 && !params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path[] inputPaths = allFiles.length == 2 ? allFiles : params.getInputPaths();
    Path outputPath = allFiles.length == 2 ? null : params.getOutputPath();

    if (params.get("repartition-only", "no").equals("yes")) {
      isReduceInactive = true;
    }


    if (params.get("joining-per-once") != null) {
      System.out.println("joining-per-once is set to: " + params.get("joining-per-once"));
      joiningThresholdPerOnce = Integer.parseInt(params.get("joining-per-once"));
    }

    if (params.get("filter-only") != null) {
      System.out.println("filer-only mode is set to: " + params.get("filter-only"));
      if (params.get("filter-only").equals("yes")) {
        isFilterOnly = true;
      }else{
        isFilterOnly = false;
      }
    }

    if (params.get("no-output") != null) {
      System.out.println("no-output mode is set to: " + params.get("no-output"));
      if (params.get("no-output").equals("yes")){
        isSpatialJoinOutputRequired = false;
      }else{
        isSpatialJoinOutputRequired = true;
      }
    }

    long t1 = System.currentTimeMillis();
    long resultSize = sjmr(inputPaths, outputPath, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
    System.out.println("Result size: "+resultSize);
  }

}
