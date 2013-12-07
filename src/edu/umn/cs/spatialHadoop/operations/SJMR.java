/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
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

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * An implementation of Spatial Join MapReduce as it appears in
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
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(SJMR.class);
  private static final String PartitionGrid = "SJMR.PartitionGrid";
  
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
   * The map class maps each object to all cells it overlaps with.
   * @author Ahmed Eldawy
   *
   */
  public static class SJMRMap extends MapReduceBase
  implements
  Mapper<Rectangle, Text, IntWritable, IndexedText> {
    /**List of cells used by the mapper*/
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
      gridInfo = (GridInfo) SpatialSite.getShape(job, PartitionGrid);
      // Create a stock shape for deserializing lines
      shape = SpatialSite.createStockShape(job);
      // Get input paths to determine file index for every record
      inputFiles = FileInputFormat.getInputPaths(job);
    }

    @Override
    public void map(Rectangle dummy, Text value,
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
  
  public static class SJMRReduce<S extends Shape> extends MapReduceBase implements
  Reducer<IntWritable, IndexedText, S, S> {
    /**Number of files in the input*/
    private int inputFileCount;
    
    /**List of cells used by the reducer*/
    private GridInfo grid;

    private S shape;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      grid = (GridInfo) SpatialSite.getShape(job, PartitionGrid);
      shape = (S) SpatialSite.createStockShape(job);
      inputFileCount = FileInputFormat.getInputPaths(job).length;
    }

    @Override
    public void reduce(IntWritable cellId, Iterator<IndexedText> values,
        final OutputCollector<S, S> output, Reporter reporter)
        throws IOException {
      // Extract CellInfo (MBR) for duplicate avoidance checking
      final CellInfo cellInfo = grid.getCell(cellId.get());
      
      // Partition retrieved shapes (values) into lists for each file
      @SuppressWarnings("unchecked")
      List<S>[] shapeLists = new List[inputFileCount];
      for (int i = 0; i < shapeLists.length; i++) {
        shapeLists[i] = new Vector<S>();
      }
      
      while (values.hasNext()) {
        IndexedText t = values.next();
        S s = (S) shape.clone();
        s.fromText(t.text);
        shapeLists[t.index].add(s);
      }
      
      // Perform spatial join between the two lists
      SpatialAlgorithms.SpatialJoin_planeSweep(shapeLists[0], shapeLists[1], new ResultCollector2<S, S>() {
        @Override
        public void collect(S x, S y) {
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
      });
    }
  }

  public static <S extends Shape> long sjmr(FileSystem fs, Path[] inputFiles,
      Path userOutputPath, S stockShape, boolean overwrite)
      throws IOException {
    JobConf job = new JobConf(SJMR.class);
    
    FileSystem outFs = inputFiles[0].getFileSystem(job);
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path(inputFiles[0].getName() + ".sjmr_"
            + (int) (Math.random() * 1000000));
      } while (outFs.exists(outputPath));
    } else {
      if (outFs.exists(outputPath)) {
        if (overwrite) {
          outFs.delete(outputPath, true);
        } else {
          throw new RuntimeException("Output path already exists and -overwrite flag is not set");
        }
      }
    }
    
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setJobName("SJMR");
    job.setMapperClass(SJMRMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IndexedText.class);
    job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));
    job.setLong("mapred.min.split.size",
        Math.max(fs.getFileStatus(inputFiles[0]).getBlockSize(),
            fs.getFileStatus(inputFiles[1]).getBlockSize()));


    job.setReducerClass(SJMRReduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

    job.setInputFormat(ShapeLineInputFormat.class);
    SpatialSite.setShapeClass(job, stockShape.getClass());
    job.setOutputFormat(TextOutputFormat.class);
    
    String commaSeparatedFiles = "";
    for (int i = 0; i < inputFiles.length; i++) {
      if (i > 0)
        commaSeparatedFiles += ',';
      commaSeparatedFiles += inputFiles[i].toUri().toString();
    }
    ShapeLineInputFormat.addInputPaths(job, commaSeparatedFiles);
    
    // Calculate and set the dimensions of the grid to use in the map phase
    long total_size = 0;
    Rectangle mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Path file : inputFiles) {
      Rectangle file_mbr = FileMBR.fileMBR(fs, file, stockShape);
      mbr.expand(file_mbr);
      total_size += FileMBR.sizeOfLastProcessedFile;
    }
    // If the largest file is globally indexed, use its partitions
    total_size += total_size * job.getFloat(SpatialSite.INDEXING_OVERHEAD,0.2f);
    int num_cells = (int) (total_size / outFs.getDefaultBlockSize(outputPath) * 20);
    GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
    gridInfo.calculateCellDimensions(num_cells);
    SpatialSite.setShape(job, PartitionGrid, gridInfo);
    
    TextOutputFormat.setOutputPath(job, outputPath);
    
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
    System.out.println("mbr:<x,y,w,h> - MBR of the two files");
    System.out.println("-overwrite - Overwrite output file without notice");
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] allFiles = cla.getPaths();
    if (allFiles.length < 2) {
      printUsage();
      throw new RuntimeException("Input files missing");
    }
    Path[] inputFiles = new Path[] {allFiles[0], allFiles[1]};
    JobConf conf = new JobConf(DistributedJoin.class);
    FileSystem fs = inputFiles[0].getFileSystem(conf);
    
    if (!fs.exists(inputFiles[0]) || !fs.exists(inputFiles[1])) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }

    Path outputPath = allFiles.length > 2 ? allFiles[2] : null;
    boolean overwrite = cla.isOverwrite();
    Shape stockShape = cla.getShape(true);
    long t1 = System.currentTimeMillis();
    long resultSize = sjmr(fs, inputFiles, outputPath, stockShape, overwrite);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
    System.out.println("Result size: "+resultSize);
  }

}
