package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.spatial.GridOutputFormat;
import org.apache.hadoop.mapred.spatial.ShapeLineInputFormat;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.ResultCollector2;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialAlgorithms;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

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
  private static final Log LOG = LogFactory.getLog(SJMR.class);
  
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
   * @author eldawy
   *
   */
  public static class SJMRMap extends MapReduceBase
  implements
  Mapper<CellInfo, Text, IntWritable, IndexedText> {
    /**List of cells used by the mapper*/
    private Shape shape;
    private IndexedText outputValue = new IndexedText();
    private CellInfo[] cellInfos;
    private IntWritable cellId = new IntWritable();
    private Path[] inputFiles;
    private InputSplit currentSplit;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      // Retrieve cells to use for partitioning
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      // Create a stock shape for deserializing lines
      shape = SpatialSite.createStockShape(job);
      // Get input paths to determine file index for every record
      inputFiles = FileInputFormat.getInputPaths(job);
    }

    @Override
    public void map(CellInfo dummy, Text value,
        OutputCollector<IntWritable, IndexedText> output,
        Reporter reporter) throws IOException {
      if (reporter.getInputSplit() != currentSplit) {
      	FileSplit fsplit = (FileSplit) reporter.getInputSplit();
      	for (int i = 0; i < inputFiles.length; i++) {
      		if (inputFiles[i].equals(fsplit.getPath())) {
      			outputValue.index = (byte) i;
      		}
      	}
      }

      Text tempText = new Text(value);
      shape.fromText(tempText);
      for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
        if (cellInfos[cellIndex].isIntersected(shape)) {
          cellId.set((int)cellInfos[cellIndex].cellId);
          outputValue.text = value;
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
    private CellInfo[] cellInfos;

    private S shape;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      shape = (S) SpatialSite.createStockShape(job);
      inputFileCount = FileInputFormat.getInputPaths(job).length;
    }

    @Override
    public void reduce(IntWritable cellId, Iterator<IndexedText> values,
        final OutputCollector<S, S> output, Reporter reporter)
        throws IOException {
      // Extract CellInfo (MBR) for duplicate avoidance checking
      int i_cell = 0;
      while (i_cell < cellInfos.length && cellInfos[i_cell].cellId != cellId.get())
        i_cell++;
      final CellInfo cellInfo = cellInfos[i_cell];
      
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
            if (cellInfo.contains(intersectionMBR.x, intersectionMBR.y)) {
              // Report to the reduce result collector
              output.collect(x, y);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  public static<S extends Shape> long sjmr(FileSystem fs, Path[] inputFiles,
      Path userOutputPath,
      GridInfo gridInfo, S stockShape, OutputCollector<S, S> output, boolean overwrite) throws IOException {
    JobConf job = new JobConf(SJMR.class);
    
    FileSystem outFs = inputFiles[0].getFileSystem(job);
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path("/" + inputFiles[0].getName() + ".sjmr_"
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
    job.set(SpatialSite.SHAPE_CLASS, stockShape.getClass().getName());
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
    long max_size = 0;
    for (Path file : inputFiles) {
      long size = fs.getFileStatus(file).getLen();
      total_size += size;
      if (size > max_size) {
        max_size = size;
      }
    }
    // If the largest file is globally indexed, use its partitions
    CellInfo[] cellsInfo;
    total_size += total_size * job.getFloat(SpatialSite.INDEXING_OVERHEAD,
    		0.002f);
    int num_cells = (int) (total_size / outFs.getDefaultBlockSize());
    gridInfo.calculateCellDimensions(num_cells);
    cellsInfo = gridInfo.getAllCells();
    job.setBoolean(SpatialSite.AutoCombineSplits, false);
    job.set(GridOutputFormat.OUTPUT_CELLS,
        GridOutputFormat.encodeCells(cellsInfo));
    
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Start the job
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();

    // Read job result
    if (output != null) {
      @SuppressWarnings("unchecked")
      S s1 = stockShape, s2 = (S) stockShape.clone();
      FileStatus[] results = outFs.listStatus(outputPath);
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0
            && fileStatus.getPath().getName().startsWith("part-")) {
          // Report every single result as a pair of shapes
          LineReader lineReader = new LineReader(outFs.open(fileStatus
              .getPath()));
          Text text = new Text();
          while (lineReader.readLine(text) > 0) {
            String str = text.toString();
            String[] parts = str.split("\t", 2);
            s1.fromText(new Text(parts[0]));
            s2.fromText(new Text(parts[1]));
            output.collect(s1, s2);
          }
          lineReader.close();
        }
      }
    }
    
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
    GridInfo gridInfo = cla.getGridInfo();
    Shape stockShape = cla.getShape(true);
    if (gridInfo == null) {
      Rectangle rect = cla.getRectangle();
      if (rect == null) {
        for (Path path : inputFiles) {
          Rectangle file_mbr = FileMBR.fileMBRLocal(fs, path, stockShape);
          rect = rect == null ? file_mbr : rect.union(file_mbr);
        }
        LOG.info("Automatically calculated MBR: "+rect);
      }
      gridInfo = new GridInfo(rect.x, rect.y, rect.width, rect.height);
    }
    long t1 = System.currentTimeMillis();
    long resultSize = sjmr(fs, inputFiles, outputPath, gridInfo, stockShape, null, overwrite);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
    System.out.println("Result size: "+resultSize);
  }

}
