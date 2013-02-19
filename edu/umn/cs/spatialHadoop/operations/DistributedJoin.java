package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.spatial.BinaryRecordReader;
import org.apache.hadoop.mapred.spatial.BinarySpatialInputFormat;
import org.apache.hadoop.mapred.spatial.BlockFilter;
import org.apache.hadoop.mapred.spatial.DefaultBlockFilter;
import org.apache.hadoop.mapred.spatial.PairWritable;
import org.apache.hadoop.mapred.spatial.ShapeArrayRecordReader;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.ResultCollector2;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SimpleSpatialIndex;
import org.apache.hadoop.spatial.SpatialAlgorithms;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 * @author eldawy
 *
 */
public class DistributedJoin {
  private static final Log LOG = LogFactory.getLog(DistributedJoin.class);

  public static class SpatialJoinFilter extends DefaultBlockFilter {
    @Override
    public void selectBlockPairs(SimpleSpatialIndex<BlockLocation> gIndex1,
        SimpleSpatialIndex<BlockLocation> gIndex2,
        ResultCollector2<BlockLocation, BlockLocation> output) {
      // Do a spatial join between the two global indexes
      SimpleSpatialIndex.spatialJoin(gIndex1, gIndex2, output);
    }
  }
  
  public static class RedistributeJoinMap extends MapReduceBase
  implements Mapper<PairWritable<CellInfo>, PairWritable<? extends Writable>, Shape, Shape> {
    public void map(
        final PairWritable<CellInfo> key,
        final PairWritable<? extends Writable> value,
        final OutputCollector<Shape, Shape> output,
        Reporter reporter) throws IOException {
      final Rectangle mapperMBR = key.first.getIntersection(key.second);

      // Join two arrays
      ArrayWritable ar1 = (ArrayWritable) value.first;
      ArrayWritable ar2 = (ArrayWritable) value.second;
      SpatialAlgorithms.SpatialJoin_planeSweep(
          (Shape[])ar1.get(), (Shape[])ar2.get(),
          new ResultCollector2<Shape, Shape>() {
            @Override
            public void collect(Shape x, Shape y) {
              Rectangle intersectionMBR = x.getMBR().getIntersection(y.getMBR());
              // Employ reference point duplicate avoidance technique 
              if (mapperMBR.contains(intersectionMBR.x, intersectionMBR.y)) {
                try {
                  output.collect(x, y);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            }
          }
      );
    }
  }
  
  /**
   * Reads a pair of arrays of shapes
   * @author eldawy
   *
   */
  public static class DJRecordReaderArray extends BinaryRecordReader<CellInfo, ArrayWritable> {
    public DJRecordReaderArray(Configuration conf, CombineFileSplit fileSplits) throws IOException {
      super(conf, fileSplits);
    }
    
    @Override
    protected RecordReader<CellInfo, ArrayWritable> createRecordReader(
        Configuration conf, CombineFileSplit split, int i) throws IOException {
      FileSplit fsplit = new FileSplit(split.getPath(i),
          split.getStartOffsets()[i],
          split.getLength(i), split.getLocations());
      return new ShapeArrayRecordReader(conf, fsplit);
    }
  }

  /**
   * Input format that returns a record reader that reads a pair of arrays of
   * shapes
   * @author eldawy
   *
   */
  public static class DJInputFormatArray extends BinarySpatialInputFormat<CellInfo, ArrayWritable> {
    
    @Override
    public RecordReader<PairWritable<CellInfo>, PairWritable<ArrayWritable>> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new DJRecordReaderArray(job, (CombineFileSplit)split);
    }
  }

  /**
   * Repartition the smaller (first) file to match the partitioning of the
   * larger file.
   * @param fs
   * @param files
   * @param stockShape
   * @param fStatus
   * @param gIndexes
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  protected static void repartitionStep(FileSystem fs, final Path[] files,
      Shape stockShape) throws IOException {
    
    final FileStatus[] fStatus = new FileStatus[files.length];
    SimpleSpatialIndex<BlockLocation>[] gIndexes =
        new SimpleSpatialIndex[files.length];
    
    for (int i_file = 0; i_file < files.length; i_file++) {
      fStatus[i_file] = fs.getFileStatus(files[i_file]);
      gIndexes[i_file] = fs.getGlobalIndex(fStatus[i_file]);
    }
    
    // Do the repartition step
    long t1 = System.currentTimeMillis();
  
    // Get the partitions to use for partitioning the smaller file
    
    // Do a spatial join between partitions of the two files to find
    // overlapping partitions.
    final Set<CellInfo> cellSet = new HashSet<CellInfo>();
    final DoubleWritable matched_area = new DoubleWritable(0);
    SimpleSpatialIndex.spatialJoin(gIndexes[0], gIndexes[1], new ResultCollector2<BlockLocation, BlockLocation>() {
      @Override
      public void collect(BlockLocation x, BlockLocation y) {
        // Always use the cell of the larger file
        cellSet.add(y.getCellInfo());
        Rectangle intersection = x.getCellInfo().getIntersection(y.getCellInfo());
        matched_area.set(matched_area.get() +
            (double)intersection.width * intersection.height);
      }
    });
    
    // Estimate output file size of repartition based on the ratio of
    // matched area to smaller file area
    Rectangle smaller_file_mbr = FileMBR.fileMBRLocal(fs, files[1], stockShape);
    long estimatedRepartitionedFileSize = (long) (fStatus[0].getLen() *
        matched_area.get() / (smaller_file_mbr.width * smaller_file_mbr.height));
    LOG.info("Estimated repartitioned file size: "+estimatedRepartitionedFileSize);
    // Choose a good block size for repartitioned file to make every partition
    // fits in one block
    long blockSize = estimatedRepartitionedFileSize / cellSet.size();
    // Adjust blockSize to a multiple of bytes per checksum
    int bytes_per_checksum =
        new Configuration().getInt("io.bytes.per.checksum", 512);
    blockSize = (long) (Math.ceil((double)blockSize / bytes_per_checksum) *
        bytes_per_checksum);
    LOG.info("Using a block size of "+blockSize);
    
    // Repartition the smaller file
    Path partitioned_file;
    do {
      partitioned_file = new Path("/"+files[0].getName()+
          ".repartitioned_"+(int)(Math.random() * 1000000));
    } while (fs.exists(partitioned_file));
    // Repartition the smaller file with no local index
    Repartition.repartitionMapReduce(files[0], partitioned_file,
        stockShape, blockSize, cellSet.toArray(new CellInfo[cellSet.size()]),
        null, true);
    long t2 = System.currentTimeMillis();
    System.out.println("Repartition time "+(t2-t1)+" millis");
  
    // Continue with the join step
    if (fs.exists(partitioned_file)) {
      // An output file might not existent if the two files are disjoing

      // Replace the smaller file with its repartitioned copy
      files[0] = partitioned_file;
      // Delete temporary repartitioned file upon exit
      fs.deleteOnExit(partitioned_file);
    }
  }

  /**
   * Performs a redistribute join between the given files using the redistribute
   * join algorithm. Currently, we only support a pair of files.
   * @param fs
   * @param inputFiles
   * @param output
   * @return
   * @throws IOException 
   */
  public static <S extends Shape> long joinStep(FileSystem fs, Path[] inputFiles,
      Path userOutputPath,
      S stockShape, OutputCollector<S, S> output, boolean overwrite) throws IOException {
    long t1 = System.currentTimeMillis();

    JobConf job = new JobConf(DistributedJoin.class);
    
    FileSystem outFs = inputFiles[0].getFileSystem(job);
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path("/"+inputFiles[0].getName()+
            ".dj_"+(int)(Math.random() * 1000000));
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

    job.setJobName("DistributedJoin");
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setMapperClass(RedistributeJoinMap.class);
    job.setMapOutputKeyClass(stockShape.getClass());
    job.setMapOutputValueClass(stockShape.getClass());
    job.setBoolean(SpatialSite.AutoCombineSplits, true);
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
    job.setNumReduceTasks(0); // No reduce needed for this task

    job.setInputFormat(DJInputFormatArray.class);
    job.setClass(SpatialSite.FilterClass, SpatialJoinFilter.class, BlockFilter.class);
    job.set(SpatialSite.SHAPE_CLASS, stockShape.getClass().getName());
    job.setOutputFormat(TextOutputFormat.class);
    
    String commaSeparatedFiles = "";
    for (int i = 0; i < inputFiles.length; i++) {
      if (i > 0)
        commaSeparatedFiles += ',';
      commaSeparatedFiles += inputFiles[i].toUri().toString();
    }
    DJInputFormatArray.addInputPaths(job, commaSeparatedFiles);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();

    // Output number of running map tasks
    Counter mapTaskCountCounter = counters
        .findCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
    System.out.println("Number of map tasks "+mapTaskCountCounter.getValue());
    
    S s1 = stockShape;
    @SuppressWarnings("unchecked")
    S s2 = (S) stockShape.clone();
    // Read job result
    FileStatus[] results = outFs.listStatus(outputPath);
    for (FileStatus fileStatus : results) {
      if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
        if (output != null) {
          // Report every single result as a pair of shapes
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
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

    if (userOutputPath == null)
      outFs.delete(outputPath, true);
    long t2 = System.currentTimeMillis();
    System.out.println("Join time "+(t2-t1)+" millis");
    
    return resultCount;
  }
  
  /**
   * Spatially joins two files. 
   * @param fs
   * @param inputFiles
   * @param stockShape
   * @param output
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static long distributedJoinSmart(FileSystem fs, final Path[] inputFiles,
      Path userOutputPath,
      Shape stockShape,
      OutputCollector<Shape, Shape> output, boolean overwrite) throws IOException {
    Path[] originalInputFiles = inputFiles.clone();
    FileSystem outFs = inputFiles[0].getFileSystem(new Configuration());
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path("/"+inputFiles[0].getName()+
            ".dj_"+(int)(Math.random() * 1000000));
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
    
    // Decide whether to do a repartition step or not
    int cost_with_repartition, cost_without_repartition;
    final FileStatus[] fStatus = new FileStatus[inputFiles.length];
    for (int i_file = 0; i_file < inputFiles.length; i_file++) {
      fStatus[i_file] = fs.getFileStatus(inputFiles[i_file]);
    }
    
    // Sort files by length (size)
    IndexedSortable filesSortable = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Path tmp1 = inputFiles[i];
        inputFiles[i] = inputFiles[j];
        inputFiles[j] = tmp1;
        
        FileStatus tmp2 = fStatus[i];
        fStatus[i] = fStatus[j];
        fStatus[j] = tmp2;
      }
      
      @Override
      public int compare(int i, int j) {
        return fStatus[i].getLen() < fStatus[j].getLen() ? -1 : 1;
      }
    };
    
    new QuickSort().sort(filesSortable, 0, inputFiles.length);
    SimpleSpatialIndex<BlockLocation>[] gIndexes =
        new SimpleSpatialIndex[fStatus.length];
    for (int i_file = 0; i_file < fStatus.length; i_file++)
      gIndexes[i_file] = fs.getGlobalIndex(fStatus[i_file]);
    
    cost_without_repartition = SimpleSpatialIndex.spatialJoin(gIndexes[0],
        gIndexes[1], null);
    // Cost of repartition + cost of join
    cost_with_repartition = gIndexes[0].size() * 3 + gIndexes[1].size();
    LOG.info("Cost with repartition is estimated to "+cost_with_repartition);
    LOG.info("Cost without repartition is estimated to "+cost_without_repartition);
    boolean need_repartition = cost_with_repartition < cost_without_repartition;
    if (need_repartition) {
      repartitionStep(fs, inputFiles, stockShape);
    }
    
    // Restore inputFiles to the original order by user
    if (inputFiles[1] != originalInputFiles[1]) {
      Path temp = inputFiles[0];
      inputFiles[0] = inputFiles[1];
      inputFiles[1] = temp;
    }
    
    // Redistribute join the larger file and the partitioned file
    long result_size = DistributedJoin.joinStep(fs, inputFiles, outputPath, stockShape,
        output, overwrite);
    
    if (userOutputPath == null)
      outFs.delete(outputPath, true);

    return result_size;
  }
  
  private static void printUsage() {
    System.out.println("Performs a spatial join between two files using the distributed join algorithm");
    System.out.println("Parameters: (* marks the required parameters)");
    System.out.println("<input file 1> - (*) Path to the first input file");
    System.out.println("<input file 2> - (*) Path to the second input file");
    System.out.println("<output file> - Path to output file");
    System.out.println("-overwrite - Overwrite output file without notice");
  }

  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] allFiles = cla.getPaths();
    JobConf conf = new JobConf(DistributedJoin.class);
    Shape stockShape = cla.getShape(true);
    String repartition = cla.getRepartition();
    if (allFiles.length < 2) {
      printUsage();
      throw new RuntimeException("Missing input files");
    }
    
    Path[] inputFiles = new Path[] {allFiles[0], allFiles[1]};
    FileSystem fs = allFiles[0].getFileSystem(conf);
    if (!fs.exists(inputFiles[0]) || !fs.exists(inputFiles[1])) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }
    
    
    Path outputPath = allFiles.length > 2 ? allFiles[2] : null;
    boolean overwrite = cla.isOverwrite();

    long result_size;
    if (repartition == null || repartition.equals("auto")) {
      result_size = distributedJoinSmart(fs, inputFiles, outputPath, stockShape, null, overwrite);
    } else if (repartition.equals("yes")) {
      repartitionStep(fs, allFiles, stockShape);
      result_size = joinStep(fs, inputFiles, outputPath, stockShape, null, overwrite);
    } else if (repartition.equals("no")) {
      result_size = joinStep(fs, inputFiles, outputPath, stockShape, null, overwrite);
    } else {
      throw new RuntimeException("Illegal parameter repartition:"+repartition);
    }
    
    System.out.println("Result size: "+result_size);
  }
}
