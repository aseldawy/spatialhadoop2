package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.spatial.BinaryRecordReader;
import org.apache.hadoop.mapred.spatial.BinarySpatialInputFormat;
import org.apache.hadoop.mapred.spatial.BlockFilter;
import org.apache.hadoop.mapred.spatial.DefaultBlockFilter;
import org.apache.hadoop.mapred.spatial.PairWritable;
import org.apache.hadoop.mapred.spatial.RTreeRecordReader;
import org.apache.hadoop.mapred.spatial.ShapeArrayRecordReader;
import org.apache.hadoop.mapred.spatial.TextOutputFormat;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GlobalIndex;
import org.apache.hadoop.spatial.Partition;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.ResultCollector2;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialAlgorithms;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.CommandLineArguments;

/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 * @author Ahmed Eldawy
 *
 */
public class DistributedJoin {
  private static final Log LOG = LogFactory.getLog(DistributedJoin.class);

  public static class SpatialJoinFilter extends DefaultBlockFilter {
    @Override
    public void selectCellPairs(GlobalIndex<Partition> gIndex1,
        GlobalIndex<Partition> gIndex2,
        ResultCollector2<Partition, Partition> output) {
      // Do a spatial join between the two global indexes
      GlobalIndex.spatialJoin(gIndex1, gIndex2, output);
    }
  }
  
  public static class RedistributeJoinMap extends MapReduceBase
  implements Mapper<PairWritable<Rectangle>, PairWritable<? extends Writable>, Shape, Shape> {
    
    public void map(
        final PairWritable<Rectangle> key,
        final PairWritable<? extends Writable> value,
        final OutputCollector<Shape, Shape> output,
        Reporter reporter) throws IOException {
      final Rectangle mapperMBR = !key.first.isValid()
          && !key.second.isValid() ? null // Both blocks are heap blocks
          : (!key.first.isValid() ? key.second // Second block is indexed
              : (!key.second.isValid() ? key.first // First block is indexed
                  : (key.first.getIntersection(key.second)))); // Both indexed

      if (value.first instanceof ArrayWritable && value.second instanceof ArrayWritable) {
        // Join two arrays using the plane sweep algorithm
        if (mapperMBR != null) {
          // Only join shapes in the intersection rectangle
          ArrayList<Shape> r = new ArrayList<Shape>();
          ArrayList<Shape> s = new ArrayList<Shape>();
          for (Shape shape : (Shape[])((ArrayWritable) value.first).get()) {
            if (mapperMBR.isIntersected(shape))
              r.add(shape);
          }
          for (Shape shape : (Shape[])((ArrayWritable) value.second).get()) {
            if (mapperMBR.isIntersected(shape))
              s.add(shape);
          }
          SpatialAlgorithms.SpatialJoin_planeSweep(r, s, new ResultCollector2<Shape, Shape>() {
            @Override
            public void collect(Shape r, Shape s) {
              try {
                double intersectionX = Math.max(r.getMBR().x1, s.getMBR().x1);
                double intersectionY = Math.max(r.getMBR().y1, s.getMBR().y1);
                // Employ reference point duplicate avoidance technique 
                if (mapperMBR.contains(intersectionX, intersectionY))
                  output.collect(r, s);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          });
        } else {
          ArrayWritable ar1 = (ArrayWritable) value.first;
          ArrayWritable ar2 = (ArrayWritable) value.second;
          SpatialAlgorithms.SpatialJoin_planeSweep(
              (Shape[])ar1.get(), (Shape[])ar2.get(),
              new ResultCollector2<Shape, Shape>() {
                @Override
                public void collect(Shape x, Shape y) {
                  try {
                    // No need to do reference point technique because input
                    // blocks are not indexed (mapperMBR is null)
                    output.collect(x, y);
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                }
              }
              );
        }
      } else if (value.first instanceof RTree && value.second instanceof RTree) {
        // Join two R-trees
        @SuppressWarnings("unchecked")
        RTree<Shape> r1 = (RTree<Shape>) value.first;
        @SuppressWarnings("unchecked")
        RTree<Shape> r2 = (RTree<Shape>) value.second;
        RTree.spatialJoin(r1, r2, new ResultCollector2<Shape, Shape>() {
          @Override
          public void collect(Shape r, Shape s) {
            try {
              if (mapperMBR == null) {
                output.collect(r, s);
              } else {
                // Reference point duplicate avoidance technique
                // The reference point is the lowest corner of the intersection
                // rectangle (the point with the least dimensions of both x and
                // y in the intersection rectangle)
                double intersectionX = Math.max(r.getMBR().x1, s.getMBR().x1);
                double intersectionY = Math.max(r.getMBR().y1, s.getMBR().y1);
                if (mapperMBR.contains(intersectionX, intersectionY))
                  output.collect(r, s);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
      } else {
        throw new RuntimeException("Cannot join " + value.first.getClass()
            + " with " + value.second.getClass());
      }
    }
  }
  

  /**
   * Input format that returns a record reader that reads a pair of arrays of
   * shapes
   * @author eldawy
   *
   */
  public static class DJInputFormatArray extends BinarySpatialInputFormat<Rectangle, ArrayWritable> {

    /**
     * Reads a pair of arrays of shapes
     * @author Ahmed Eldawy
     *
     */
    public static class DJRecordReader extends BinaryRecordReader<Rectangle, ArrayWritable> {
      public DJRecordReader(Configuration conf, CombineFileSplit fileSplits) throws IOException {
        super(conf, fileSplits);
      }
      
      @Override
      protected RecordReader<Rectangle, ArrayWritable> createRecordReader(
          Configuration conf, CombineFileSplit split, int i) throws IOException {
        FileSplit fsplit = new FileSplit(split.getPath(i),
            split.getStartOffsets()[i],
            split.getLength(i), split.getLocations());
        return new ShapeArrayRecordReader(conf, fsplit);
      }
    }

    @Override
    public RecordReader<PairWritable<Rectangle>, PairWritable<ArrayWritable>> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new DJRecordReader(job, (CombineFileSplit)split);
    }
  }


  /**
   * Input format that returns a record reader that reads a pair of arrays of
   * shapes
   * @author Ahmed Eldawy
   *
   */
  public static class DJInputFormatRTree<S extends Shape> extends BinarySpatialInputFormat<Rectangle, RTree<S>> {

    /**
     * Reads a pair of arrays of shapes
     * @author Ahmed Eldawy
     *
     */
    public static class DJRecordReader<S extends Shape> extends BinaryRecordReader<Rectangle, RTree<S>> {
      public DJRecordReader(Configuration conf, CombineFileSplit fileSplits) throws IOException {
        super(conf, fileSplits);
      }
      
      @Override
      protected RecordReader<Rectangle, RTree<S>> createRecordReader(
          Configuration conf, CombineFileSplit split, int i) throws IOException {
        FileSplit fsplit = new FileSplit(split.getPath(i),
            split.getStartOffsets()[i],
            split.getLength(i), split.getLocations());
        return new RTreeRecordReader<S>(conf, fsplit);
      }
    }

    @Override
    public RecordReader<PairWritable<Rectangle>, PairWritable<RTree<S>>> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new DJRecordReader<S>(job, (CombineFileSplit)split);
    }
  }

  /**
   * Select a file to repartition based on some heuristics.
   * If only one file is indexed, the non-indexed file is repartitioned.
   * If both files are indexed, the smaller file is repartitioned.
   * @return the index in the given array of the file to be repartitioned.
   *   -1 if all files are non-indexed
   * @throws IOException 
   */
  protected static int selectRepartition(FileSystem fs, final Path[] files) throws IOException {
    int largest_partitioned_file = -1;
    long largest_size = 0;
    
    for (int i = 0; i < files.length; i++) {
      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, files[i]);
      if (gindex != null) {
        // Compute total size (all files in directory)
        long total_size = 0;
        for (Partition p : gindex) {
          Path file = new Path(files[i], p.filename);
          total_size += fs.getFileStatus(file).getLen();
        }
        if (total_size > largest_size) {
          largest_partitioned_file = i;
          largest_size = total_size;
        }
      }
    }
    return largest_partitioned_file == -1 ? -1 : 1 - largest_partitioned_file;
  }
  
  /**
   * Repartition a file to match the partitioning of the other file.
   * @param fs
   * @param files
   * @param stockShape
   * @param fStatus
   * @param gIndexes
   * @throws IOException
   */
  protected static void repartitionStep(FileSystem fs, final Path[] files,
      int file_to_repartition, Shape stockShape) throws IOException {
    
    // Do the repartition step
    long t1 = System.currentTimeMillis();
  
    // Repartition the smaller file
    Path partitioned_file;
    do {
      partitioned_file = new Path(files[file_to_repartition].getName()+
          ".repartitioned_"+(int)(Math.random() * 1000000));
    } while (fs.exists(partitioned_file));
    
    // Get the cells to use for repartitioning
    CellInfo[] cells = SpatialSite.cellsOf(fs, files[1-file_to_repartition]);
    // Repartition the file to match the other file
    Repartition.repartitionMapReduce(files[file_to_repartition],
        partitioned_file, stockShape, 0, cells, null, SpatialSite.isRTree(fs,
            files[1 - file_to_repartition]) ? "rtree" : null, true);
    long t2 = System.currentTimeMillis();
    System.out.println("Repartition time "+(t2-t1)+" millis");
  
    // Continue with the join step
    if (fs.exists(partitioned_file)) {
      // An output file might not existent if the two files are disjoint

      // Replace the smaller file with its repartitioned copy
      files[file_to_repartition] = partitioned_file;

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
  public static <S extends Shape> long joinStep(FileSystem fs,
      Path[] inputFiles, Path userOutputPath, S stockShape, boolean overwrite)
      throws IOException {
    long t1 = System.currentTimeMillis();

    JobConf job = new JobConf(DistributedJoin.class);
    
    FileSystem outFs = inputFiles[0].getFileSystem(job);
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path(inputFiles[0].getName()+
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
    LOG.info("Joining "+inputFiles[0]+" X "+inputFiles[1]);
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
  public static long distributedJoinSmart(FileSystem fs,
      final Path[] inputFiles, Path userOutputPath, Shape stockShape,
      boolean overwrite) throws IOException {
    Path[] originalInputFiles = inputFiles.clone();
    FileSystem outFs = inputFiles[0].getFileSystem(new Configuration());
    Path outputPath = userOutputPath;
    if (outputPath == null) {
      do {
        outputPath = new Path(inputFiles[0].getName()+
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
    GlobalIndex<Partition>[] gIndexes = new GlobalIndex[fStatus.length];
    int[] numBlocks = new int[fStatus.length];
    for (int i_file = 0; i_file < fStatus.length; i_file++) {
      gIndexes[i_file] = SpatialSite.getGlobalIndex(outFs, fStatus[i_file].getPath());
      numBlocks[i_file] = outFs.getFileBlockLocations(fStatus[i_file], 0, fStatus[i_file].getLen()).length;
    }
    
    cost_without_repartition = gIndexes[0] != null && gIndexes[1] != null ?
        GlobalIndex.spatialJoin(gIndexes[0], gIndexes[1], null) :
        (numBlocks[0] * numBlocks[1]);
    // Total cost = Cost of repartition (2 * numBlocks[0]) +
    //    cost of join (numBlocks[0] + numBlocks[1])
    cost_with_repartition = numBlocks[0] * 3 + numBlocks[1];
    LOG.info("Cost with repartition is estimated to "+cost_with_repartition);
    LOG.info("Cost without repartition is estimated to "+cost_without_repartition);
    boolean need_repartition = cost_with_repartition < cost_without_repartition;
    if (need_repartition) {
      int file_to_repartition = selectRepartition(fs, inputFiles);
      repartitionStep(fs, inputFiles, file_to_repartition, stockShape);
    }
    
    // Restore inputFiles to the original order by user
    if (inputFiles[1] != originalInputFiles[1]) {
      Path temp = inputFiles[0];
      inputFiles[0] = inputFiles[1];
      inputFiles[1] = temp;
    }
    
    // Redistribute join the larger file and the partitioned file
    long result_size = DistributedJoin.joinStep(fs, inputFiles, outputPath, stockShape,
        overwrite);
    
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
      result_size = distributedJoinSmart(fs, inputFiles, outputPath, stockShape, overwrite);
    } else if (repartition.equals("yes")) {
      int file_to_repartition = selectRepartition(fs, inputFiles);
      repartitionStep(fs, inputFiles, file_to_repartition, stockShape);
      result_size = joinStep(fs, inputFiles, outputPath, stockShape, overwrite);
    } else if (repartition.equals("no")) {
      result_size = joinStep(fs, inputFiles, outputPath, stockShape, overwrite);
    } else {
      throw new RuntimeException("Illegal parameter repartition:"+repartition);
    }
    
    System.out.println("Result size: "+result_size);
  }
}
