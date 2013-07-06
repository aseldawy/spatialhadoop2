package edu.umn.cs.spatialHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.RTreeGridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.ShapeRecordWriter;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RTreeGridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomInputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomShapeGenerator;
import edu.umn.cs.spatialHadoop.mapred.RandomShapeGenerator.DistributionType;
import edu.umn.cs.spatialHadoop.operations.Repartition;



/**
 * Generates a random file of rectangles or points based on some user
 * parameters
 * @author Ahmed Eldawy
 *
 */
public class RandomSpatialGenerator {
  
  public static void generateMapReduce(Path file, Rectangle mbr, long size,
      long blocksize, Shape shape,
      String gindex, String lindex, long seed, int rectsize,
      RandomShapeGenerator.DistributionType type, boolean overwrite) throws IOException {
    JobConf job = new JobConf(RandomSpatialGenerator.class);
    
    job.setJobName("Generator");
    FileSystem outFs = file.getFileSystem(job);
    
    // Overwrite output file
    if (outFs.exists(file)) {
      if (overwrite)
        outFs.delete(file, true);
      else
        throw new RuntimeException("Output file '" + file
            + "' already exists and overwrite flag is not set");
    }

    // Set generation parameters in job
    job.setLong(RandomShapeGenerator.GenerationSize, size);
    SpatialSite.setRectangle(job, RandomShapeGenerator.GenerationMBR, mbr);
    if (seed != 0)
      job.setLong(RandomShapeGenerator.GenerationSeed, seed);
    if (rectsize != 0)
      job.setInt(RandomShapeGenerator.GenerationRectSize, rectsize);
    if (type != null)
      job.set(RandomShapeGenerator.GenerationType, type.toString());

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    // Set input format and map class
    job.setInputFormat(RandomInputFormat.class);
    job.setMapperClass(Repartition.RepartitionMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
    
    SpatialSite.setShapeClass(job, shape.getClass());
    
    if (blocksize != 0) {
      job.setLong(SpatialSite.LOCAL_INDEX_BLOCK_SIZE, blocksize);
    }
    
    CellInfo[] cells;
    if (gindex == null) {
      cells = new CellInfo[] {new CellInfo(1, mbr)};
    } else {
      GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
      FileSystem fs = file.getFileSystem(job);
      if (blocksize == 0) {
        blocksize = fs.getDefaultBlockSize(file);
      }
      int numOfCells = Repartition.calculateNumberOfPartitions(job, size, fs, file, blocksize);
      gridInfo.calculateCellDimensions(numOfCells);
      cells = gridInfo.getAllCells();
    }
    
    SpatialSite.setCells(job, cells);
    
    // Do not set a reduce function. Use the default identity reduce function
    job.setNumReduceTasks(Math.max(1, Math.min(cells.length,
        (clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));
    

    // Set output path
    FileOutputFormat.setOutputPath(job, file);
    if (lindex == null) {
      job.setOutputFormat(GridOutputFormat.class);
    } else if (lindex.equals("rtree")) {
      // For now, the two types of local index are the same
      job.setOutputFormat(RTreeGridOutputFormat.class);
    } else {
      throw new RuntimeException("Unsupported local index: "+lindex);
    }
    
    JobClient.runJob(job);

  }
  

  
  /**
   * Generates random rectangles and write the result to a file.
   * @param outFS - The file system that contains the output file
   * @param outputFile - The file name to write to. If either outFS or
   *   outputFile is null, data is generated to the standard output
   * @param mbr - The whole MBR to generate in
   * @param shape 
   * @param totalSize - The total size of the generated file
   * @param blocksize 
   * @throws IOException 
   */
  public static void generateFileLocal(Path outFile,
      Shape shape, String gindex, String lindex, long totalSize, Rectangle mbr,
      DistributionType type, int rectSize, long seed, long blocksize,
      boolean overwrite) throws IOException {
    FileSystem outFS = outFile.getFileSystem(new Configuration());
    if (blocksize == 0)
      blocksize = outFS.getDefaultBlockSize(outFile);
    
    // Calculate the dimensions of each partition based on gindex type
    CellInfo[] cells;
    if (gindex == null) {
      throw new RuntimeException("Unsupported global index: "+gindex);
    } else if (gindex.equals("grid")) {
      int num_partitions = Repartition.calculateNumberOfPartitions(new Configuration(),
          totalSize, outFS, outFile, blocksize);

      GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
      gridInfo.calculateCellDimensions(num_partitions);
      cells = gridInfo.getAllCells();
    } else {
      throw new RuntimeException("Unsupported global index: "+gindex);
    }
    
    
    // Overwrite output file
    if (outFS.exists(outFile)) {
      if (overwrite)
        outFS.delete(outFile, true);
      else
        throw new RuntimeException("Output file '" + outFile
            + "' already exists and overwrite flag is not set");
    }
    outFS.mkdirs(outFile);

    ShapeRecordWriter<Shape> writer;
    if (lindex == null) {
      writer = new GridRecordWriter<Shape>(outFile, null, null, cells, gindex.equals("rtree"));
    } else if (lindex.equals("grid") || lindex.equals("rtree")) {
      writer = new RTreeGridRecordWriter<Shape>(outFile, null, null, cells, gindex.equals("rtree"));
      writer.setStockObject(shape);
    } else {
      throw new RuntimeException("Unupoorted local idnex: "+lindex);
    }

    if (rectSize == 0)
      rectSize = 100;
    long t1 = System.currentTimeMillis();
    
    RandomShapeGenerator<Shape> generator = new RandomShapeGenerator<Shape>(
        totalSize, mbr, type, rectSize, seed);
    
    Rectangle key = generator.createKey();
    
    while (generator.next(key, shape)) {
      // Serialize it to text
      writer.write(NullWritable.get(), shape);
    }
    long t2 = System.currentTimeMillis();
    
    System.out.println("Generation time: "+(t2-t1)+" millis");
  }

  private static void printUsage() {
    System.out.println("Generates a file with random shapes");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<output file> - Path to the file to generate. If omitted, file is generated to stdout.");
    System.out.println("mbr:<x,y,w,h> - (*) The MBR of the generated data. Originated at (x,y) with dimensions (w,h)");
    System.out.println("shape:<point|(rectangle)|polygon> - Type of shapes in generated file");
    System.out.println("blocksize:<size> - Block size in the generated file");
    System.out.println("global:<grid|rtree> - Type of global index in generated file");
    System.out.println("local:<grid|rtree> - Type of local index in generated file");
    System.out.println("seed:<s> - Use a specific seed to generate the file");
    System.out.println("-overwrite - Overwrite output file without notice");
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Rectangle mbr = cla.getRectangle();
    if (mbr == null) {
      printUsage();
      throw new RuntimeException("Set MBR of the generated file using rect:<x,y,w,h>");
    }

    Path outputFile = cla.getPath();
    Shape stockShape = cla.getShape(false);
    long blocksize = cla.getBlockSize();
    int rectSize = cla.getRectSize();
    long seed = cla.getSeed();
    if (stockShape == null)
      stockShape = new Rectangle();
    
    long totalSize = cla.getSize();
    String gindex = cla.getGIndex();
    String lindex = cla.getLIndex();
    boolean overwrite = cla.isOverwrite();
    
    DistributionType type = DistributionType.UNIFORM;
    String strType = cla.get("type");
    if (strType != null) {
      strType = strType.toLowerCase();
      if (strType.startsWith("uni"))
        type = DistributionType.UNIFORM;
      else if (strType.startsWith("gaus"))
        type = DistributionType.GAUSSIAN;
      else if (strType.startsWith("cor"))
        type = DistributionType.CORRELATED;
      else if (strType.startsWith("anti"))
        type = DistributionType.ANTI_CORRELATED;
      else if (strType.startsWith("circle"))
        type = DistributionType.CIRCLE;
      else {
        System.err.println("Unknown distribution type: "+cla.get("type"));
        printUsage();
        return;
      }
    }

    if (outputFile != null) {
      System.out.print("Generating a file ");
      System.out.print("with gindex:"+gindex+" ");
      System.out.print("with lindex:"+lindex+" ");
      System.out.println("file of size: "+totalSize);
      System.out.println("To: " + outputFile);
      System.out.println("In the range: " + mbr);
    }

    if (totalSize < 100*1024*1024)
      generateFileLocal(outputFile, stockShape, gindex, lindex, totalSize, mbr, type, rectSize, seed, blocksize, overwrite);
    else
    generateMapReduce(outputFile, mbr, totalSize, blocksize, stockShape,
        gindex, lindex, seed, rectSize, type, overwrite);
//    if (gindex == null && lindex == null)
//      generateHeapFile(fs, outputFile, stockShape, totalSize, mbr, type, rectSize, seed, blocksize, overwrite);
//    else
//      generateGridFile(fs, outputFile, stockShape, totalSize, mbr, type, rectSize, seed, blocksize, gindex, lindex, overwrite);
  }

}
  