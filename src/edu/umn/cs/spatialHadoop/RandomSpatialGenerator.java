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
package edu.umn.cs.spatialHadoop;

import java.awt.Color;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.ShapeRecordWriter;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomInputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomShapeGenerator;
import edu.umn.cs.spatialHadoop.mapred.RandomShapeGenerator.DistributionType;
import edu.umn.cs.spatialHadoop.operations.Plot;
import edu.umn.cs.spatialHadoop.operations.Repartition;
import edu.umn.cs.spatialHadoop.operations.Repartition.RepartitionReduce;



/**
 * Generates a random file of rectangles or points based on some user
 * parameters
 * @author Ahmed Eldawy
 *
 */
public class RandomSpatialGenerator {
  
  public static void generateMapReduce(Path file, Rectangle mbr, long size,
      long blocksize, Shape shape,
      String sindex, long seed, int rectsize,
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
    if (sindex == null) {
      cells = new CellInfo[] {new CellInfo(1, mbr)};
    } else if (sindex.equals("grid")) {
      GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
      FileSystem fs = file.getFileSystem(job);
      if (blocksize == 0) {
        blocksize = fs.getDefaultBlockSize(file);
      }
      int numOfCells = Repartition.calculateNumberOfPartitions(job, size, fs, file, blocksize);
      gridInfo.calculateCellDimensions(numOfCells);
      cells = gridInfo.getAllCells();
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    SpatialSite.setCells(job, cells);
    
    // Do not set a reduce function. Use the default identity reduce function
    if (cells.length == 1) {
      // All objects are in one partition. No need for a reduce phase
      job.setNumReduceTasks(0);
    } else {
      // More than one partition. Need a reduce phase to group shapes of the
      // same partition together
      job.setReducerClass(RepartitionReduce.class);
      job.setNumReduceTasks(Math.max(1, Math.min(cells.length,
          (clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));
    }
    
    // Set output path
    FileOutputFormat.setOutputPath(job, file);
    if (sindex == null || sindex.equals("grid")) {
      job.setOutputFormat(GridOutputFormat.class);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    JobClient.runJob(job);
    
    // Concatenate all master files into one file
    FileStatus[] resultFiles = outFs.listStatus(file, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().contains("_master");
      }
    });
    String ext = resultFiles[0].getPath().getName()
        .substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
    Path masterPath = new Path(file, "_master" + ext);
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
    
    // Plot an image for the partitions used in file
    Path imagePath = new Path(file, "_partitions.png");
    int imageSize = (int) (Math.sqrt(cells.length) * 300);
    Plot.plotLocal(masterPath, imagePath, new Partition(), imageSize, imageSize, false, Color.BLACK, false, false, false);
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
      Shape shape, String sindex, long totalSize, Rectangle mbr,
      DistributionType type, int rectSize, long seed, long blocksize,
      boolean overwrite) throws IOException {
    FileSystem outFS = outFile.getFileSystem(new Configuration());
    if (blocksize == 0)
      blocksize = outFS.getDefaultBlockSize(outFile);
    
    // Calculate the dimensions of each partition based on gindex type
    CellInfo[] cells;
    if (sindex == null) {
      cells = new CellInfo[] {new CellInfo(1, mbr)};
    } else if (sindex.equals("grid")) {
      int num_partitions = Repartition.calculateNumberOfPartitions(new Configuration(),
          totalSize, outFS, outFile, blocksize);

      GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
      gridInfo.calculateCellDimensions(num_partitions);
      cells = gridInfo.getAllCells();
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
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
    if (sindex == null || sindex.equals("grid")) {
      writer = new GridRecordWriter<Shape>(outFile, null, null, cells, false, false);
    } else {
      throw new RuntimeException("Unupoorted spatial idnex: "+sindex);
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
    writer.close(null);
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
    String sindex = cla.get("sindex");
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
      System.out.print("with sindex:"+sindex+" ");
      System.out.println("file of size: "+totalSize);
      System.out.println("To: " + outputFile);
      System.out.println("In the range: " + mbr);
    }

    if (totalSize < 100*1024*1024)
      generateFileLocal(outputFile, stockShape, sindex, totalSize, mbr, type, rectSize, seed, blocksize, overwrite);
    else
    generateMapReduce(outputFile, mbr, totalSize, blocksize, stockShape,
        sindex, seed, rectSize, type, overwrite);
//    if (gindex == null && lindex == null)
//      generateHeapFile(fs, outputFile, stockShape, totalSize, mbr, type, rectSize, seed, blocksize, overwrite);
//    else
//      generateGridFile(fs, outputFile, stockShape, totalSize, mbr, type, rectSize, seed, blocksize, gindex, lindex, overwrite);
  }

}
  