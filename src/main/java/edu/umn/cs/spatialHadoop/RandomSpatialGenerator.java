/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.ShapeRecordWriter;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomInputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomShapeGenerator;
import edu.umn.cs.spatialHadoop.mapred.RandomShapeGenerator.DistributionType;
import edu.umn.cs.spatialHadoop.operations.Repartition;
import edu.umn.cs.spatialHadoop.operations.Repartition.RepartitionReduce;



/**
 * Generates a random file of rectangles or points based on some user
 * parameters
 * @author Ahmed Eldawy
 *
 */
public class RandomSpatialGenerator {
  
  private static void generateMapReduce(Path outFile, OperationsParams params)
      throws IOException{
    JobConf job = new JobConf(params, RandomSpatialGenerator.class);
    job.setJobName("Generator");
    Shape shape = params.getShape("shape");
    
    FileSystem outFs = outFile.getFileSystem(job);

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    // Set input format and map class
    job.setInputFormat(RandomInputFormat.class);
    job.setMapperClass(Repartition.RepartitionMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
    
    String sindex = params.get("sindex");
    Rectangle mbr = params.getShape("mbr").getMBR();
    
    CellInfo[] cells;
    if (sindex == null) {
      cells = new CellInfo[] {new CellInfo(1, mbr)};
    } else if (sindex.equals("grid")) {
      GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
      FileSystem fs = outFile.getFileSystem(job);
      long blocksize = fs.getDefaultBlockSize(outFile);
      long size = params.getSize("size");
      int numOfCells = Repartition.calculateNumberOfPartitions(job, size, fs, outFile, blocksize);
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
    FileOutputFormat.setOutputPath(job, outFile);
    if (sindex == null || sindex.equals("grid")) {
      job.setOutputFormat(GridOutputFormat.class);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    JobClient.runJob(job);
    
    // TODO move the following part to OutputCommitter
    // Concatenate all master files into one file
    FileStatus[] resultFiles = outFs.listStatus(outFile, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().contains("_master");
      }
    });
    String ext = resultFiles[0].getPath().getName()
        .substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
    Path masterPath = new Path(outFile, "_master" + ext);
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
  private static void generateFileLocal(Path outFile, OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, RandomSpatialGenerator.class);
    FileSystem outFS = outFile.getFileSystem(params);
    long blocksize = outFS.getDefaultBlockSize(outFile);
    String sindex = params.get("sindex");
    Rectangle mbr = params.getShape("mbr").getMBR();
    long totalSize = params.getSize("size");
    
    // Calculate the dimensions of each partition based on gindex type
    CellInfo[] cells;
    if (sindex == null) {
      cells = new CellInfo[] {new CellInfo(1, mbr)};
    } else if (sindex.equals("grid")) {
      int num_partitions = Repartition.calculateNumberOfPartitions(params,
          totalSize, outFS, outFile, blocksize);

      GridInfo gridInfo = new GridInfo(mbr.x1, mbr.y1, mbr.x2, mbr.y2);
      gridInfo.calculateCellDimensions(num_partitions);
      cells = gridInfo.getAllCells();
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    outFS.mkdirs(outFile);

    ShapeRecordWriter<Shape> writer;
    if (sindex == null || sindex.equals("grid")) {
      writer = new GridRecordWriter<Shape>(outFile, job, null, cells);
    } else {
      throw new RuntimeException("Unupoorted spatial idnex: "+sindex);
    }

    int rectSize = params.getInt("rectsize", 100);
    long seed = params.getLong("seed", System.currentTimeMillis());
    float circleThickness = params.getFloat("thickness", 1);
    DistributionType type = SpatialSite.getDistributionType(params, "type",
        DistributionType.UNIFORM);
    Shape shape = params.getShape("shape");
    long t1 = System.currentTimeMillis();
    
    RandomShapeGenerator<Shape> generator = new RandomShapeGenerator<Shape>(
        totalSize, mbr, type, rectSize, seed, circleThickness);
    
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
    System.out.println("mbr:<x1,y1,x2,y2> - (*) The MBR of the generated data. Originated at (x,y) with dimensions (w,h)");
    System.out.println("shape:<point|(rectangle)> - Type of shapes in generated file");
    System.out.println("sindex:<grid> - Type of global index in generated file. The only supported index is 'grid'");
    System.out.println("seed:<s> - Use a specific seed to generate the file");
    System.out.println("rectsize:<rs> - Maximum edge size for generated rectangles");
    System.out.println("-overwrite - Overwrite output file without notice");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  /**
   * @param args Command line arguments
   * @throws IOException If an exception happens during the underlying MapReduce job 
   */
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    if (params.get("mbr") == null) {
      System.err.println("Set MBR of the generated file using rect:<x1,y1,x2,y2>");
      printUsage();
      System.exit(1);
    }
    
    if (params.get("shape") == null) {
      System.err.println("Shape should be specified");
      printUsage();
      System.exit(1);
    }
    
    if (!params.checkOutput()) {
      printUsage();
      System.exit(1);
    }

    Path outputFile = params.getPath();
    
    long totalSize = params.getSize("size");

    if (totalSize < 100*1024*1024)
      generateFileLocal(outputFile, params);
    else
      generateMapReduce(outputFile, params);
  }

}
  