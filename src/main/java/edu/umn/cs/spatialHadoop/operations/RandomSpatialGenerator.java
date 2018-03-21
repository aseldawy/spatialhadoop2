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

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.indexing.*;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.ShapeRecordWriter;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapreduce.RandomInputFormat;
import edu.umn.cs.spatialHadoop.mapreduce.RandomShapeGenerator;
import edu.umn.cs.spatialHadoop.mapreduce.RandomShapeGenerator.DistributionType;



/**
 * Generates a random file of rectangles or points based on some user
 * parameters
 * @author Ahmed Eldawy
 *
 */
public class RandomSpatialGenerator {
  
  private static Job generateMapReduce(Path outFile, OperationsParams params)
      throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(params, "Generator");
    job.setJarByClass(RandomSpatialGenerator.class);

    Shape shape = params.getShape("shape");
    FileSystem outFs = outFile.getFileSystem(params);

    ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
    // Set input format and map class
    job.setInputFormatClass(RandomInputFormat.class);
    job.setMapperClass(Indexer.PartitionerMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(shape.getClass());

    String sindex = params.get("sindex");
    Rectangle mbr = params.getShape("mbr").getMBR();
    
    CellInfo[] cells;
    if (sindex == null) {
      cells = new CellInfo[] {new CellInfo(1, mbr)};
    } else if (sindex.equals("grid")) {
      FileSystem fs = outFile.getFileSystem(params);
      long blocksize = fs.getDefaultBlockSize(outFile);
      long size = params.getSize("size");
      int numOfCells = (int) Math.ceil((float)size / blocksize);
      GridPartitioner grid = new GridPartitioner(mbr, numOfCells);
      cells = new CellInfo[grid.getPartitionCount()];
      for (int i = 0; i < cells.length; i++) {
        cells[i] = grid.getPartitionAt(i).clone();
      }
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    CellPartitioner p = new CellPartitioner(cells);
    Partitioner.setPartitioner(job.getConfiguration(), p);
    
    // Do not set a reduce function. Use the default identity reduce function
    if (cells.length == 1) {
      // All objects are in one partition. No need for a reduce phase
      job.setNumReduceTasks(0);
    } else {
      // More than one partition. Need a reduce phase to group shapes of the
      // same partition together
      job.setReducerClass(Indexer.PartitionerReduce.class);
      job.setNumReduceTasks(Math.max(1, Math.min(cells.length,
          (clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));
    }
    
    // Set output path
    IndexOutputFormat.setOutputPath(job, outFile);
    if (sindex == null || sindex.equals("grid")) {
      job.setOutputFormatClass(IndexOutputFormat.class);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }

    job.waitForCompletion(false);

    return job;
  }
  
  /**
   * Generates random rectangles and write the result to a file.
   * @param outFile The path of the file to create
   * @param params the job parameters
   * @throws IOException 
   */
  private static void generateFileLocal(Path outFile, OperationsParams params) throws IOException, InterruptedException {
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
      int num_partitions = (int) Math.ceil((float) totalSize / blocksize);
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
    long t1 = System.currentTimeMillis();
    
    RandomShapeGenerator<Shape> generator = new RandomShapeGenerator<Shape>();
    generator.initialize(totalSize, mbr, type, rectSize, seed, circleThickness);
    generator.setShape(params.getShape("shape"));
    
    while (generator.nextKeyValue()) {
      // Serialize it to text
      writer.write(NullWritable.get(), generator.getCurrentValue());
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
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
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
  