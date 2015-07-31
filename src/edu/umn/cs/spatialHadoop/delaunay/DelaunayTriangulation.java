/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.delaunay;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;

/**
 * Computes the Delaunay triangulation (DT) for a set of points.
 * @author Ahmed Eldawy
 * 
 */
public class DelaunayTriangulation {

  /**Logger to write log messages for this class*/
  static final Log LOG = LogFactory.getLog(DelaunayTriangulation.class);
  
  /**
   * The map function computes the DT for a partition and splits the
   * triangulation into safe and non-safe edges. Safe edges are final and are
   * written to the output, while non-safe edges can be modified and are sent to
   * the reduce function.
   * 
   * @param <S> - The type of shape for sites
   */
  public static class DelaunayMap<S extends Point>
    extends Mapper<Rectangle, Iterable<S>, NullWritable, Triangulation> {
    
    /**Whether the map function should remove duplicates in the input or not*/
    private boolean deduplicate;
    /**Threshold used in duplicate detection*/
    private float threshold;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      this.deduplicate = conf.getBoolean("dedup", true);
      if (deduplicate)
        this.threshold = conf.getFloat("threshold", 1E-5f);
    }
    
    @Override
    protected void map(Rectangle key, Iterable<S> values, Context context)
        throws IOException, InterruptedException {
      List<S> sites = new Vector<S>();
      for (S site : values)
        sites.add((S) site.clone());
      
      if (deduplicate) {
        LOG.info("Removing duplicates from "+sites.size()+" points");
        // Remove duplicates to ensure correctness
        Collections.sort(sites);
        
        int i = 1;
        while (i < sites.size()) {
          S s1 = sites.get(i-1);
          S s2 = sites.get(i);
          if (Math.abs(s1.x - s2.x) < threshold &&
              Math.abs(s1.y - s2.y) < threshold)
            sites.remove(i);
          else
            i++;
        }
        LOG.info("Duplicates removed and only "+sites.size()+" points left");
      }
      
      context.setStatus("Computing DT");
      GuibasStolfiDelaunayAlgorithm algo = new GuibasStolfiDelaunayAlgorithm(
          sites.toArray(new Point[sites.size()]), context);
      if (key.isValid()) {
        context.setStatus("Splitting DT");
        Triangulation finalPart = new Triangulation();
        Triangulation nonfinalPart = new Triangulation();
        algo.splitIntoFinalAndNonFinalParts(key, finalPart, nonfinalPart);
        // TODO write final part to the output path
        context.write(NullWritable.get(), nonfinalPart);
      } else {
        context.setStatus("Writing DT");
        context.write(NullWritable.get(), algo.getFinalAnswer());
      }
    }
  }

  public static class DelaunayReduce
  extends Reducer<NullWritable, Triangulation, NullWritable, Triangulation> {
    
    @Override
    protected void reduce(NullWritable dummy, Iterable<Triangulation> values,
        Context context) throws IOException, InterruptedException {
      List<List<Triangulation>> columns = new Vector<List<Triangulation>>();
      
      // Arrange triangulations column-by-column
      for (Triangulation t : values) {
        double x1 = t.mbr.x1, x2 = t.mbr.x2;
        List<Triangulation> selectedColumn = null;
        int iColumn = 0;
        while (iColumn < columns.size() && selectedColumn == null) {
          Rectangle cmbr = columns.get(iColumn).get(0).mbr;
          double cx1 = cmbr.x1;
          double cx2 = cmbr.x2;
          if (x2 > cx1 && cx2 > x1) {
            selectedColumn = columns.get(iColumn);
          }
        }
        
        if (selectedColumn == null) {
          // Create a new column
          selectedColumn = new Vector<Triangulation>();
          columns.add(selectedColumn);
        }
        selectedColumn.add(t);
      }
      
      List<Triangulation> mergedColumns = new Vector<Triangulation>();
      // Merge all triangulations together column-by-column
      for (List<Triangulation> column : columns) {
        // Sort this column by y-axis
        Collections.sort(column, new Comparator<Triangulation>() {
          @Override
          public int compare(Triangulation t1, Triangulation t2) {
            double dy = t1.mbr.y1 - t2.mbr.y1;
            if (dy < 0)
              return -1;
            if (dy > 0)
              return 1;
            return 0;
          }
        });

        GuibasStolfiDelaunayAlgorithm algo =
            new GuibasStolfiDelaunayAlgorithm(column.toArray(new Triangulation[column.size()]), context);
        mergedColumns.add(algo.getFinalAnswer());
      }
      
      // Merge the result horizontally
      Collections.sort(mergedColumns, new Comparator<Triangulation>() {
        @Override
        public int compare(Triangulation t1, Triangulation t2) {
          double dx = t1.mbr.x1 - t2.mbr.x1;
          if (dx < 0)
            return -1;
          if (dx > 0)
            return 1;
          return 0;
        }
      });
      GuibasStolfiDelaunayAlgorithm algo = new GuibasStolfiDelaunayAlgorithm(
          mergedColumns.toArray(new Triangulation[mergedColumns.size()]),
          context);
      
      context.setStatus("Writing DT");
      context.write(NullWritable.get(), algo.getFinalAnswer());
    }
  }

  /**
   * Run the DT algorithm in MapReduce
   * @param inPaths
   * @param outPath
   * @param params
   * @return
   * @throws IOException 
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   */
  public static Job delaunayMapReduce(Path[] inPaths, Path outPath, OperationsParams params) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(params, "Delaunay Triangulation");
    job.setJarByClass(DelaunayTriangulation.class);

    // Set map and reduce
    job.setMapperClass(DelaunayMap.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Triangulation.class);
    job.setReducerClass(DelaunayReduce.class);
    ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks() * 9 / 10));

    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inPaths);

    job.setOutputFormatClass(TextOutputFormat3.class);
    TextOutputFormat.setOutputPath(job, outPath);

    // Submit the job
    if (!params.getBoolean("background", false)) {
      job.waitForCompletion(false);
      if (!job.isSuccessful())
        throw new RuntimeException("Job failed!");
    } else {
      job.submit();
    }
    return job;
  }

  /**
   * Compute the Deluanay triangulation in the local machine
   * @param inPaths
   * @param outPath
   * @param params
   * @throws IOException
   * @throws InterruptedException
   */
  public static <P extends Point> void delaunayLocal(Path[] inPaths, Path outPath,
      OperationsParams params) throws IOException, InterruptedException {
    // 1- Split the input path/file to get splits that can be processed
    // independently
    final SpatialInputFormat3<Rectangle, P> inputFormat =
        new SpatialInputFormat3<Rectangle, P>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inPaths);
    final List<InputSplit> splits = inputFormat.getSplits(job);
    
    // 2- Read all input points in memory
    List<P> points = new Vector<P>();
    for (InputSplit split : splits) {
      FileSplit fsplit = (FileSplit) split;
      final RecordReader<Rectangle, Iterable<P>> reader =
          inputFormat.createRecordReader(fsplit, null);
      if (reader instanceof SpatialRecordReader3) {
        ((SpatialRecordReader3)reader).initialize(fsplit, params);
      } else if (reader instanceof RTreeRecordReader3) {
        ((RTreeRecordReader3)reader).initialize(fsplit, params);
      } else if (reader instanceof HDFRecordReader) {
        ((HDFRecordReader)reader).initialize(fsplit, params);
      } else {
        throw new RuntimeException("Unknown record reader");
      }
      while (reader.nextKeyValue()) {
        Iterable<P> pts = reader.getCurrentValue();
        for (P p : pts) {
          points.add((P) p.clone());
        }
      }
      reader.close();
    }
    
    if (params.getBoolean("dedup", true)) {
      // Remove duplicates to ensure correctness
      final float threshold = params.getFloat("threshold", 1E-5f);
      Collections.sort(points, new Comparator<P>() {
        @Override
        public int compare(P p1, P p2) {
          double dx = p1.x - p2.x;
          if (dx < 0)
            return -1;
          if (dx > 0)
            return 1;
          double dy = p1.y - p2.y;
          if (dy < 0)
            return -1;
          if (dy > 0)
            return 1;
          return 0;
        }
      });
      
      int i = 1;
      while (i < points.size()) {
        P p1 = points.get(i-1);
        P p2 = points.get(i);
        double dx = Math.abs(p1.x - p2.x);
        double dy = Math.abs(p1.y - p2.y);
        if (dx < threshold && dy < threshold)
          points.remove(i);
        else
          i++;
      }
    }
    
    LOG.info("Read "+points.size()+" points and computing DT");
    GuibasStolfiDelaunayAlgorithm dtAlgorithm = new GuibasStolfiDelaunayAlgorithm(points.toArray(
        (P[]) Array.newInstance(points.get(0).getClass(), points.size())), null);
    dtAlgorithm.getFinalAnswer().draw();
    /*Triangulation finalPart = new Triangulation();
    Triangulation nonfinalPart = new Triangulation();
    dtAlgorithm.splitIntoFinalAndNonFinalParts(new Rectangle(-180, -90, 180, 90), finalPart, nonfinalPart);
    System.out.println("group {");
    finalPart.draw();
    System.out.println("}");
    System.out.println("group {");
    nonfinalPart.draw();
    System.out.println("}");*/
  }

  /**
   * Compute the DT for an input file that contains points. If the params
   * contains an explicit execution option (local or MapReduce), it is used.
   * Otherwise, this method automatically uses a single-machine (local)
   * algorithm or a MapReduce algorithm depending on the input file size.
   * 
   * @param inFiles
   * @param outFile
   * @param params
   * @return
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public static Job delaunay(Path[] inFiles, Path outFile,
      final OperationsParams params) throws IOException, InterruptedException,
      ClassNotFoundException {
    if (OperationsParams.isLocal(params, inFiles)) {
      delaunayLocal(inFiles, outFile, params);
      return null;
    } else {
      return delaunayMapReduce(inFiles, outFile, params);
    }
  }
  
  private static void printUsage() {
    System.out.println("Delaunay Triangulation");
    System.out.println("Computes the delaunay triangulation of a set of points.");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to file that contains all shapes");
    System.out.println("<output file>: (*) Path to output file");
    System.out.println("shape:<s> - Type of shapes stored in the input file");
    System.out.println("-dup - Automatically remove duplicates in the input");
    System.out.println("-local - Implement a local machine algorithm (no MapReduce)");
  }

  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    OperationsParams params = new OperationsParams(parser);
    
    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path[] inFiles = params.getInputPaths();
    Path outFile = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    delaunay(inFiles, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: " + (t2 - t1) + " millis");
  }
}
