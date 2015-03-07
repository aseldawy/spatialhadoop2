/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.operations.Aggregate;
import edu.umn.cs.spatialHadoop.operations.Aggregate.MinMax;

/**
 * Plots all datasets from NASA satisfying the following criteria:
 * 1- Spatial criteria: Falls in the spatial range provided in 'rect' parameter
 * 2- Time critieria: Falls the in the time range provided in 'time' parameter
 * 3- Dataset: Selects the exact dataset given in 'dataset' parameter
 * If overwrite flag is set, all datasets are plotted and overwritten if exists.
 * If the flag is not set, only time instances that are not generated are
 * generated.
 * @author Ahmed Eldawy
 *
 */
public class HDFPlot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(HDFPlot.class);

  public static MinMax lastRange;
  
  private static void printUsage() {
    System.out.println("Plots all NASA datasets matching user criteria");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to NASA repository of all datasets");
    System.out.println("<output file> - (*) Path to output images");
    System.out.println("shape:<point|rectangle|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("tilewidth:<w> - Width of each tile in pixels");
    System.out.println("tileheight:<h> - Height of each tile in pixels");
    System.out.println("color:<c> - Main color used to draw shapes (black)");
    System.out.println("numlevels:<n> - Number of levels in the pyrmaid");
    System.out.println("dataset:<d> - Dataset to plot from HDF files");
    System.out.println("time:<from..to> - Time range each formatted as yyyy.mm.dd");
    System.out.println("-overwrite: Override output file without notice");
    System.out.println("rect:<x1,y1,x2,y2> - Limit drawing to the selected area");
    System.out.println("-vflip: Vertically flip generated image to correct +ve Y-axis direction");
  }

  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] input = params.getInputPaths();
    Path output = params.getOutputPath();
    String timeRange = params.get("time");
    if (timeRange == null) {
      System.err.println("timerange must be specified");
      printUsage();
      System.exit(1);
    }
    final Date dateFrom, dateTo;
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
    try {
      dateFrom = dateFormat.parse(timeRange.split("\\.\\.")[0]);
      dateTo = dateFormat.parse(timeRange.split("\\.\\.")[1]);
    } catch (ArrayIndexOutOfBoundsException e) {
      System.err.println("Use the seperator two periods '..' to seperate from and to dates");
      printUsage();
      System.exit(1);
      return; // To avoid an error that causes dateFrom to be uninitialized
    } catch (ParseException e) {
      System.err.println("Illegal date format in "+timeRange);
      printUsage();
      System.exit(1);
      return;
    }
    // Retrieve all matching input directories based on date range
    Configuration conf = new Configuration();
    Vector<Path> matchingPathsV = new Vector<Path>();
    for (Path inputFile : input) {
      FileSystem inFs = inputFile.getFileSystem(conf);
      FileStatus[] matchingDirs = inFs.listStatus(input, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          String dirName = p.getName();
          try {
            Date date = dateFormat.parse(dirName);
            return date.compareTo(dateFrom) >= 0 &&
                date.compareTo(dateTo) <= 0;
          } catch (ParseException e) {
            LOG.warn("Cannot parse directory name: "+dirName);
            return false;
          }
        }
      });
      for (FileStatus matchingDir : matchingDirs)
        matchingPathsV.add(new Path(matchingDir.getPath(), "*.hdf"));
    }
    if (matchingPathsV.size() == 0) {
      LOG.warn("No matching directories to given input");
      return;
    }
    
    Path[] matchingPaths = matchingPathsV.toArray(new Path[matchingPathsV.size()]);
    
    // Retrieve the scale
    MinMax valueRange = null;
    if (params.get("valuerange") == null) {
      String scale = params.get("scale", "preset");
      if (scale.equals("preset") || scale.equals("tight")) {
        params.setBoolean("force", true);
        valueRange = Aggregate.aggregate(matchingPaths, params);
      } else if (scale.matches("\\d+,\\d+")) {
        String[] parts = scale.split(",");
        valueRange = new MinMax(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
      } else {
        valueRange = Aggregate.aggregate(matchingPaths, params);
      }
      lastRange = valueRange;
    }

    params.clearAllPaths();
    
    boolean overwrite = params.is("overwrite");
    boolean pyramid = params.is("pyramid");
    FileSystem outFs = output.getFileSystem(conf);
    Vector<Job> jobs = new Vector<Job>();
    boolean background = params.is("background");
    for (Path inputPath : matchingPaths) {
      Path outputPath = new Path(output+"/"+inputPath.getParent().getName()+
          (pyramid? "" : ".png"));
      if (overwrite || !outFs.exists(outputPath)) {
        // Need to plot
        Job rj = HDFPlot2.plotHeatMap(new Path[] {inputPath}, outputPath, params);
        if (background)
          jobs.add(rj);
      }
    }
    while (!jobs.isEmpty()) {
      int i_job = 0;
      int size_before = jobs.size();
      while (i_job < jobs.size()) {
        if (jobs.get(i_job).isComplete())
          jobs.remove(i_job);
        else
          i_job++;
      }
      if (jobs.size() != size_before) {
        LOG.info(jobs.size()+" plot jobs remaining");
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
