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

package edu.umn.cs.spatialHadoop.nasa;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.operations.Aggregate;
import edu.umn.cs.spatialHadoop.operations.Aggregate.MinMax;
import edu.umn.cs.spatialHadoop.operations.GeometricPlot;
import edu.umn.cs.spatialHadoop.operations.PyramidPlot;

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
  
  /**
   * Download all HDF files in the given spatio-temporal range
   * @param input
   * @param range
   * @param from
   * @param to
   * @throws IOException 
   */
  public static void HDFDownloader(Path input, Path output, Shape range,
      final Date from, final Date to) throws IOException {
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
    // Retrieve all matching input directories based on date range
    Configuration conf = new Configuration();
    final FileSystem inFs = input.getFileSystem(conf);
    FileStatus[] matchingDirs = inFs.listStatus(input, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        String dirName = p.getName();
        try {
          Date date = dateFormat.parse(dirName);
          return date.compareTo(from) >= 0 &&
              date.compareTo(to) <= 0;
        } catch (ParseException e) {
          LOG.warn("Cannot parse directory name: "+dirName);
          return false;
        }
      }
    });
    
    final FileSystem outFs = output.getFileSystem(conf);
    for (FileStatus matchingDir : matchingDirs) {
      final Path matchingPath = matchingDir.getPath();
      final Path downloadPath = new Path(output, matchingPath.getName());
      outFs.mkdirs(downloadPath);
      
      GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(inFs, matchingPath);
      if (gindex == null)
        throw new RuntimeException("Cannot retrieve global index for "+matchingPath);
      if (range != null) {

      gindex.rangeQuery(range, new ResultCollector<Partition>() {
        @Override
        public void collect(Partition r) {
          // Download only HDF files
          if (!r.filename.toLowerCase().endsWith(".hdf"))
            return;
          Path filePath = new Path(matchingPath, r.filename);
          try {
            Path downloadFile = new Path(downloadPath, r.filename);
            if (!outFs.exists(downloadFile)) {
              Path tempFile = new Path(File.createTempFile(r.filename, "downloaded").getAbsolutePath());
              inFs.copyToLocalFile(filePath, tempFile);
              outFs.moveFromLocalFile(tempFile, downloadFile);
            }
          } catch (IOException e) {
            throw new RuntimeException("Error downloading file '"+filePath+"'", e);
          }
        }
      });
      } else {
        for (Partition r : gindex) {
          if (!r.filename.toLowerCase().endsWith(".hdf"))
            continue;
          Path filePath = new Path(matchingPath, r.filename);
          try {
            Path downloadFile = new Path(downloadPath, r.filename);
            if (!outFs.exists(downloadFile)) {
              Path tempFile = new Path(File.createTempFile(r.filename, "downloaded").getAbsolutePath());
              inFs.copyToLocalFile(filePath, tempFile);
              outFs.moveFromLocalFile(tempFile, downloadFile);
            }
          } catch (IOException e) {
            throw new RuntimeException("Error downloading file '"+filePath+"'", e);
          }

        }
      }
    }
  }

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
   */
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path input = params.getPaths()[0];
    Path output = params.getPaths()[1];
    String timeRange = params.get("time");
    if (timeRange == null) {
      printUsage();
    }
    final Date dateFrom, dateTo;
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
    try {
      dateFrom = dateFormat.parse(timeRange.split("\\.\\.")[0]);
      dateTo = dateFormat.parse(timeRange.split("\\.\\.")[1]);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.error("Use the seperator two periods '..' to seperate from and to dates");
      printUsage();
      return;
    } catch (ParseException e) {
      LOG.error("Illegal date format in "+timeRange);
      printUsage();
      return;
    }
    // Retrieve all matching input directories based on date range
    Configuration conf = new Configuration();
    FileSystem inFs = input.getFileSystem(conf);
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
    if (matchingDirs.length == 0) {
      LOG.warn("No matching directories to given input");
      return;
    }
    Path[] matchingPaths = new Path[matchingDirs.length];
    for (int i = 0; i < matchingDirs.length; i++)
      matchingPaths[i] = new Path(matchingDirs[i].getPath(), "*.hdf");
    
    // Retrieve range to plot if provided by user
    Rectangle plotRange = (Rectangle) params.getShape("rect");
    
    if (params.is("download-only")) {
      HDFDownloader(input, output, plotRange, dateFrom, dateTo);
      return;
    }

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
    
    Vector<String> vargs = new Vector<String>(Arrays.asList(args));
    // Keep all arguments except input and output which change for each call
    // to Plot or PlotPyramid
    for (int i = 0; i < vargs.size();) {
      if (vargs.get(i).startsWith("-") && vargs.get(i).length() > 1) {
        i++; // Skip
      } else if (vargs.get(i).indexOf(':') != -1 && vargs.get(i).indexOf(":/") == -1) {
        if (vargs.get(i).toLowerCase().startsWith("scale:"))
          vargs.remove(i);
        else
          i++; // Skip
      } else {
        vargs.remove(i);
      }
    }

    boolean overwrite = params.is("overwrite");
    boolean pyramid = params.is("pyramid");
    FileSystem outFs = output.getFileSystem(conf);
    Vector<RunningJob> jobs = new Vector<RunningJob>();
    boolean background = params.is("background");
    for (Path inputPath : matchingPaths) {
      Path outputPath = new Path(output+"/"+inputPath.getParent().getName()+
          (pyramid? "" : ".png"));
      if (overwrite || !outFs.exists(outputPath)) {
        String[] plotArgs = vargs.toArray(new String[vargs.size() + (valueRange == null? 2 : 3)]);
        plotArgs[vargs.size()] = inputPath.toString();
        plotArgs[vargs.size() + 1] = outputPath.toString();
        if (valueRange != null)
          plotArgs[vargs.size() + 2] =
            "valuerange:"+valueRange.minValue+","+valueRange.maxValue;
        if (pyramid) {
          PyramidPlot.main(plotArgs);
          if (background)
            jobs.add(GeometricPlot.lastSubmittedJob);
        } else {
          GeometricPlot.main(plotArgs);
          if (background)
            jobs.add(GeometricPlot.lastSubmittedJob);
        }
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
