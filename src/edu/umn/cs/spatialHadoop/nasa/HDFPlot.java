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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.operations.PlotPyramid;

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
  /**Milliseconds in one day*/
  private static final long ONE_DAY = 24l * 60 * 60 * 1000;

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
    CommandLineArguments cla = new CommandLineArguments(args);
    Vector<String> vargs = new Vector<String>(Arrays.asList(args));
    Path input = cla.getPaths()[0];
    Path output = cla.getPaths()[1];
    // Keep all arguments except input and output which are dynamic
    vargs.remove(input.toString());
    vargs.remove(output.toString());
    String timeRange = cla.get("time");
    if (timeRange == null) {
      printUsage();
    }
    Date dateFrom, dateTo;
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
    boolean overwrite = cla.isOverwrite();
    Configuration conf = new Configuration();
    FileSystem outFs = output.getFileSystem(conf);
    while (dateFrom.compareTo(dateTo) <= 0) {
      Path inputPath = new Path(input+"/"+dateFormat.format(dateFrom)+"/*.hdf");
      Path outputPath = new Path(output+"/"+dateFormat.format(dateFrom));
      if (overwrite || !outFs.exists(outputPath)) {
        String[] plotArgs = vargs.toArray(new String[vargs.size() + 2]);
        plotArgs[vargs.size()] = inputPath.toString();
        plotArgs[vargs.size() + 1] = outputPath.toString();
        PlotPyramid.main(plotArgs);
      }
      // Advance one day
      dateFrom = new Date(dateFrom.getTime() + ONE_DAY);
    }
    
  }

}
