/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ProgramDriver;

import edu.umn.cs.spatialHadoop.RandomSpatialGenerator;
import edu.umn.cs.spatialHadoop.ReadFile;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot2;
import edu.umn.cs.spatialHadoop.nasa.HDFToText;
import edu.umn.cs.spatialHadoop.nasa.MakeHDFVideo;
import edu.umn.cs.spatialHadoop.nasa.ShahedServer;
import edu.umn.cs.spatialHadoop.nasa.SpatioTemporalAggregateQuery;

/**
 * The main entry point to all queries.
 * 
 * @author eldawy
 *
 */
public class Main {
  
  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
  public static void main(String[] args) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("rangequery", RangeQuery.class,
          "Finds all objects in the query range given by a rectangle");

      pgd.addClass("knn", KNN.class,
          "Finds the k nearest neighbor in a file to a point");

      pgd.addClass("dj", DistributedJoin.class,
          "Computes the spatial join between two input files using the " +
          "distributed join algorithm");
      
      pgd.addClass("sjmr", SJMR.class,
          "Computes the spatial join between two input files using the " +
          "SJMR algorithm");
      
      pgd.addClass("index", Repartition.class,
          "Builds an index on an input file");

      pgd.addClass("partition", Indexer.class,
          "Spatially partition a file using a specific partitioner");
      
      pgd.addClass("mbr", FileMBR.class,
          "Finds the minimal bounding rectangle of an input file");
      
      pgd.addClass("readfile", ReadFile.class,
          "Retrieve some information about the index of a file");

      pgd.addClass("sample", Sampler.class,
          "Reads a random sample from the input file");

      pgd.addClass("generate", RandomSpatialGenerator.class,
          "Generates a random file containing spatial data");

      pgd.addClass("union", Union.class,
          "Computes the union of input shapes");

      pgd.addClass("hdfplot", HDFPlot.class,
          "Plots NASA datasets in the spatiotemporal range provided by user");

      pgd.addClass("hdfplot2", HDFPlot2.class,
          "Plots NASA datasets in the spatiotemporal range provided by user");
      
      pgd.addClass("makevideo", MakeHDFVideo.class,
          "Creates a video out of a set of HDF files");

      pgd.addClass("gplot", GeometricPlot.class,
          "Plots a file to an image");

      pgd.addClass("hplot", HeatMapPlot.class,
          "Plots a heat map to an image");
      
      pgd.addClass("hdfx", HDFToText.class,
          "Extracts data from a set of HDF files to text files");

      pgd.addClass("skyline", Skyline.class,
          "Computes the skyline of an input set of points");
      
      pgd.addClass("convexhull", ConvexHull.class,
          "Computes the convex hull of an input set of points");

      pgd.addClass("farthestpair", FarthestPair.class,
          "Computes the farthest pair of point of an input set of points");

      pgd.addClass("closestpair", ClosestPair.class,
          "Computes the closest pair of point of an input set of points");

      pgd.addClass("distcp", DistributedCopy.class,
          "Copies a directory or file using a MapReduce job");
      
      pgd.addClass("vizserver", ShahedServer.class,
          "Starts a server that handles visualization requests");

      pgd.addClass("staggquery", SpatioTemporalAggregateQuery.class,
          "Runs a spatio temporal aggregate query on HDF files");
      
      pgd.driver(args);
      
      // Success
      exitCode = 0;
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
