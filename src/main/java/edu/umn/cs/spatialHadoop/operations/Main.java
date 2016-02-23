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
import edu.umn.cs.spatialHadoop.indexing.Indexer;
import edu.umn.cs.spatialHadoop.nasa.AggregateQuadTree;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot;
import edu.umn.cs.spatialHadoop.nasa.HDFToText;
import edu.umn.cs.spatialHadoop.nasa.MultiHDFPlot;
import edu.umn.cs.spatialHadoop.nasa.ShahedServer;
import edu.umn.cs.spatialHadoop.nasa.SpatioAggregateQueries;
import edu.umn.cs.spatialHadoop.visualization.GeometricPlot;
import edu.umn.cs.spatialHadoop.visualization.HadoopvizServer;
import edu.umn.cs.spatialHadoop.visualization.HeatMapPlot;
import edu.umn.cs.spatialHadoop.visualization.LakesPlot;
import edu.umn.cs.spatialHadoop.visualization.MagickPlot;
import edu.umn.cs.spatialHadoop.delaunay.DelaunayTriangulation;


/**
 * The main entry point to all queries.
 * 
 * @author Ahmed Eldawy
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
      
      pgd.addClass("index", Indexer.class,
          "Spatially index a file using a specific indexer");
      
      pgd.addClass("oldindex", Repartition.class,
          "Spatially index a file using a specific indexer");
      
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

      pgd.addClass("uunion", UltimateUnion.class,
          "Computes the union of input shapes using the UltimateUnion algorithm");

      pgd.addClass("delaunay", DelaunayTriangulation.class,
          "Computes the Delaunay triangulation for a set of points");

      pgd.addClass("multihdfplot", MultiHDFPlot.class,
          "Plots NASA datasets in the spatiotemporal range provided by user");

      pgd.addClass("hdfplot", HDFPlot.class,
          "Plots a heat map for a give NASA dataset");
      
      pgd.addClass("gplot", GeometricPlot.class,
          "Plots a file to an image");

      pgd.addClass("hplot", HeatMapPlot.class,
          "Plots a heat map to an image");
      
      pgd.addClass("lakesplot", LakesPlot.class,
          "Plots lakes to SVG image");
      
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

      pgd.addClass("staggquery", SpatioAggregateQueries.class,
          "Runs a spatio temporal aggregate query on HDF files");
      
      pgd.addClass("shahedindexer", AggregateQuadTree.class,
          "Creates a multilevel spatio-temporal indexer for NASA data");
      
      pgd.addClass("hadoopviz", HadoopvizServer.class,
          "Run Hadoopviz Server");
      
      pgd.addClass("mplot", MagickPlot.class, "Plot using ImageMagick");
      
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
