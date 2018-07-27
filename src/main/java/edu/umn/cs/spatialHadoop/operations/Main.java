/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import edu.umn.cs.spatialHadoop.core.SpatialSite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ProgramDriver;

import edu.umn.cs.spatialHadoop.ReadFile;
import edu.umn.cs.spatialHadoop.indexing.Indexer;
import edu.umn.cs.spatialHadoop.nasa.AggregateQuadTree;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot;
import edu.umn.cs.spatialHadoop.nasa.HDFToText;
import edu.umn.cs.spatialHadoop.nasa.MultiHDFPlot;
import edu.umn.cs.spatialHadoop.nasa.ShahedServer;
import edu.umn.cs.spatialHadoop.visualization.GeometricPlot;
import edu.umn.cs.spatialHadoop.visualization.HadoopvizServer;
import edu.umn.cs.spatialHadoop.visualization.HeatMapPlot;
import edu.umn.cs.spatialHadoop.visualization.LakesPlot;
import edu.umn.cs.spatialHadoop.visualization.MagickPlot;
import edu.umn.cs.spatialHadoop.delaunay.DelaunayTriangulation;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;


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
      // Add classes from a configuration file
      Yaml yaml = new Yaml();
      List<String> ops = yaml.load(SpatialSite.class.getResourceAsStream("/spatial-operations.yaml"));
      for (String op : ops) {
        Class<?> opClass = Class.forName(op);
        OperationMetadata opMetadata = opClass.getAnnotation(OperationMetadata.class);
        pgd.addClass(opMetadata.shortName(), opClass, opMetadata.description());
      }

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
