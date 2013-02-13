package edu.umn.cs.spatialHadoop.operations;

import org.apache.hadoop.util.ProgramDriver;

import edu.umn.cs.spatialHadoop.RandomSpatialGenerator;
import edu.umn.cs.spatialHadoop.ReadFile;

/**
 * The main entry point to all queries.
 * @author eldawy
 *
 */
public class Main {
  
  public static void main(String[] args) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      
      pgd.addClass("rangequery", RangeQuery.class,
          "Finds all objects in the query range given by rect:x1,y1,w,h");

      pgd.addClass("knn", KNN.class,
          "Finds the k nearest neighbor in a file to a point");
      pgd.addClass(
          "dj",
          DistributedJoin.class,
          "Computes the spatial join between two input files using the " +
          "distributed join algorithm");
      pgd.addClass("sjmr", SJMR.class,
          "Computes the spatial join between two input files using the " +
          "SJMR algorithm");
      pgd.addClass("index", Repartition.class,
          "Builds an index on an input file");
      pgd.addClass("mbr", FileMBR.class,
          "Finds the minimal bounding rectangle of an input file");
      pgd.addClass("readfile", ReadFile.class,
          "Retrieve some information about the global index of a file");

      pgd.addClass("sample", Sampler.class,
          "Reads a random sample from the input file");

      pgd.addClass("generate", RandomSpatialGenerator.class,
          "Generates a random file containing spatial data");
      
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
