package edu.umn.cs.spatialHadoop.operations;

import edu.umn.cs.spatialHadoop.RandomSpatialGenerator;

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
                   "Finds all objects in the query range given by rect:x1,y1,w,h",
                   "<path to input file>",
                   "rect:<x,y,w,h> - The query range given by a rectangle");

      pgd.addClass("knn", KNN.class, 
                   "Finds the k nearest neighbor in a file to a point",
                   "<path to input file>",
                   "k:<k> - number of neighbors to return",
                   "point:<x,y> - the query point");
      pgd.addClass("dj", DistributedJoin.class,
                   "Computes the spatial join between two input files using the distributed join algorithm",
                   "<path to first input file>",
                   "<path to second input file>",
                   "repartition:<yes|no|auto> - Whether to repartition the smaller file or not");
      pgd.addClass("sjmr", SJMR.class,
                   "Computes the spatial join between two input files using the SJMR algorithm",
                   "<path to first input file>",
                   "<path to second input file>",
                   "rect:<x,y,w,h> - (Optional) The MBR of the two files to join.",
                   "                 If not set, automatically obtained by computing the MBR of the two files");
      pgd.addClass("index", Repartition.class,
                   "Builds an index on an input file",
                   "<path to input file> - Path to the unindexed file",
                   "<path to output file> - Path to write the indexed file",
                   "rect:<x,y,w,h> - (Optional) MBR of the input file",
                   "                 If not set, automatically obtained by computing the MBR of the two files",
                   "global:<grid|rtree> - Type of the global index to use",
                   "local:<grid|rtree> - (Optional) Type of the local index to use",
                   "shape:<point|rectangle> - Type of the shape stored in input file",
                   "-overwrite - Overwrite output files without notice");
      pgd.addClass("mbr", FileMBR.class,
                   "Finds the minimal bounding rectangle of an input file",
                   "<path to input file>",
                   "shape:<point|rectangle> - Type of the shape stored in input file");

      pgd.addClass("sample", Sampler.class,
                   "Reads a random sample from the input file",
                   "<path to input file>",
                   "count:<n> - Approximate number of samples to read",
                   "ratio:<r> - Approximate ratio of the input file to sample 0<r<1",
                   "size:<s> - Approximate size (in bytes) of the input file to sample"); 

      pgd.addClass("generate", RandomSpatialGenerator.class,
                   "Generates a random file containing spatial data",
                   "<path to output file>",
                   "size:<s> - Approximate size of the output file, e.g., 100.kb, 10.mb, 4.gb",
                   "rect:<x,y,w,h> - The area to generate in",
                   "rectsize:<l> - Maximum length of an edge for generated rectangles",
                   "shape:<point|rectangle> - Type of the shape to generate",
                   "blocksize:<B> - Size of the block in the generated file",
                   "global:<grid|rtree> - (Optional) Type of the global index in generated file",
                   "local:<grid|rtree> - (Optional) Type of the local index in generated file",
                   "-overwrite - Overwrite output file without notice");
      
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
