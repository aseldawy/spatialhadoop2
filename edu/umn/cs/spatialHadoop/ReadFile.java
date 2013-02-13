package edu.umn.cs.spatialHadoop;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.spatial.CellInfo;

/**
 * Reads spatial information associated with a file
 * @author eldawy
 *
 */
public class ReadFile {

  private static void printUsage() {
    System.out.println("Displays information about blocks in a file");
    System.out.println("Parameters:");
    System.out.println("<input file> - Path to input file");
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      printUsage();
      throw new RuntimeException("Illegal parameters");
    }
    Configuration conf = new Configuration();
    Path inFile = new Path(args[0]);
    FileSystem fs = inFile.getFileSystem(conf);
    
    Map<CellInfo, Integer> blocks_per_cell = new HashMap<CellInfo, Integer>();
    int heap_blocks = 0;
    
    long length = fs.getFileStatus(inFile).getLen();
    
    BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fs.getFileStatus(inFile), 0, length);
    for (BlockLocation blk : fileBlockLocations) {
      if (blk.getCellInfo() != null) {
        if (blocks_per_cell.containsKey(blk.getCellInfo())) {
          int count = blocks_per_cell.get(blk.getCellInfo());
          blocks_per_cell.put(blk.getCellInfo(), count + 1);
        } else {
          blocks_per_cell.put(blk.getCellInfo(), 1);
        }
      } else {
        heap_blocks++;
      }
    }
    if (blocks_per_cell.isEmpty()) {
      System.out.println("No spatial blocks");
    } else {
      for (Map.Entry<CellInfo, Integer> map_entry : blocks_per_cell.entrySet()) {
        System.out.println(map_entry.getKey()+" -- "+map_entry.getValue());
      }
    }
    if (heap_blocks == 0) {
      System.out.println("No heap blocks");
    } else {
      System.out.println(heap_blocks+" heap blocks");
    }
    
  }
}
