package edu.umn.cs.spatialHadoop;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

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
    CommandLineArguments cla = new CommandLineArguments(args);
    Path input = cla.getPath();
    if (input == null) {
      printUsage();
      throw new RuntimeException("Illegal parameters");
    }
    Configuration conf = new Configuration();
    Path inFile = new Path(args[0]);
    FileSystem fs = inFile.getFileSystem(conf);
    
    long length = fs.getFileStatus(inFile).getLen();
    
    BlockLocation[] locations = cla.getOffset() == -1 ?
        fs.getFileBlockLocations(fs.getFileStatus(inFile), 0, length) :
        fs.getFileBlockLocations(fs.getFileStatus(inFile), cla.getOffset(), 1);
    Arrays.sort(locations, new Comparator<BlockLocation>() {
      @Override
      public int compare(BlockLocation o1, BlockLocation o2) {
        if (o1.getCellInfo() == null && o2.getCellInfo() != null) {
          return -1;
        }
        if (o1.getCellInfo() != null && o2.getCellInfo() == null) {
          return 1;
        }
        if (o1.getCellInfo() == null && o2.getCellInfo() == null) {
          return o1.getOffset() < o2.getOffset() ? -1 : 1;
        }
        if (o1.getCellInfo() != null && o2.getCellInfo() != null) {
          return o1.getCellInfo().compareTo(o2.getCellInfo());
        }
        return 0;
      }
    });
    
    for (int i = 0; i < locations.length; i++) {
      if (i == 0 || !locations[i].equals(locations[i-1])) {
        System.out.println(locations[i].getCellInfo() == null?
            "Heap file" :
            locations[i].getCellInfo());
      }
      System.out.println("   ["+locations[i].getOffset()+","+locations[i].getLength()+"]");
    }
  }
}
