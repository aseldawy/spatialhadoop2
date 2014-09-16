/**
 * 
 */
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.temporal.TemporalIndex;
import edu.umn.cs.spatialHadoop.temporal.TemporalIndex.TemporalPartition;

/**
 * @author Ahmed Eldawy
 *
 */
public class SpatioTemporalSelectionQuery {
  
  public static class TimeRange {
    /**Date format of time range*/
    static final SimpleDateFormat DateFormat = new SimpleDateFormat("yyyy.MM.dd");
    
    /**Regular expression for a time range*/
    static final Pattern TimeRange = Pattern.compile("^(\\d{4}\\.\\d{2}\\.\\d{2})\\.\\.(\\d{4}\\.\\d{2}\\.\\d{2})$");

    /**Start time (inclusive)*/
    public long start;
    /**End time (exclusive)*/
    public long end;
    
    public TimeRange(String str) throws ParseException {
      Matcher matcher = TimeRange.matcher(str);
      if (!matcher.matches())
        throw new RuntimeException("Illegal time range '"+str+"'");
      start = DateFormat.parse(matcher.group(1)).getTime();
      end = DateFormat.parse(matcher.group(2)).getTime();
    }
    
    public TimeRange(long start, long end) {
      this.start = start;
      this.end = end;
    }
  }


  /**
   * Performs a spatio-temporal aggregate query on an indexed directory
   * @param inFile
   * @param params
   * @throws ParseException 
   * @throws IOException 
   */
  public static void aggregateQuery(Path inFile, OperationsParams params) throws ParseException, IOException {
    // 1- Run a temporal filter step to find all matching temporal partitions
    Vector<Path> matchingPartitions = new Vector<Path>();
    // List of time ranges to check. Initially it contains one range as
    // specified by the user. Eventually, it can be split into at most two
    // partitions if partially matched by a partition.
    Vector<TimeRange> temporalRanges = new Vector<TimeRange>();
    temporalRanges.add(new TimeRange(params.get("time")));
    Path[] temporalIndexes = new Path[] {
      new Path(inFile, "yearly"),
      new Path(inFile, "monthly"),
      new Path(inFile, "daily")
    };
    int index = 0;
    FileSystem fs = inFile.getFileSystem(params);
    while (index < temporalIndexes.length && !temporalRanges.isEmpty()) {
      Path indexDir = temporalIndexes[index];
      TemporalIndex temporalIndex = new TemporalIndex(fs, indexDir);
      for (int iRange = 0; iRange < temporalRanges.size(); iRange++) {
        TimeRange range = temporalRanges.get(iRange);
        TemporalPartition[] matches = temporalIndex.selectContained(range.start, range.end);
        if (matches != null) {
          for (TemporalPartition match : matches)
            matchingPartitions.add(new Path(indexDir, match.dirName));
          // Update range to remove matching part
          TemporalPartition firstMatch = matches[0];
          TemporalPartition lastMatch = matches[matches.length - 1];
          if (range.start < firstMatch.start && range.end > lastMatch.end) {
            // Need to split the range into two
            temporalRanges.setElementAt(
                new TimeRange(range.start, firstMatch.start), iRange);
            temporalRanges.insertElementAt(
                new TimeRange(lastMatch.end, range.end), iRange);
          } else if (range.start < firstMatch.start) {
            // Update range in-place
            range.end = firstMatch.start;
          } else if (range.end > lastMatch.end) {
            // Update range in-place
            range.start = lastMatch.end;
          } else {
            // Current range was completely covered. Remove it
            temporalRanges.remove(iRange);
          }
        }
      }
      index++;
    }
    
    Shape spatialRange = params.getShape("rect");
  }
  
  /**
   * Prints the usage of this operation.
   */
  public static void printUsage() {
    System.out.println("Runs a spatio-temporal aggregate query on indexed MODIS data");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("rect:<x1,y1,x2,y2> - (*) Spatial query range");
    System.out.println("time:<date1..date2> - (*) Temporal query range. "
        + "Format of each date is yyyy.mm.dd");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }
  
  /**
   * @param args
   * @throws IOException 
   * @throws ParseException 
   */
  public static void main(String[] args) throws IOException, ParseException {
    final OperationsParams params =
        new OperationsParams(new GenericOptionsParser(args), false);
    // Check input
    if (!params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    
    if (params.get("rect") == null) {
      System.err.println("Spatial range missing");
      printUsage();
      System.exit(1);
    }

    if (params.get("time") == null) {
      System.err.println("Temporal range missing");
      printUsage();
      System.exit(1);
    }
    
    if (!TimeRange.TimeRange.matcher(params.get("time")).matches()) {
      System.err.print("Illegal time range format '"+params.get("time")+"'");
      printUsage();
      System.exit(1);
    }
    
    long t1 = System.currentTimeMillis();
    aggregateQuery(params.getInputPath(), params);
    long t2 = System.currentTimeMillis();
    System.out.println("Aggregate query finished in "+(t2-t1)+" millis");
  }

}
