/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * A set of method and algorithms used to support management of file splits.
 * 
 * @author eldawy
 *
 */
public class FileSplitUtil {
  static final Log LOG = LogFactory.getLog(FileSplitUtil.class);

  /**Disallow instantiation of this class*/
  private FileSplitUtil() {}
  
  /**
   * Combines a number of file splits into one CombineFileSplit. If number of
   * splits to be combined is one, it returns this split as is without creating
   * a CombineFileSplit.
   * @param splits
   * @param startIndex
   * @param count
   * @return
   * @throws IOException 
   */
  public static InputSplit combineFileSplits(JobConf conf,
      List<FileSplit> splits, int startIndex, int count) throws IOException {
    if (count == 1) {
      return splits.get(startIndex);
    } else {
      Path[] paths = new Path[count];
      long[] starts = new long[count];
      long[] lengths = new long[count];
      Vector<String> vlocations = new Vector<String>();
      while (count > 0) {
        paths[count - 1] = splits.get(startIndex).getPath();
        starts[count - 1] = splits.get(startIndex).getStart();
        lengths[count - 1] = splits.get(startIndex).getLength();
        vlocations.addAll(Arrays.asList(splits.get(startIndex).getLocations()));
        count--;
        startIndex++;
      }
      String[] locations = prioritizeLocations(vlocations);
      if (locations.length > 3) {
        String[] topLocations = new String[3];
        System.arraycopy(locations, 0, topLocations, 0, topLocations.length);
        locations = topLocations;
      }
      return new CombineFileSplit(conf, paths, starts, lengths, locations);
    }
  }

  /**
   * Combines a number of file splits into one CombineFileSplit (mapreduce). If
   * number of splits to be combined is one, it returns this split as is without
   * creating a CombineFileSplit.
   * 
   * @param splits
   * @param startIndex
   * @param count
   * @return
   * @throws IOException
   */
  public static org.apache.hadoop.mapreduce.InputSplit combineFileSplits(
      List<org.apache.hadoop.mapreduce.lib.input.FileSplit> splits, int startIndex, int count) throws IOException {
    if (count == 1) {
      return splits.get(startIndex);
    } else {
      Path[] paths = new Path[count];
      long[] starts = new long[count];
      long[] lengths = new long[count];
      Vector<String> vlocations = new Vector<String>();
      while (count > 0) {
        paths[count - 1] = splits.get(startIndex).getPath();
        starts[count - 1] = splits.get(startIndex).getStart();
        lengths[count - 1] = splits.get(startIndex).getLength();
        vlocations.addAll(Arrays.asList(splits.get(startIndex).getLocations()));
        count--;
        startIndex++;
      }
      String[] locations = prioritizeLocations(vlocations);
      if (locations.length > 3) {
        String[] topLocations = new String[3];
        System.arraycopy(locations, 0, topLocations, 0, topLocations.length);
        locations = topLocations;
      }
      return new org.apache.hadoop.mapreduce.lib.input.CombineFileSplit(paths, starts, lengths, locations);
    }
  }
  
  /**
   * Combines two file splits into a CombineFileSplit.
   * @param conf
   * @param split1
   * @param split2
   * @return
   * @throws IOException 
   */
  public static InputSplit combineFileSplits(JobConf conf,
      FileSplit split1, FileSplit split2) throws IOException {
    Path[] paths = new Path[2];
    long[] starts = new long[2];
    long[] lengths = new long[2];
    Vector<String> vlocations = new Vector<String>();
    paths[0] = split1.getPath();
    starts[0] = split1.getStart();
    lengths[0] = split1.getLength();
    vlocations.addAll(Arrays.asList(split1.getLocations()));
    paths[1] = split2.getPath();
    starts[1] = split2.getStart();
    lengths[1] = split2.getLength();
    vlocations.addAll(Arrays.asList(split2.getLocations()));
    String[] locations = prioritizeLocations(vlocations);
    return new CombineFileSplit(conf, paths, starts, lengths, locations);
  }
  
  /**
   * Takes a list of locations as a vector, and returns a unique array of
   * locations where locations on the head are more frequent in the original
   * vector than the ones on the tail.
   * 
   * @param vlocations - A vector of locations with possible duplicates
   * @return - A unique array of locations.
   */
  public static String[] prioritizeLocations(Vector<String> vlocations) {
    Collections.sort(vlocations);
    @SuppressWarnings("unchecked")
    Vector<String>[] locations_by_count = new Vector[vlocations.size()+1];
    
    int unique_location_count = 0;
    int first_in_run = 0;
    int i = 1;
    while (i < vlocations.size()) {
      if (vlocations.get(first_in_run).equals(vlocations.get(i))) {
        i++;
      } else {
        // End of run
        unique_location_count++;
        int count = i - first_in_run;
        if (locations_by_count[count] == null) {
          locations_by_count[count] = new Vector<String>();
        }
        locations_by_count[count].add(vlocations.get(first_in_run));
        first_in_run = i;
      }
    }
    // add last run
    unique_location_count++;
    int count = i - first_in_run;
    if (locations_by_count[count] == null) {
      locations_by_count[count] = new Vector<String>();
    }
    locations_by_count[count].add(vlocations.get(first_in_run));

    String[] unique_locations = new String[unique_location_count];
    for (Vector<String> locations_with_same_count : locations_by_count) {
      if (locations_with_same_count == null)
        continue;
      for (String loc : locations_with_same_count) {
        unique_locations[--unique_location_count] = loc;
      }
    }
    if (unique_location_count != 0)
      throw new RuntimeException();
    return unique_locations;
  }
  
  /**
   * Combines a number of input splits into the given numSplits.
   * @param conf
   * @param inputSplits
   * @param numSplits
   * @return
   * @throws IOException 
   */
  public static InputSplit[] autoCombineSplits(JobConf conf,
      Vector<FileSplit> inputSplits, int numSplits) throws IOException {
    LOG.info("Combining "+inputSplits.size()+" splits into "+numSplits);
    Map<String, Vector<FileSplit>> blocksPerHost =
        new HashMap<String, Vector<FileSplit>>();
    for (FileSplit fsplit : inputSplits) {
      // Get locations for this split
      final Path path = fsplit.getPath();
      final FileSystem fs = path.getFileSystem(conf);
      BlockLocation[] blockLocations = fs.getFileBlockLocations(
          fs.getFileStatus(path), fsplit.getStart(), fsplit.getLength());
      for (BlockLocation blockLocation : blockLocations) {
        for (String hostName : blockLocation.getHosts()) {
          if (!blocksPerHost.containsKey(hostName))
            blocksPerHost.put(hostName, new Vector<FileSplit>());
          blocksPerHost.get(hostName).add(fsplit);
        }
      }
    }

    // If the user requested a fewer number of splits, start to combine them
    InputSplit[] combined_splits = new InputSplit[numSplits];
    int splitsAvailable = inputSplits.size();

    for (int i = 0; i < numSplits; i++) {
      // Decide how many splits to combine
      int numSplitsToCombine = splitsAvailable / (numSplits - i);
      Vector<FileSplit> splitsToCombine = new Vector<FileSplit>();
      while (numSplitsToCombine > 0) {
        // Choose the host with minimum number of splits
        Map.Entry<String, Vector<FileSplit>> minEntry = null;
        for (Map.Entry<String, Vector<FileSplit>> entry : blocksPerHost.entrySet()) {
          if (minEntry == null || entry.getValue().size() < minEntry.getValue().size()) {
            minEntry = entry;
          }
        }
        // Combine all or some of blocks in this host
        for (FileSplit fsplit : minEntry.getValue()) {
          if (!splitsToCombine.contains(fsplit)) {
            splitsToCombine.add(fsplit);
            if (--numSplitsToCombine == 0)
              break;
          }
        }
        if (numSplitsToCombine != 0) {
          // Remove this host so that it is not selected again
          blocksPerHost.remove(minEntry.getKey());
        }
      }
      
      combined_splits[i] = combineFileSplits(conf, splitsToCombine, 0,
          splitsToCombine.size());
      
      for (Map.Entry<String, Vector<FileSplit>> entry : blocksPerHost.entrySet()) {
        entry.getValue().removeAll(splitsToCombine);
      }
      splitsAvailable -= splitsToCombine.size();
    }
    
    LOG.info("Combined splits "+combined_splits.length);
    return combined_splits;
  }
}
