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
package edu.umn.cs.spatialHadoop.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.NASADataset;
import edu.umn.cs.spatialHadoop.core.NASAPoint;

/**
 * @author Ahmed Eldawy
 *
 */
public class HDFInputFormat extends FileInputFormat<NASADataset, NASAPoint> {
  
  /**The configuration entry for skipping the fill value*/
  public static final String SkipFillValue = "HDFRecordReader.SkipFillValue";
  
  /**Configuration for name of the dataset to read from HDF file*/
  public static final String DatasetName = "HDFInputFormat.DatasetName";
  
  @Override
  public RecordReader<NASADataset, NASAPoint> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new HDFRecordReader(job, (FileSplit)split, job.get(DatasetName),
        job.getBoolean(SkipFillValue, true));
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    // HDF files cannot be split
    return false;
  }
  
  /**
   * Returns a split for each HDF file in the input. If the input path point to
   * an HDF file (by extension), we just return a split that points to that
   * file. Otherwise, it is assumed to be a text file where each line points
   * to an HDF file to be processed.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // If the input files is a text files, parse it and retrieve all URLs in it
    Path[] inputPaths = getInputPaths(job);
    FileSystem fs = inputPaths[0].getFileSystem(job);

    if (inputPaths[0].getName().matches("(?i:.*\\.hdf$)") ||
        fs.getFileStatus(inputPaths[0]).isDir()) {
      // An HDF file, use the default behavior
      // Or a directory filled with HDF files
      return super.getSplits(job, numSplits);
    }
    // A text file, parse it and generate a split for each file
    FSDataInputStream in = fs.open(inputPaths[0]);
    LineRecordReader lineReader = new LineRecordReader(in, 0, Long.MAX_VALUE, job);
    LongWritable key = lineReader.createKey();
    Text line = lineReader.createValue();
    Vector<InputSplit> allSplits = new Vector<InputSplit>();
    while (lineReader.next(key, line)) {
      Path hdfPath = new Path(line.toString());
      // The next two lines would get the correct file length but we don't need
      // it as HDFRecordReader doesn't really use file length in FileSplit.
      // HDFRecordReader will compute file size itself. This is expected to be
      // faster and more scalable as RecordReaders are instantiated and run
      // on slave nodes while the InputFormat runs on the single master node
      // which is not parallelized
//      FileSystem hdfFs = hdfPath.getFileSystem(job);
//      long hdfLength = hdfFs.getFileStatus(hdfPath).getLen();
      allSplits.add(new FileSplit(hdfPath, 0, Long.MAX_VALUE, new String[] {}));
    }
    return allSplits.toArray(new InputSplit[allSplits.size()]);
  }
  
  public static void main(String[] args) throws IOException {
    String x = "habal.hdF";
    System.out.println(x.matches("(?i:.*\\.hdf$)"));
    System.exit(0);
    
    final Pattern HDFLink = Pattern.compile("<a href=\"([^\"]*\\.hdf)\">");
    String baseUrl = "http://e4ftl01.cr.usgs.gov/MOLT/MOD11A1.005/2013.11.13/";
    URL website = new URL(baseUrl);
    InputStream inStream = website.openStream();
    BufferedReader inBuffer = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = inBuffer.readLine()) != null) {
      Matcher matcher = HDFLink.matcher(line);
      while (matcher.find()) {
        String url = matcher.group(1);
        System.out.println(baseUrl+url);
      }
    }
  }
}
