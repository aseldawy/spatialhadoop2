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
package edu.umn.cs.spatialHadoop.nasa;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Stack;

import javax.swing.tree.DefaultMutableTreeNode;

import ncsa.hdf.object.Attribute;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.HObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

/**
 * Reads a specific dataset from an HDF file. Upon instantiation, the portion
 * of the HDF file being read is copied to a local directory. After that, the
 * HDF Java library is used to access the file.
 * This class is designed in specific to read an HDF file provided by NASA
 * in their MODIS datasets. It assumes a certain structure and HDF file format
 * and parses the file accordingly.
 * @author Ahmed Eldawy
 *
 */
public class HDFRecordReader implements RecordReader<NASADataset, NASAShape> {
  private static final Log LOG = LogFactory.getLog(HDFRecordReader.class);

  /** The HDF file being read*/
  private FileFormat hdfFile;
  
  /**Information about the dataset being read*/
  private NASADataset nasaDataset;
  
  /**Array of values in the dataset being read*/
  private Object dataArray;
  
  /**Position to read next in the data array*/
  private int position;
  
  /**The default value that indicates that a number is not set*/
  private int fillValue;
  
  /**Whether or not to skip fill value when returning values in a dataset*/
  private boolean skipFillValue;
  
  /**The projector used to convert NASA points to a different space*/
  private GeoProjector projector;
  
  /**Shape returned by the reader*/
  private NASAShape value;

  /**Configuration for name of the dataset to read from HDF file*/
  public static final String DatasetName = "HDFInputFormat.DatasetName";

  /**The configuration entry for skipping the fill value*/
  public static final String SkipFillValue = "HDFRecordReader.SkipFillValue";
  
  /**The class used to project NASA points upon read*/
  public static final String ProjectorClass = "HDFRecordReader.ProjectorClass";
  

  /**
   * Initializes the HDF reader to read a specific dataset from the given HDF
   * file.
   * @param job - Job configuration
   * @param split - A file split pointing to the HDF file
   * @param datasetName - Name of the dataset to read (case insensitive)
   * @throws Exception
   */
  public HDFRecordReader(Configuration job, FileSplit split,
      String datasetName, boolean skipFillValue) throws IOException {
    this.skipFillValue = skipFillValue;
    init(job, split, datasetName);
    try {
      Class<? extends GeoProjector> projectorClass =
          job.getClass(ProjectorClass, null, GeoProjector.class);
      if (projectorClass != null)
        this.projector = projectorClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * Initializes the reader to read a specific dataset from the given HDF file.
   * It open the HDF file and retrieves the dataset of interest into memory.
   * After this, the values are retrieved one by one from the dataset as
   * {@link #next(Dataset, NASAPoint)} is called.
   * @param job
   * @param split
   * @param datasetName
   * @throws Exception
   */
  private void init(Configuration job, FileSplit split, String datasetName) throws IOException {
    try {
      // HDF library can only deal with local files. So, we need to copy the file
      // to a local temporary directory before using the HDF Java library
      String localFile = copyFileSplit(job, split);

      // retrieve an instance of H4File
      FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);

      hdfFile = fileFormat.createInstance(localFile, FileFormat.READ);

      // open the file and retrieve the file structure
      hdfFile.open();

      // Retrieve the root of the HDF file structure
      Group root =
          (Group)((DefaultMutableTreeNode)hdfFile.getRootNode()).getUserObject();

      // Search for the datset of interest in file
      Stack<Group> groups2bSearched = new Stack<Group>();
      groups2bSearched.add(root);

      Dataset firstDataset = null;
      Dataset matchDataset = null;

      while (!groups2bSearched.isEmpty()) {
        Group top = groups2bSearched.pop();
        List<HObject> memberList = top.getMemberList();

        for (HObject member : memberList) {
          if (member instanceof Group) {
            groups2bSearched.add((Group) member);
          } else if (member instanceof Dataset) {
            if (firstDataset == null)
              firstDataset = (Dataset) member;
            if (member.getName().equalsIgnoreCase(datasetName)) {
              matchDataset = (Dataset) member;
              break;
            }
          }
        }
      }

      if (matchDataset == null) {
        LOG.warn("Dataset "+datasetName+" not found in file "+split.getPath());
        // Just work on the first dataset to ensure we return some data
        matchDataset = firstDataset;
      }

      nasaDataset = new NASADataset(root);
      nasaDataset.datasetName = datasetName;
      List<Attribute> attrs = matchDataset.getMetadata();
      String fillValueStr = null;
      for (Attribute attr : attrs) {
        if (attr.getName().equals("_FillValue")) {
          fillValueStr = Array.get(attr.getValue(), 0).toString();
        } else if (attr.getName().equals("valid_range")) {
          nasaDataset.minValue = Integer.parseInt(Array.get(attr.getValue(), 0).toString());
          nasaDataset.maxValue = Integer.parseInt(Array.get(attr.getValue(), 1).toString());
          if (nasaDataset.maxValue < 0) {
            nasaDataset.maxValue += 65536;
          }
        }
      }
      
      this.value = (NASAShape) SpatialSite.createStockShape(job);
      
      if (fillValueStr == null) {
        this.skipFillValue = false;
      } else {
        this.fillValue = Integer.parseInt(fillValueStr);
      }

      dataArray = matchDataset.read();

      // No longer need the HDF file
      hdfFile.close();
      fileFormat.close();
    } catch (Exception e) {
      throw new RuntimeException("Error reading HDF file '"+split.getPath()+"'", e);
    }
  }

  /**
   * Copies a part of a file from a remote file system (e.g., HDFS) to a local
   * file. Returns a path to a local temporary file.
   * @param conf
   * @param split
   * @return
   * @throws IOException 
   */
  static String copyFileSplit(Configuration conf, FileSplit split) throws IOException {
    FileSystem fs = split.getPath().getFileSystem(conf);

    // Special case of a local file. Skip copying the file
    if (fs instanceof LocalFileSystem && split.getStart() == 0)
      return split.getPath().toUri().getPath();

    // Length of input file. We do not depend on split.length because it is not
    // set by input format for performance reason. Setting it in the input
    // format would cost a lot of time because it runs on the client machine
    // while the record reader runs on slave nodes in parallel
    long length = fs.getFileStatus(split.getPath()).getLen();

    FSDataInputStream in = fs.open(split.getPath());
    in.seek(split.getStart());
    
    // Prepare output file for write
    File tempFile = File.createTempFile(split.getPath().getName(), "hdf");
    OutputStream out = new FileOutputStream(tempFile);
    
    // A buffer used between source and destination
    byte[] buffer = new byte[1024*1024];
    while (length > 0) {
      int numBytesRead = in.read(buffer, 0, (int)Math.min(length, buffer.length));
      out.write(buffer, 0, numBytesRead);
      length -= numBytesRead;
    }
    
    in.close();
    out.close();
    return tempFile.getAbsolutePath();
  }

  @Override
  public boolean next(NASADataset key, NASAShape shape) throws IOException {
    if (dataArray == null)
      return false;
    // Key doesn't need to be changed because all points in the same dataset
    // have the same key
    while (position < Array.getLength(dataArray)) {
      // Set the x and y to longitude and latitude by doing the correct projection
      int row = position / nasaDataset.resolution;
      int col = position % nasaDataset.resolution;
      if (shape instanceof Point) {
        Point pt = (Point) shape;
        pt.y = (90 - nasaDataset.v * 10) -
            (double) row * 10 / nasaDataset.resolution;
        pt.x = (nasaDataset.h * 10 - 180) +
            (double) (col) * 10 / nasaDataset.resolution;
        pt.x /= Math.cos(pt.y * Math.PI / 180);
      } else if (shape instanceof Rectangle) {
        Rectangle rect = (Rectangle) shape;
        rect.y2 = (90 - nasaDataset.v * 10) -
            (double) row * 10 / nasaDataset.resolution;
        rect.y1 = (90 - nasaDataset.v * 10) -
            (double) (row + 1) * 10 / nasaDataset.resolution;
        double[] xs = new double[4];
        xs[0] = xs[1] = (nasaDataset.h * 10 - 180) +
            (double) (col) * 10 / nasaDataset.resolution;
        xs[2] = xs[3] = (nasaDataset.h * 10 - 180) +
            (double) (col+1) * 10 / nasaDataset.resolution;

        // Project all four corners and select the min-max for the rectangle
        xs[0] /= Math.cos(rect.y1 * Math.PI / 180);
        xs[1] /= Math.cos(rect.y2 * Math.PI / 180);
        xs[2] /= Math.cos(rect.y1 * Math.PI / 180);
        xs[3] /= Math.cos(rect.y2 * Math.PI / 180);
        rect.x1 = rect.x2 = xs[0];
        for (double x : xs) {
          if (x < rect.x1)
            rect.x1 = x;
          if (x > rect.x2)
            rect.x2 = x;
        }
      }
      if (projector != null)
        projector.project(shape);
      
      // Read next value
      Object value = Array.get(dataArray, position);
      if (value instanceof Integer) {
        shape.setValue((Integer) value);
      } else if (value instanceof Short) {
        shape.setValue((Short) value);
      } else if (value instanceof Byte) {
        shape.setValue((Byte) value);
      } else {
        throw new RuntimeException("Cannot read a value of type "+value.getClass());
      }
      position++;
      if (!skipFillValue || shape.getValue() != fillValue)
        return true;
    }
    return false;
  }

  @Override
  public NASADataset createKey() {
    return nasaDataset;
  }

  @Override
  public NASAShape createValue() {
    return value;
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public void close() throws IOException {
    // Nothing to do. The HDF file is already closed
  }

  @Override
  public float getProgress() throws IOException {
    return dataArray == null ? 0 : (float)position / Array.getLength(dataArray);
  }

}
