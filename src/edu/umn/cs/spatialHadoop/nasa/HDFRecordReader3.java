/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * A record reader for HDF files with the new mapreduce interface
 * @author Ahmed Eldawy
 *
 */
public class HDFRecordReader3<S extends NASAShape>
    extends RecordReader<NASADataset, Iterable<S>> {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(HDFRecordReader3.class);
  
  /**Configuration line for the path to water mask*/
  private static final String WATER_MASK_PATH = "HDFRecordReader.WaterMaskPath";
  
  /** The HDF file being read*/
  private FileFormat hdfFile;

  /**Information about the dataset being read*/
  private NASADataset nasaDataset;
  
  /**Value used to read from input*/
  private S nasaShape;

  /**Special value used to mark non-set entries*/
  private int fillValue;

  /**Set to true to skip non-set (fill) values in the input*/
  private boolean skipFillValue;
  
  /**
   * Array of values in the dataset being read. Notice that this would work
   * only if the values stored in HDF file are of type short. Otherwise,
   * this record reader would not work
   */
  private short[] dataArray;
  
  /**Position to read next in the data array*/
  private int position;

  /**HDF file format used to open HDF files.*/
  private FileFormat fileFormat;

  /**The iterator that is returned to MapReduce calls*/
  private NASAIterator value;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    initialize(split, conf);
  }

  public void initialize(InputSplit split, Configuration conf) {
    try {
      // HDF library can only deal with local files. So, we need to copy the file
      // to a local temporary directory before using the HDF Java library
      String localFile = FileUtil.copyFileSplit(conf, (FileSplit)split);

      fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);

      hdfFile = fileFormat.createInstance(localFile, FileFormat.READ);

      // open the file and retrieve the file structure
      hdfFile.open();

      // Retrieve the root of the HDF file structure
      Group root =
          (Group)((DefaultMutableTreeNode)hdfFile.getRootNode()).getUserObject();

      String datasetName = conf.get("dataset");
      Dataset matchDataset = findDataset(root, datasetName, true);

      nasaDataset = new NASADataset(root);
      nasaDataset.datasetName = datasetName;
      @SuppressWarnings("unchecked")
      List<Attribute> attrs = matchDataset.getMetadata();
      String fillValueStr = null;
      for (Attribute attr : attrs) {
        if (attr.getName().equals("_FillValue")) {
          fillValueStr = Array.get(attr.getValue(), 0).toString();
        } else if (attr.getName().equals("valid_range")) {
          nasaDataset.minValue = Integer.parseInt(Array.get(attr.getValue(), 0).toString());
          nasaDataset.maxValue = Integer.parseInt(Array.get(attr.getValue(), 1).toString());
          if (nasaDataset.maxValue < 0) {
            // Convert unsigned to signed
            nasaDataset.maxValue += 65536;
          }
        }
      }

      if (fillValueStr == null) {
        this.skipFillValue = false;
      } else {
        this.fillValue = Integer.parseInt(fillValueStr);
      }

      dataArray = (short[])matchDataset.read();

      // Whether we need to recover fill values or not
      boolean recoverFillValues = conf.getBoolean("recoverholes", true);

      if (recoverFillValues && fillValueStr != null)
        recoverFillValues(conf);

      this.nasaShape = (S) OperationsParams.getShape(conf, "shape", new NASARectangle());
      this.value = new NASAIterator();
    } catch (Exception e) {
      LOG.error("Error opening HDF file", e);
    } finally {
      if (hdfFile != null)
        try {
          hdfFile.close();
        } catch (Exception e) {
          LOG.error("Error closing HDF file", e);
        }
    }

  }


  @Override
  public NASADataset getCurrentKey() throws IOException, InterruptedException {
    return nasaDataset;
  }

  @Override
  public Iterable<S> getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return dataArray == null ? 0 : (float) position / dataArray.length;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return value.hasNext();
  }
  
  @Override
  public void close() throws IOException {
    // Nothing to do because the file has been completely loaded in memory
    // and the original file is already closed
  }
  
  /**
   * Reads next NASA object from the array
   * @param key
   * @param shape
   * @return
   * @throws IOException
   */
  protected boolean nextObject(NASADataset key, NASAShape shape) throws IOException {
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
//      if (projector != null)
//        projector.project(shape);
      shape.setTimestamp(key.time);
      
      // Read next value
      shape.setValue(dataArray[position]);
      position++;
      if (!skipFillValue || shape.getValue() != fillValue)
        return true;
    }
    return false;
  }
  
  
  public class NASAIterator implements Iterable<S>, Iterator<S> {
    
    /**Next value to be returned*/
    protected S next;
    /**Last value returned*/
    protected S last;

    public NASAIterator() throws IOException {
      last = HDFRecordReader3.this.nasaShape;
      next = (S) last.clone();
      if (!nextObject(nasaDataset, next))
        next = null;
    }
    
    @Override
    public Iterator<S> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public S next() {
      try {
        last = next;
        if (!nextObject(nasaDataset, next))
          next = null;
        return last;
      } catch (IOException e) {
        LOG.warn("Error getting next shape", e);
        return null;
      }
    }

    @Override
    public void remove() {
      throw new RuntimeException("Method not implemented");
    }
  }

  /**
   * Recover fill values in the array {@link Values}.
   * @param conf
   * @throws Exception 
   */
  private void recoverFillValues(Configuration conf) throws Exception {
    FileFormat wmHDFFile = null;
    try {
      // Read water mask
      Path wmPath = new Path(conf.get(WATER_MASK_PATH, "http://e4ftl01.cr.usgs.gov/MOLT/MOD44W.005/2000.02.24/"));
      final String tileIdentifier = String.format("h%02dv%02d", nasaDataset.h, nasaDataset.v);
      FileSystem wmFs = wmPath.getFileSystem(conf);
      FileStatus[] wmFile = wmFs.listStatus(wmPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().contains(tileIdentifier);
        }});   
      if (wmFile.length == 0) {
        LOG.warn("Could not find water mask for tile '"+tileIdentifier+"'");
        return;
      }
      String localFile = FileUtil.copyFile(conf, wmFile[0]);
      wmHDFFile = fileFormat.createInstance(localFile, FileFormat.READ);
      wmHDFFile.open();
      
      Group root = (Group)((DefaultMutableTreeNode)wmHDFFile.getRootNode()).getUserObject();
      
      // Search for the datset of interest in file
      Dataset matchDataset = findDataset(root, "water_mask", false);
      
      if (matchDataset == null) {
        LOG.warn("Water mask dataset 'water_mask' not found in file "+localFile);
        return;
      }
      
      // Stores which values has been recovered by copying a single value
      // without interpolation in the x-direction
      byte[] valueStatus = new byte[Array.getLength(dataArray)];
      
      // Extract the water mask
      byte[] water_mask = (byte[]) matchDataset.read();

      recoverXDirection(water_mask, valueStatus);
      recoverYDirection(water_mask, valueStatus);
    } finally {
      if (wmHDFFile != null)
        wmHDFFile.close();
    }
  }

  /***
   * Recover fill values in x-direction (horizontally)
   * @param water_mask
   * @param valueStatus - (output) tells the status of each entry in the input <br/>
   *  0 - the value does not need to be recovered <br/>
   *  1 - Needs to be recovered, but not touched <br/>
   *  2 - Needs to be recovered, copied from a neighboring value <br/>
   *  3 - Needs to be recovered, interpolated from two neighboring values <br/>
   */
  private void recoverXDirection(byte[] water_mask, byte[] valueStatus) {
    // Resolution of the input dataset
    int inputRes = nasaDataset.resolution;
    // Recover in x-direction
    for (int y = 0; y < inputRes; y++) {
      int x2 = 0;
      while (x2 < inputRes) {
        int x1 = x2;
        // x1 should point to the first missing point
        while (x1 < inputRes &&
            dataArray[y * inputRes + x1] != fillValue)
          x1++;
        // Now advance x2 until it reaches the first non-missing value
        x2 = x1;
        while (x2 < inputRes &&
            dataArray[y * inputRes + x2] == fillValue)
          x2++;
        // Recover all points in the range [x1, x2)
        if (x1 == 0 && x2 == inputRes) {
          // The whole line is empty. Nothing to do
          for (int x = x1; x < x2; x++)
            valueStatus[y * inputRes + x] = 1;
        } else if (x1 == 0 || x2 == inputRes) {
          // We have only one value at one end of the missing run
          short copyVal = dataArray[y * inputRes + (x1 == 0 ? x2 : (x1 - 1))];
          for (int x = x1; x < x2; x++) {
            if (onLand(water_mask, x, y, inputRes)) {
              dataArray[y * inputRes + x] = copyVal;
              valueStatus[y * inputRes + x] = 2;
            }
          }
        } else {
          // Interpolate values between x1 and x2
          short val1 = dataArray[y * inputRes + (x1-1)];
          short val2 = dataArray[y * inputRes + x2];
          for (int x = x1; x < x2; x++) {
            if (onLand(water_mask, x, y, inputRes)) {
              short interpolatedValue = (short) ((val1 * (x2 - x) + val2 * (x - x1)) / (x2 - x1));
              dataArray[y * inputRes + x] = interpolatedValue;
              valueStatus[y * inputRes + x] = 3;
            }
          }
        }
      }
    }
  }
  
  /**
   * Recover fill values in the y-direction (vertically).
   * @param water_mask
   * @param valueStatus - (input) tells the status of each entry in the input <br/>
   *  0 - the value does not need to be recovered <br/>
   *  1 - Needs to be recovered, has not been touched <br/>
   *  2 - Needs to be recovered, copied from a neighboring value <br/>
   *  3 - Needs to be recovered, interpolated from two neighboring values <br/>
   */
  private void recoverYDirection(byte[] water_mask, byte[] valueStatus) {
    // Resolution of the input dataset
    int inputRes = nasaDataset.resolution;
    // Recover in x-direction
    for (int x = 0; x < inputRes; x++) {
      int y2 = 0;
      while (y2 < inputRes) {
        int y1 = y2;
        // y1 should point to the first missing point
        while (y1 < inputRes &&
            valueStatus[y1 * inputRes + x] != 0)
          y1++;
        // Now advance y2 until it reaches the first non-missing value
        y2 = y1;
        while (y2 < inputRes &&
            valueStatus[y2 * inputRes + x] == 0)
          y2++;
        // Recover all points in the range [x1, x2)
        if (y1 == 0 && y2 == inputRes) {
          // The whole column is empty. Nothing to do
        } else if (y1 == 0 || y2 == inputRes) {
          // We have only one value at one end of the missing run
          short copyVal = dataArray[(y1 == 0 ? y2 : (y1 - 1)) * inputRes + x];
          for (int y = y1; y < y2; y++) {
            if (onLand(water_mask, x, y, inputRes)) {
              if (valueStatus[y * inputRes + x] == 1) {
                // Value has never been recovered but needs to
                dataArray[y * inputRes + x] = copyVal;
              } else if (valueStatus[y * inputRes + x] == 2) {
                // Value has been previously copied. Take average
                dataArray[y * inputRes + x] =
                    (short) ((dataArray[y * inputRes + x] + copyVal) / 2);
              }
            }
          }
        } else {
          // Interpolate values between x1 and x2
          short val1 = dataArray[(y1-1) * inputRes + x];
          short val2 = dataArray[y2 * inputRes + x];
          for (int y = y1; y < y2; y++) {
            if (onLand(water_mask, x, y, inputRes)) {
              short interValue =
                  (short) ((val1 * (y2 - y) + val2 * (y - y1)) / (y2 - y1));
              if (valueStatus[y * inputRes + x] <= 2) {
                // Value has never been recovered or has been copied
                dataArray[y * inputRes + x] = interValue;
              } else {
                // Value has been previously interpolated, takea verage
                dataArray[y * inputRes + x] = 
                    (short) ((dataArray[y * inputRes + x] + interValue) / 2);
              }
            }
          }
        }
      }
    }
  }
  
  /***
   * Checks whether a value is on land or not. If a value is on land, it will
   * be recovered during the hole recovery process.
   * @param water_mask
   * @param x
   * @param y
   * @param inputRes
   * @return
   */
  private static boolean onLand(byte[] water_mask, int x, int y, int resolution) {
    int wm_x = x * 4800 / resolution;
    int wm_y = y * 4800 / resolution;
    int size = 4800 / resolution;
    byte wm_sum = 0;
    for (int xx = 0; xx < size; xx++) {
      for (int yy = 0; yy < size; yy++) {
        byte wm_value = water_mask[(yy +wm_y) * 4800 + (xx+wm_x)];
        if (wm_value == 0)
          wm_sum++; // value = 0 means land
      }
    }
    if (wm_sum >= (size * size) / 2)
      return true;
    return false;
  }

  /**
   * @param split
   * @param datasetName
   * @param root
   * @param matchAny
   * @return
   */
  public static Dataset findDataset(Group root, String datasetName, boolean matchAny) {
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

    if (matchDataset == null && matchAny) {
      LOG.warn("Dataset "+datasetName+" not found in file");
      // Just work on the first dataset to ensure we return some data
      matchDataset = firstDataset;
    }
    return matchDataset;
  }


}
