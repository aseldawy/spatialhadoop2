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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import edu.umn.cs.spatialHadoop.hdf.DDNumericDataGroup;
import edu.umn.cs.spatialHadoop.hdf.DDVDataHeader;
import edu.umn.cs.spatialHadoop.hdf.DDVGroup;
import edu.umn.cs.spatialHadoop.hdf.DataDescriptor;
import edu.umn.cs.spatialHadoop.hdf.HDFFile;
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

  /**The iterator that is returned to MapReduce calls*/
  private NASAIterator value;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    initialize(split, conf);
  }

  public void initialize(InputSplit split, Configuration conf) throws IOException {
    String datasetName = conf.get("dataset");
    if (datasetName == null)
      throw new RuntimeException("Dataset name should be provided");
    FileSplit fsplit = (FileSplit) split;
    FileSystem fs = fsplit.getPath().getFileSystem(conf);
    FSDataInputStream input = fs.open(fsplit.getPath());
    HDFFile hdfFile = new HDFFile(input);
    
    // Retrieve meta data
    nasaDataset = new NASADataset((String) hdfFile.findHeaderByName("CoreMetadata.0").getEntryAt(0));
    
    // Retrieve the data array
    DDVGroup dataGroup = hdfFile.findGroupByName(datasetName);
    boolean fillValueFound = false;
    int resolution = 0;
    for (DataDescriptor dd : dataGroup.getContents()) {
      if (dd instanceof DDNumericDataGroup) {
        DDNumericDataGroup numericDataGroup = (DDNumericDataGroup) dd;
        dataArray = (short[])numericDataGroup.getAsAnArray();
        resolution = numericDataGroup.getDimensions()[0];
      } else if (dd instanceof DDVDataHeader) {
        DDVDataHeader vheader = (DDVDataHeader) dd;
        if (vheader.getName().equals("_FillValue")) {
          this.fillValue = (Integer) vheader.getEntryAt(0);
          fillValueFound = true;
        } else if (vheader.getName().equals("valid_range")) {
          nasaDataset.minValue = (Integer) vheader.getEntryAt(0);
          nasaDataset.maxValue = (Integer) vheader.getEntryAt(1);
        }
      }
    }
    nasaDataset.resolution = resolution;
    if (!fillValueFound) {
      skipFillValue = false;
    } else {
      // Whether we need to recover fill values or not
      boolean recoverFillValues = conf.getBoolean("recoverholes", true);
      if (recoverFillValues)
        recoverFillValues(conf);
    }
    this.nasaShape = (S) OperationsParams.getShape(conf, "shape", new NASARectangle());
    this.value = new NASAIterator();
    hdfFile.close();
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
   * @throws IOException 
   * @throws Exception 
   */
  private void recoverFillValues(Configuration conf) throws IOException {
    HDFFile waterMaskFile = null;
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
      Path wmFileToLoad = wmFile[0].getPath();
      if (wmFs instanceof HTTPFileSystem) {
        wmFileToLoad = new Path(FileUtil.copyFile(conf, wmFileToLoad));
        wmFs = FileSystem.getLocal(conf);
      }
      waterMaskFile = new HDFFile(wmFs.open(wmFileToLoad));
      DDVGroup waterMaskGroup = waterMaskFile.findGroupByName("water_mask");
      if (waterMaskGroup == null) {
        LOG.warn("Water mask dataset 'water_mask' not found in file "+wmFile[0]);
        return;
      }
      byte[] waterMask = null;
      for (DataDescriptor dd : waterMaskGroup.getContents()) {
        if (dd instanceof DDNumericDataGroup) {
          DDNumericDataGroup numericDataGroup = (DDNumericDataGroup) dd;
          waterMask = (byte[])numericDataGroup.getAsAnArray();
        }
      }

      // Stores which values has been recovered by copying a single value
      // without interpolation in the x-direction
      byte[] valueStatus = new byte[dataArray.length];
      
      recoverXDirection(waterMask, valueStatus);
      recoverYDirection(waterMask, valueStatus);
    } finally {
      if (waterMaskFile != null)
        waterMaskFile.close();
    }
  }

  /***
   * Recover fill values in x-direction (horizontally)
   * @param water_mask
   * @param valueStatus - (output) tells the status of each entry in the input <br/>
   *  0 - the corresponding entry originally contained a value<br/>
   *  1 - the entry did not contain a value and still does not contain one<br/>
   *  2 - the entry did not contain a value and its current value is copied<br/>
   *  3 - the entry did not contain a value and its current value is interpolated<br/>
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
          for (int x = x1; x < x2; x++) {
            valueStatus[y * inputRes + x] = 1;
          }
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
              short interpolatedValue = (short) (((double)val1 * (x2 - x) + (double)val2 * (x - x1)) / (x2 - x1));
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
   *  0 - the corresponding entry originally contained a value<br/>
   *  1 - the entry did not contain a value and still does not contain one<br/>
   *  2 - the entry did not contain a value and its current value is copied<br/>
   *  3 - the entry did not contain a value and its current value is interpolated<br/>
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
                    (short) (((int)dataArray[y * inputRes + x] + copyVal) / 2);
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
                  (short) (((double)val1 * (y2 - y) + (double)val2 * (y - y1)) / (y2 - y1));
              if (valueStatus[y * inputRes + x] <= 2) {
                // Value has never been recovered or has been copied
                dataArray[y * inputRes + x] = interValue;
              } else {
                // Value has been previously interpolated, take average
                dataArray[y * inputRes + x] = 
                    (short) (((int)dataArray[y * inputRes + x] + interValue) / 2);
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

}
