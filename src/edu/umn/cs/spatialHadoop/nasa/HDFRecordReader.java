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
import java.util.Iterator;

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
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.hdf.DDNumericDataGroup;
import edu.umn.cs.spatialHadoop.hdf.DDVDataHeader;
import edu.umn.cs.spatialHadoop.hdf.DDVGroup;
import edu.umn.cs.spatialHadoop.hdf.DataDescriptor;
import edu.umn.cs.spatialHadoop.hdf.HDFConstants;
import edu.umn.cs.spatialHadoop.hdf.HDFFile;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * A record reader for HDF files with the new mapreduce interface
 * @author Ahmed Eldawy
 *
 */
public class HDFRecordReader<S extends NASAShape>
    extends RecordReader<NASADataset, Iterable<S>> {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(HDFRecordReader.class);
  
  /**Configuration line for the path to water mask*/
  public static final String WATER_MASK_PATH = "HDFRecordReader.WaterMaskPath";
  
  /**Information about the dataset being read*/
  private NASADataset nasaDataset;
  
  /**Value used to read from input*/
  private S nasaShape;

  /**Special value used to mark non-set entries*/
  private int fillValue;

  /**Set to true to skip non-set (fill) values in the input*/
  private boolean skipFillValue;
  
  /**
   * The raw data (unparsed) of the underlying dataset. We have to keep it as
   * an unparsed byte array because Java has very limited support to generic
   * arrays of primitive data types.
   */
  private byte[] unparsedDataArray;
  
  /**Number of bytes per data entry*/
  private int valueSize;
  
  /**Position to read next in the data array*/
  private int position;

  /**The iterator that is returned to MapReduce calls*/
  private NASAIterator value;

  /**The underlying HDF file*/
  private HDFFile hdfFile;

  /**Path to the input file*/
  private Path inFile;

  /**File system of the input file*/
  private FileSystem fs;
  /**A flag to delete the underlying HDF file on exit (if copied over HTTP)*/
  private boolean deleteOnEnd;


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
    inFile = ((FileSplit) split).getPath();
    fs = inFile.getFileSystem(conf);
    if (fs instanceof HTTPFileSystem) {
      // For performance reasons, we don't open HDF files from HTTP
      inFile = new Path(FileUtil.copyFile(conf, inFile));
      fs = FileSystem.getLocal(conf);
      this.deleteOnEnd = true;
    }
    hdfFile = new HDFFile(fs.open(inFile));
    
    // Retrieve meta data
    String archiveMetadata = (String) hdfFile.findHeaderByName("ArchiveMetadata.0").getEntryAt(0);
    String coreMetadata = (String) hdfFile.findHeaderByName("CoreMetadata.0").getEntryAt(0);
    nasaDataset = new NASADataset(coreMetadata, archiveMetadata);
    
    // Retrieve the data array
    DDVGroup dataGroup = hdfFile.findGroupByName(datasetName);
    boolean fillValueFound = false;
    int resolution = 0;
    for (DataDescriptor dd : dataGroup.getContents()) {
      if (dd instanceof DDNumericDataGroup) {
        DDNumericDataGroup numericDataGroup = (DDNumericDataGroup) dd;
        unparsedDataArray = numericDataGroup.getAsByteArray();
        valueSize = numericDataGroup.getDataSize();
        resolution = numericDataGroup.getDimensions()[0];
        if (valueSize * resolution * resolution != unparsedDataArray.length)
          throw new RuntimeException("Error parsing metadata");
      } else if (dd instanceof DDVDataHeader) {
        DDVDataHeader vheader = (DDVDataHeader) dd;
        if (vheader.getName().equals("_FillValue")) {
          Object fillValue = vheader.getEntryAt(0);
          if (fillValue instanceof Integer)
            this.fillValue = (Integer) fillValue;
          else if (fillValue instanceof Byte)
            this.fillValue = (Byte) fillValue;
          fillValueFound = true;
        } else if (vheader.getName().equals("valid_range")) {
          Object minValue = vheader.getEntryAt(0);
          if (minValue instanceof Integer)
            nasaDataset.minValue = (Integer) minValue;
          else if (minValue instanceof Byte)
            nasaDataset.minValue = (Byte) minValue;
          Object maxValue = vheader.getEntryAt(1);
          if (maxValue instanceof Integer)
            nasaDataset.maxValue = (Integer) maxValue;
          else if (maxValue instanceof Byte)
            nasaDataset.maxValue = (Byte) maxValue;
        }
      }
    }
    nasaDataset.resolution = resolution;
    if (!fillValueFound) {
      skipFillValue = false;
    } else {
      skipFillValue = conf.getBoolean("skipfill", true);
      // Whether we need to recover fill values or not
      boolean recoverFillValues = conf.getBoolean("recoverholes", true);
      if (recoverFillValues)
        recoverFillValues(conf);
    }
    this.nasaShape = (S) OperationsParams.getShape(conf, "shape", new NASARectangle());
    this.nasaShape.setTimestamp(nasaDataset.time);
    this.value = new NASAIterator();
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
    return unparsedDataArray == null? 0 : (float) position / unparsedDataArray.length;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return value.hasNext();
  }
  
  @Override
  public void close() throws IOException {
    hdfFile.close();
    if (deleteOnEnd) {
      fs.delete(inFile, true);
    }
  }
  
  /**
   * Sets the geometry information for the given object according to its
   * position in the array
   * @param p
   * @param position
   */
  protected void setShapeGeometry(Shape s, int position) {
    position /= valueSize;
    int row = position / nasaDataset.resolution;
    int col = position % nasaDataset.resolution;
    if (s instanceof Point) {
      Point p = (Point)s;
      p.y = (90 - nasaDataset.v * 10) -
          (double) row * 10 / nasaDataset.resolution;
      p.x = (nasaDataset.h * 10 - 180) +
          (double) (col) * 10 / nasaDataset.resolution;
      p.x /= Math.cos(p.y * Math.PI / 180);
    } else if (s instanceof Rectangle) {
      Rectangle r = (Rectangle)s;
      r.y2 = (90 - nasaDataset.v * 10) -
          (double) row * 10 / nasaDataset.resolution;
      r.y1 = (90 - nasaDataset.v * 10) -
          (double) (row + 1) * 10 / nasaDataset.resolution;
      double[] xs = new double[4];
      xs[0] = xs[1] = (nasaDataset.h * 10 - 180) +
          (double) (col) * 10 / nasaDataset.resolution;
      xs[2] = xs[3] = (nasaDataset.h * 10 - 180) +
          (double) (col+1) * 10 / nasaDataset.resolution;
      
      // Project all four corners and select the min-max for the rectangle
      xs[0] /= Math.cos(r.y1 * Math.PI / 180);
      xs[1] /= Math.cos(r.y2 * Math.PI / 180);
      xs[2] /= Math.cos(r.y1 * Math.PI / 180);
      xs[3] /= Math.cos(r.y2 * Math.PI / 180);
      r.x1 = r.x2 = xs[0];
      for (double x : xs) {
        if (x < r.x1)
          r.x1 = x;
        if (x > r.x2)
          r.x2 = x;
      }
    } else {
      throw new RuntimeException("Unsupported shape "+s.getClass());
    }
  }
  
  public class NASAIterator implements Iterable<S>, Iterator<S> {
    /**The underlying shape*/
    protected S shape;
    /**Next position to be read from the array*/
    protected int position;
    
    public NASAIterator() {
      shape = (S) HDFRecordReader.this.nasaShape.clone();
      // Initialize position on the first element to be read
      this.position = 0;
      skipFillValue();
    }
    
    private void skipFillValue() {
      while (position < unparsedDataArray.length
          && (!skipFillValue || HDFConstants.readAsAinteger(unparsedDataArray,
              position, valueSize) == fillValue))
        position += valueSize;
    }


    @Override
    public Iterator<S> iterator() {
      return this;
    }
    
    @Override
    public boolean hasNext() {
      return position < unparsedDataArray.length;
    }
    
    @Override
    public S next() {
      shape.setValue(HDFConstants.readAsAinteger(unparsedDataArray, position, valueSize));
      setShapeGeometry(shape, position);
      position += valueSize;
      skipFillValue();
      return shape;
    }
    
    @Override
    public void remove() {
      throw new RuntimeException("Method not implemented");
    }
  }
  
  /**
   * Return the value at the given offset in the array
   * @param x
   * @param y
   * @return
   */
  private int getValueAt(int x, int y) {
    int position = (y * nasaDataset.resolution + x) * valueSize;
    return HDFConstants.readAsAinteger(unparsedDataArray, position, valueSize);
  }

  private void setValueAt(int x, int y, int value) {
    int position = (y * nasaDataset.resolution + x) * valueSize;
    HDFConstants.writeAt(unparsedDataArray, position, value, valueSize);
  }
  
  /**
   * Recover fill values in the array {@link Values}.
   * @param conf
   * @throws IOException 
   * @throws Exception 
   */
  private void recoverFillValues(Configuration conf) throws IOException {
    // For now, we can only recover values of type short
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
          waterMask = (byte[])numericDataGroup.getAsByteArray();
        }
      }

      // Stores which values has been recovered by copying a single value
      // without interpolation in the x-direction
      byte[] valueStatus = new byte[unparsedDataArray.length / valueSize];
      
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
        while (x1 < inputRes && getValueAt(x1, y) != fillValue)
          x1++;
        // Now advance x2 until it reaches the first non-missing value
        x2 = x1;
        while (x2 < inputRes && getValueAt(x2, y) == fillValue)
          x2++;
        // Recover all points in the range [x1, x2)
        if (x1 == 0 && x2 == inputRes) {
          // The whole line is empty. Nothing to do
          for (int x = x1; x < x2; x++)
            valueStatus[y * inputRes + x] = 1;
        } else if (x1 == 0 || x2 == inputRes) {
          // We have only one value at one end of the missing run
          int copyVal = getValueAt(x1 == 0 ? x2 : (x1 - 1), y);
          for (int x = x1; x < x2; x++) {
            if (onLand(water_mask, x, y, inputRes))
              setValueAt(x, y, copyVal);
            valueStatus[y * inputRes + x] = 2;
          }
        } else {
          // Interpolate values between x1 and x2
          int val1 = getValueAt(x1 - 1, y);
          int val2 = getValueAt(x2, y);
          for (int x = x1; x < x2; x++) {
            if (onLand(water_mask, x, y, inputRes)) {
              short interpolatedValue = (short) (((double)val1 * (x2 - x) + (double)val2 * (x - x1)) / (x2 - x1));
              setValueAt(x, y, interpolatedValue);
            }
            valueStatus[y * inputRes + x] = 3;
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
        while (y1 < inputRes && valueStatus[y1 * inputRes + x] == 0)
          y1++;
        // Now advance y2 until it reaches the first non-missing value
        y2 = y1;
        while (y2 < inputRes && valueStatus[y2 * inputRes + x] != 0)
          y2++;
        // Recover all points in the range [y1, y2)
        if (y1 == 0 && y2 == inputRes) {
          // The whole column is empty. Nothing to do
        } else if (y1 == 0 || y2 == inputRes) {
          // We have only one value at one end of the missing run
          int copyVal = getValueAt(x, y1 == 0 ? y2 : (y1 - 1));
          for (int y = y1; y < y2; y++) {
            if (onLand(water_mask, x, y, inputRes)) {
              if (valueStatus[y * inputRes + x] == 1) {
                // Value has never been recovered but needs to
                setValueAt(x, y, copyVal);
              } else if (valueStatus[y * inputRes + x] == 2) {
                // Value has been previously copied. Take average
                setValueAt(x, y,  ((getValueAt(x, y) + copyVal) / 2));
              }
            }
          }
        } else {
          // Interpolate values between x1 and x2
          int val1 = getValueAt(x, y1 - 1);
          int val2 = getValueAt(x, y2);
          for (int y = y1; y < y2; y++) {
            if (onLand(water_mask, x, y, inputRes)) {
              short interValue =
                  (short) (((double)val1 * (y2 - y) + (double)val2 * (y - y1)) / (y2 - y1));
              if (valueStatus[y * inputRes + x] <= 2) {
                // Value has never been recovered or has been copied
                setValueAt(x, y, interValue);
              } else {
                // Value has been previously interpolated, take average
                setValueAt(x, y, (getValueAt(x, y) + interValue) / 2);
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
