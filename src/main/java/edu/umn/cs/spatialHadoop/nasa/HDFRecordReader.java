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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Vector;

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
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
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
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import edu.umn.cs.spatialHadoop.util.ShortArray;

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

  /**A list of file splits to read*/
  private Vector<FileSplit> splits;

  /**The configuration of the underlying task. Used to initialize more splits*/
  private Configuration conf;

  private byte[] fillValueBytes;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    initialize(split, conf);
  }

  public void initialize(InputSplit split, Configuration conf) throws IOException {
    this.conf = conf;
    String datasetName = conf.get("dataset");
    if (datasetName == null)
      throw new RuntimeException("Dataset name should be provided");
    if (split instanceof CombineFileSplit) {
      CombineFileSplit csplits = (CombineFileSplit) split;
      splits = new Vector<FileSplit>(csplits.getNumPaths());
      for (int i = 0; i < csplits.getNumPaths(); i++) {
        FileSplit fsplit = new FileSplit(csplits.getPath(i),
            csplits.getOffset(i), csplits.getLength(i), csplits.getLocations());
        splits.add(fsplit);
      }
      this.initialize(splits.remove(splits.size() - 1), conf);
      return;
    }
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
    // Retrieve metadata
    int fillValuee=0;
    for (DataDescriptor dd : dataGroup.getContents()) {
      if (dd instanceof DDVDataHeader) {
        DDVDataHeader vheader = (DDVDataHeader) dd;
        if (vheader.getName().equals("_FillValue")) {
          Object fillValue = vheader.getEntryAt(0);
          if (fillValue instanceof Integer)
            fillValuee = (Integer) fillValue;
          else if (fillValue instanceof Short)
            fillValuee = (Short) fillValue;
          else if (fillValue instanceof Byte)
            fillValuee = (Byte) fillValue;
          else
            throw new RuntimeException("Unsupported type: "+fillValue.getClass());
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
    // Retrieve data
    for (DataDescriptor dd : dataGroup.getContents()) {
      if (dd instanceof DDNumericDataGroup) {
        DDNumericDataGroup numericDataGroup = (DDNumericDataGroup) dd;
        valueSize = numericDataGroup.getDataSize();
        resolution = numericDataGroup.getDimensions()[0];
        unparsedDataArray = new byte[valueSize * resolution * resolution];
        if (fillValueFound) {
          fillValueBytes = new byte[valueSize];
          HDFConstants.writeAt(fillValueBytes, 0, fillValuee, valueSize);
          for (int i = 0; i < unparsedDataArray.length; i++)
            unparsedDataArray[i] = fillValueBytes[i % valueSize];
        }
        numericDataGroup.getAsByteArray(unparsedDataArray, 0, unparsedDataArray.length);
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
    boolean moreRecordsInCurrentFile = value.hasNext();
    while (!moreRecordsInCurrentFile && splits != null && !splits.isEmpty()) {
      // End of current file. Open next file and check if it has any records
      this.close(); // Close current HDF file
      this.initialize(splits.remove(splits.size() - 1), conf);
      moreRecordsInCurrentFile = value.hasNext();
    }
    return moreRecordsInCurrentFile;
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
   * @param s
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
          && (!skipFillValue || isFillValue(position)))
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
      shape.setValue(HDFConstants.readAsInteger(unparsedDataArray, position, valueSize));
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
  
  private boolean isFillValue(int position) {
    int sizeToCheck = valueSize;
    int b = 0;
    while (sizeToCheck > 0 && unparsedDataArray[position++] == fillValueBytes[b++])
      sizeToCheck--;
    return sizeToCheck == 0;
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
      // Convert the waterMask to a BinArray of the right size
      int size = 4800 / nasaDataset.resolution;
      BitArray waterMaskBits = convertWaterMaskToBits(ByteBuffer.wrap(waterMask), size);
      
      short fillValueShort = (short) HDFConstants.readAsInteger(fillValueBytes, 0, fillValueBytes.length);
      recoverXYShorts(ByteBuffer.wrap(unparsedDataArray), fillValueShort, waterMaskBits);
    } finally {
      if (waterMaskFile != null)
        waterMaskFile.close();
    }
  }
  
  /**
   * Converts a water mask from the byte_array format to the bit_array format.
   * In the byte array format, 0 means land, anything else means water.
   * In the bit array format, false means land and true means water.
   * Each square with side length of <code>size</code> will be converted to
   * one value in the output bit array depending on average value in this
   * square box. If at least half of the values are land (i.e., 0), the
   * corresponding value in the bit array is set to false. Otherwise, the
   * corresponding value in the bit array is set to true. 
   * @param waterMaskBytes
   * @param size
   * @return
   */
  static BitArray convertWaterMaskToBits(ByteBuffer waterMaskBytes, int size) {
    int wmRes = (int) Math.sqrt(waterMaskBytes.limit());
    int dataRes = wmRes / size;
    BitArray waterMaskBits = new BitArray(dataRes * dataRes);
    // Size of each pixel of the data when mapped to the water mask
    for (int row = 0; row < dataRes; row++)
      for (int col = 0; col < dataRes; col++) {
        int r1 = row * size;
        int r2 = (row + 1) * size;
        int c1 = col * size;
        int c2 = (col + 1) * size;
        
        byte wm_sum = 0;
        for (int r = r1; r < r2; r++)
          for (int c = c1; c < c2; c++) {
            byte wm_value = waterMaskBytes.get(r * wmRes + c);
            if (wm_value == 0)
              wm_sum++;
          }
        waterMaskBits.set(row * dataRes + col, wm_sum < (size * size) / 2);
      }
    return waterMaskBits;
  }

  /**
   * Recovers all missing entries using a two-dimensional interpolation technique.
   * @param values The dataset that need to be recovered
   * @param fillValue The marker that marks missing values
   * @param waterMask A bit-mask with <code>true</code> values in water areas
   * and <code>false</code> values for land areas.
   */
  public static void recoverXYShorts(ByteBuffer values, short fillValue, BitArray waterMask) {
    // Resolution of the dataset which is the size of each of its two dimensions
    // e.g., 1200x1200, 2400x2400, or 4800x4800
    int resolution = (int) Math.sqrt(values.limit() / 2);
    // This array stores all the runs of true (non-fill) values. The size is
    // always even where the two values point to the first and last positions
    // of the run, respectively
    ShortArray[] trueRuns = findTrueRuns(values, fillValue);

    // Now, scan the dataset column by column to recover missing values
    for (short col = 0; col < resolution; col++) {
      // Find runs of fillValues and recover all of them
      short row1 = 0;
      while (row1 < resolution) {
        // Skip as many true values as we can
        while (row1 < resolution && values.getShort(2*(row1 * resolution + col)) != fillValue)
          row1++;
        // Now, row1 points to the first fillValue
        if (row1 == resolution) {
          // All entries in the column have true values. No processing needed
          continue;
        }
        short row2 = (short) (row1 + 1);
        // Skip as many fillValues as we can
        while (row2 < resolution && values.getShort(2*(row2 * resolution + col)) == fillValue)
          row2++;
        // Now, row2 points to a true value
        
        // Offsets of the four true values to the (top, bottom, left, right)
        short[] offsetsToInterpolate = {-1, -1, -1, -1};
        short[] valuesToInterpolate = new short[4];
        if (row1 > 0) {
          offsetsToInterpolate[0] = (short) (row1 - 1);
          valuesToInterpolate[0] = values.getShort(2*(offsetsToInterpolate[0] * resolution + col));
        }
        if (row2 < resolution) {
          offsetsToInterpolate[1] = row2;
          valuesToInterpolate[1] = values.getShort(2*(offsetsToInterpolate[1] * resolution + col));
        }
        
        for (int row = row1; row < row2; row++) {
          if (values.getShort(2*(row * resolution + col)) == fillValue &&
              !waterMask.get((row * resolution + col))) {
            // The point at (row, col) is on land and has a fill (empty) value
            // Find the position of the run in this row to find points to the left and right
            int position = -trueRuns[row].binarySearch(col) - 1;
            if (position > 0) {
              // There's a true value to the left
              offsetsToInterpolate[2] = trueRuns[row].get(position-1);
              valuesToInterpolate[2] = values.getShort(2*(row * resolution + offsetsToInterpolate[2]));
            } else {
              offsetsToInterpolate[2] = -1;
            }
            if (position < trueRuns[row].size()) {
              // There's a true value to the right
              offsetsToInterpolate[3] = trueRuns[row].get(position);
              valuesToInterpolate[3] = values.getShort(2*(row * resolution + offsetsToInterpolate[3]));
            } else {
              offsetsToInterpolate[3] = -1;
            }
            short interpolatedValue = interpolatePoint(row, col, offsetsToInterpolate, valuesToInterpolate, fillValue);
            values.putShort(2*(row * resolution + col), interpolatedValue);
          }
        }
        
        // Skip the current empty run and go to the next one
        row1 = row2;
      }
    }
  }

  private static short interpolatePoint(int row, short col,
      short[] offsetsToInterpolate, short[] valuesToInterpolate, short fillValue) {
    // First interpolation value along the row
    int vr, dr;
    int d0 = row - offsetsToInterpolate[0];
    int d1 = offsetsToInterpolate[1] - row;
    if (offsetsToInterpolate[0] != -1 && offsetsToInterpolate[1] != -1) {
      // Interpolate the two values based on their distances.
      vr = (valuesToInterpolate[0] * d1 + valuesToInterpolate[1] * d0) / (d0 + d1);
      dr = (d0 + d1 + 1) / 2;
    } else if (offsetsToInterpolate[0] != -1) {
      vr = valuesToInterpolate[0];
      dr = d0;
    } else if (offsetsToInterpolate[1] != -1) {
      vr = valuesToInterpolate[1];
      dr = d1;
    } else {
      vr = 0;
      dr = 0;
    }
    // Second interpolation value along the column
    int vc, dc;
    int d2 = col - offsetsToInterpolate[2];
    int d3 = offsetsToInterpolate[3] - col;
    if (offsetsToInterpolate[2] != -1 && offsetsToInterpolate[3] != -1) {
      // Interpolate the two values based on their distances.
      vc = (valuesToInterpolate[2] * d3 + valuesToInterpolate[3] * d2) / (d2 + d3);
      dc = (d2 + d2 + 1) / 2;
    } else if (offsetsToInterpolate[2] != -1) {
      vc = valuesToInterpolate[2];
      dc = d2;
    } else if (offsetsToInterpolate[3] != -1) {
      vc = valuesToInterpolate[3];
      dc = d3;
    } else {
      vc = 0;
      dc = 0;
    }
    
    if (dr != 0 && dc != 0) {
      // Interpolate the two values based on their distances
      return (short) ((((vr * dc + vc * dr) *2) + (dr + dc)) / (2*(dr + dc)));
    }
    if (dr != 0)
      return (short) vr;
    if (dc != 0)
      return (short) vc;
    // Couldn't find any values to interpolate
    return fillValue;
  }

  /**
   * Find runs of true values in each row of the given 2D array of value.
   * @param values All the short values stored in a {@link ByteBuffer}
   * @param fillValue The marker that marks fillValue
   * @return An array of runs as one for each row in the given array.
   */
  static ShortArray[] findTrueRuns(ByteBuffer values, short fillValue) {
    int resolution = (int) Math.sqrt(values.limit() / 2);
    ShortArray[] trueRuns = new ShortArray[resolution];
    for (short row = 0; row < resolution; row++) {
      trueRuns[row] = new ShortArray();
      // A flag that is set to true if currently inside a run of fillValues.
      boolean insideFillValue = true;
      for (short col = 0; col < resolution; col++) {
        if ((values.getShort((row * resolution + col) *2) == fillValue) ^ insideFillValue) {
          // Found a flip between true and fill values.
          if (!insideFillValue && col != 0)
            trueRuns[row].append((short)(col - 1));
          else
            trueRuns[row].append(col);
          insideFillValue = !insideFillValue;
        }
      }
      if (!insideFillValue)
        trueRuns[row].append((short) (resolution - 1));
    }
    return trueRuns;
  }

}
