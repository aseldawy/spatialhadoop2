package edu.umn.cs.spatialHadoop.mapred;

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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import edu.umn.cs.spatialHadoop.core.NASADataset;
import edu.umn.cs.spatialHadoop.core.NASAPoint;

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
public class HDFRecordReader implements RecordReader<NASADataset, NASAPoint> {
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

  /**Configuration for name of the dataset to read from HDF file*/
  public static final String DatasetName = "HDFInputFormat.DatasetName";

  /**The configuration entry for skipping the fill value*/
  public static final String SkipFillValue = "HDFRecordReader.SkipFillValue";
  

  /**
   * Initializes the HDF reader to read a specific dataset from the given HDF
   * file.
   * @param job - Job configuration
   * @param split - A file split pointing to the HDF file
   * @param datasetName - Name of the dataset to read (case insensitive)
   * @throws Exception
   */
  public HDFRecordReader(Configuration job, FileSplit split, String datasetName, boolean skipFillValue) throws IOException {
    this.skipFillValue = skipFillValue;
    init(job, split, datasetName);
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
      if (fillValueStr == null) {
        this.skipFillValue = false;
      } else {
        this.fillValue = Integer.parseInt(fillValueStr);
      }
      
      dataArray = matchDataset.read();
      
      // No longer need the HDF file
      hdfFile.close();
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
  public boolean next(NASADataset key, NASAPoint point) throws IOException {
    if (dataArray == null)
      return false;
    // Key doesn't need to be changed because all points in the same dataset
    // have the same key
    while (position < Array.getLength(dataArray)) {
      // Set the x and y to longitude and latitude by doing the correct projection
      int row = position / nasaDataset.resolution;
      int col = position % nasaDataset.resolution;
      point.y = (90 - nasaDataset.v * 10) -
          (double) row * 10 / nasaDataset.resolution;
      point.x = ((nasaDataset.h * 10 - 180) +
          (double) (col) * 10 / nasaDataset.resolution) / Math.cos(point.y * Math.PI / 180);
      if (!nasaDataset.contains(point)) {
        LOG.warn("Point: "+point+" not inside dataset MBR: "+nasaDataset);
      }
      
      // Read next value
      Object value = Array.get(dataArray, position);
      if (value instanceof Integer) {
        point.value = (Integer) value;
      } else if (value instanceof Short) {
        point.value = (Short) value;
      } else if (value instanceof Byte) {
        point.value = (Byte) value;
      } else {
        throw new RuntimeException("Cannot read a value of type "+value.getClass());
      }
      position++;
      if (!skipFillValue || point.value != fillValue)
        return true;
    }
    return false;
  }

  @Override
  public NASADataset createKey() {
    return nasaDataset;
  }

  @Override
  public NASAPoint createValue() {
    return new NASAPoint();
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

  public static void main(String[] args) throws Exception {
    FileSplit hdfFileSplit = new FileSplit(
        new Path("http://e4ftl01.cr.usgs.gov/MOLT/MOD11A1.005/2000.03.05/MOD11A1.A2000065.h35v08.005.2007176170906.hdf"),
        0, 0, new String[] {});
    HDFRecordReader reader = new HDFRecordReader(new Configuration(),
        hdfFileSplit, "LST_Night_1km", true);
    
    NASADataset dataset = reader.createKey();
    NASAPoint point = reader.createValue();
    
    System.out.println(dataset);
    
    int count = 0;
    while (reader.next(dataset, point)) {
      count++;
    }
    
    System.out.println("Read "+count+" values");
    
    reader.close();
  }
}
