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

import java.io.IOException;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.util.FileUtil;


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
  
  /**Array of values in the dataset being read*/
  private Object dataArrayTemp;
  
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

  /**Configuration line for the path to water mask*/
  private static final String WATER_MASK_PATH = "HDFRecordReader.WaterMaskPath";
  
  /** corner case for cover holes in x directions*/
  private static int corner_case_x = 0;
  
  /** corner case for cover holes in y directions*/
  private static int corner_case_y = 0;

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
//    	Class<? extends GeoProjector> projectorClass =  MercatorProjector.class;
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
      String localFile = FileUtil.copyFileSplit(job, split);
      
      // retrieve an instance of H4File
      FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);

      hdfFile = fileFormat.createInstance(localFile, FileFormat.READ);

      // open the file and retrieve the file structure
      hdfFile.open();

      // Retrieve the root of the HDF file structure
      Group root =
          (Group)((DefaultMutableTreeNode)hdfFile.getRootNode()).getUserObject();

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
      
      this.value = (NASAShape) SpatialSite.createStockShape(job);
      
      if (fillValueStr == null) {
        this.skipFillValue = false;
      } else {
        this.fillValue = Integer.parseInt(fillValueStr);
      }

      dataArray = matchDataset.read();
      
      // Recover holes if asked by user
      if (job.getBoolean("recoverholes", false)) {
        // Read water mask
        if (job.get(WATER_MASK_PATH) == null) {
          LOG.warn("Could not find water mask files");
        } else {
          Path wmPath = new Path(job.get(WATER_MASK_PATH));
          final String tileIdentifier = String.format("h%02dv%02d", nasaDataset.h, nasaDataset.v);
          FileSystem wmFs = wmPath.getFileSystem(job);
          FileStatus[] wmFile = wmFs.listStatus(wmPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
              return path.getName().contains(tileIdentifier);
            }});
          if (wmFile.length == 0) {
            LOG.warn("Could not find water mask for tile '"+tileIdentifier+"'");
          } else {
            localFile = FileUtil.copyFile(job, wmFile[0]);
            FileFormat wmHDFFile = fileFormat.createInstance(localFile, FileFormat.READ);
            wmHDFFile.open();
            
            root =
                (Group)((DefaultMutableTreeNode)wmHDFFile.getRootNode()).getUserObject();
            
            // Search for the datset of interest in file
            matchDataset = findDataset(root, "water_mask", false);

            if (matchDataset == null) {
              LOG.warn("Water mask dataset "+datasetName+" not found in file "+split.getPath());
            } else {
              // Do interpolation
              byte[] water_mask = (byte[]) matchDataset.read();
              // Go over this image row by row
              corner_case_x=0;
              corner_case_y=0;
              coverHoles(water_mask);
              System.out.println(corner_case_x+" , "+corner_case_y);
              if(corner_case_x==nasaDataset.resolution&&corner_case_y==nasaDataset.resolution){
            	  // TODO all data set is empty.. check surrounding datasets. 
            	  System.out.println("corner case");
            	  Object[] surroundingDataSet = new Object[4];
            	  int[] dh=new int[]{0,0,-1,1};
            	  int[] dv=new int[]{1,-1,0,0};
            	  for (int i = 0; i < dv.length; i++) {
            		  int h,v;
            		  h=nasaDataset.h+dh[i];
            		  v=nasaDataset.v+dv[i];
            		  surroundingDataSet[i]=ReadDataSet(h,v,split, job, datasetName);
            		  System.out.println(surroundingDataSet[i]);
            	  }
            	  
					if(surroundingDataSet[0]!=null){ // h, v+1 the down cell .. copy the up row to the down row
						System.out.println("h , v+1 ");
						
						int offset=(nasaDataset.resolution*nasaDataset.resolution)-nasaDataset.resolution;
						for (int index = 0; index < nasaDataset.resolution; index++) {
							Object value = null;
							int i=0;
							
							while(i<nasaDataset.resolution&&( notFillValue(value=Array.get(surroundingDataSet[0],index+(i*nasaDataset.resolution))) ))i++;
							
							Array.set(dataArray, offset+index, value);
						}
					}
					
					if(surroundingDataSet[1]!=null){ // h, v-1 the up cell .. copy the down row to the up row
						System.out.println("h , v-1 ");

						int offset=(nasaDataset.resolution*nasaDataset.resolution)-nasaDataset.resolution;
						for (int index = 0; index < nasaDataset.resolution; index++) {
							Object value = null;
							int i=0;
							while(i<nasaDataset.resolution&&(notFillValue( value=Array.get(surroundingDataSet[1],offset+index-(i*nasaDataset.resolution)))))i++;
							
							Array.set(dataArray, index, value );
						}
					}
					
					if(surroundingDataSet[2]!=null){ // h-1, v the left cell .. copy the right column to left column
						System.out.println("h-1 , v ");

						
						int offset=nasaDataset.resolution;
						for (int index = 0; index < nasaDataset.resolution; index++) {
							Object value = null;
							int i=1;
							while(i<nasaDataset.resolution&&(notFillValue( value=Array.get(surroundingDataSet[2],(offset*index)+offset-i))))i++;
							Array.set(dataArray, offset*index,value);
						}
					}
					
					if(surroundingDataSet[3]!=null){ // h+1, v the right cell .. copy the left column to the right column
						System.out.println("h+1 , v ");

						
						int offset=nasaDataset.resolution;
						for (int index = 0; index < nasaDataset.resolution; index++) {
							Object value =null;
							int i=0;
							while(i<nasaDataset.resolution && notFillValue(value=Array.get(surroundingDataSet[3],offset*index+i)))i++;
							Array.set(dataArray, (offset*index)+offset-1, value);
						}
					}
					
            	  for (int i = 0; i < nasaDataset.resolution*nasaDataset.resolution; i++) {
					Array.set(dataArrayTemp, i, Array.get(dataArray, i));
            	  }
            	  

            	  
            	  coverHoles(water_mask);
            	  
            	  coverHoles(water_mask);
              }
              else{
            	  coverHoles(water_mask);
              }
            }
            
            wmHDFFile.close();
          }
          
        }
      }

      // No longer need the HDF file
      hdfFile.close();
      fileFormat.close();
    } catch (Exception e) {
      throw new RuntimeException("Error reading HDF file '"+split.getPath()+"'", e);
    }
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


  private boolean notFillValue(Object object) {
	// TODO Auto-generated method stub
	  if (object instanceof Integer) {
	      return (Integer)object==fillValue;
	    } else if (object instanceof Short) {
	    	return (Short)object==fillValue;
	    } else {
	      throw new RuntimeException("Values of type "+object.getClass()+" is not supported yet");
	    }
}

private Object ReadDataSet(int h, int v, FileSplit split, Configuration job,String datasetName) throws OutOfMemoryError, Exception {
	// TODO Auto-generated method stub
	  String localFile = null;
      FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);
	  Group root;
	  Stack<Group> groups2bSearched;
	  Dataset matchDataset;
	  
	  
	  Path wmPath = split.getPath().getParent();
      final String tileIdentifier = String.format("h%02dv%02d", h, v);
      FileSystem wmFs = wmPath.getFileSystem(job);
      FileStatus[] wmFile = wmFs.listStatus(wmPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().contains(tileIdentifier);
          
        }});
      if (wmFile.length == 0) {
        LOG.warn("Could not find dataset for tile '"+tileIdentifier+"'");
        wmFile = wmFs.listStatus(wmPath.getParent(), new PathFilter() {
            @Override
            public boolean accept(Path path) {
              return path.getName().contains(tileIdentifier);
              
            }});
        if(wmFile.length==0){
        	 LOG.warn("Could not find dataset for tile '"+tileIdentifier+"'");
        	 return null;
        }
        localFile=FileUtil.copyFile(job, wmFile[0]);
        
      } else {
        localFile = FileUtil.copyFile(job, wmFile[0]);
      }
        FileFormat wmHDFFile = fileFormat.createInstance(localFile, FileFormat.READ);
        wmHDFFile.open();
        
        root =
            (Group)((DefaultMutableTreeNode)wmHDFFile.getRootNode()).getUserObject();
        
        // Search for the datset of interest in file
        groups2bSearched = new Stack<Group>();
        groups2bSearched.add(root);

        matchDataset = null;

        while (!groups2bSearched.isEmpty()) {
          Group top = groups2bSearched.pop();
          List<HObject> memberList = top.getMemberList();

          for (HObject member : memberList) {
            if (member instanceof Group) {
              groups2bSearched.add((Group) member);
            } else if (member instanceof Dataset) {
              if (member.getName().equalsIgnoreCase(datasetName)) {
                matchDataset = (Dataset) member;
                break;
              }
            }
          }
        }

        if (matchDataset == null) {
//          LOG.warn("Water mask dataset "+datasetName+" not found in file "+split.getPath());
        } else {
        	
         return matchDataset.read();
         
        }
      
      return null;
}

private void coverHoles(byte[] water_mask) {
	// TODO Auto-generated method stub
	  for (int y = 0; y < nasaDataset.resolution; y++) {
          // First empty point in current run
          int x1 = 0;
          // First non-empty point in next run (last empty point + 1)
          int x2 = 0;
          while (x2 < nasaDataset.resolution) {
            // Detect next run of empty points
            x1 = x2;
            // Find first point that needs to be recovered
            while (x1 < nasaDataset.resolution && !needsToBeRecovered(dataArray, x1, y, nasaDataset.resolution, null)) x1++;
            
            x2 = x1; // start of needed to be recovered in X1
            
            while (x2 < nasaDataset.resolution && needsToBeRecovered(dataArray, x2, y, nasaDataset.resolution, null)) x2++;
            
            // now X2 contains the end of to be recovered.
            
            
            if (x1 == 0 && x2 < nasaDataset.resolution || x1 > 0 && x1 < nasaDataset.resolution && x2 == nasaDataset.resolution) {
              // Only one point at one end is inside image boundaries.
              // Use the available point to paint all missing points
              Object valueToBeReplicated = Array.get(dataArray, y * nasaDataset.resolution + (x1 == 0? x2 : (x1-1)));
              
              int startOffsetToBeReplicated = y * nasaDataset.resolution + x1;
              int endOffsetToBeReplicated = y * nasaDataset.resolution + x2;
              
              for (int offsetToBeReplicated = startOffsetToBeReplicated; offsetToBeReplicated < endOffsetToBeReplicated; offsetToBeReplicated++) {
                if (needsToBeRecovered(dataArray, x1 + (offsetToBeReplicated - startOffsetToBeReplicated), y, nasaDataset.resolution, water_mask))
                  Array.set(dataArrayTemp, offsetToBeReplicated, valueToBeReplicated);
              }
            } else if (x1 > 0 && x2 < nasaDataset.resolution) {
              // Two ends are available, interpolate to recover points
              Object value1 = Array.get(dataArray, y * nasaDataset.resolution + x1-1);
              Object value2 = Array.get(dataArray, y * nasaDataset.resolution + x2);
              
              // Recover all missing points that should be recovered in this run
              int startOffsetToBeInterpolated = y * nasaDataset.resolution + x1;
              int endOffsetToBeInterpolated = y * nasaDataset.resolution + x2;
              for (int offsetToBeInterpolated = startOffsetToBeInterpolated; offsetToBeInterpolated < endOffsetToBeInterpolated; offsetToBeInterpolated++) {
                if (needsToBeRecovered(dataArray, x1 + (offsetToBeInterpolated - startOffsetToBeInterpolated), y, nasaDataset.resolution, water_mask)) {
                  Object interpolatedValue = interpolate(value1, value2, startOffsetToBeInterpolated, endOffsetToBeInterpolated, offsetToBeInterpolated);
                  Array.set(dataArrayTemp, offsetToBeInterpolated, interpolatedValue);
                }
              }
            }
            else if ( x1 == 0 && x2 == nasaDataset.resolution){
            	corner_case_x++;
            }
          }
        }
        
        for (int x = 0 ; x< nasaDataset.resolution ; x++){

            // First empty point in current run
            int y1 = 0;
            // First non-empty point in next run (last empty point + 1)
            int y2 = 0;
            while (y2 < nasaDataset.resolution) {
              // Detect next run of empty points
              y1 = y2;
              // Find first point that needs to be recovered
              while (y1 < nasaDataset.resolution && !needsToBeRecovered(dataArray, x, y1, nasaDataset.resolution, null)) y1++;
              
              y2 = y1; // start of needed to be recovered in Y1
              
              while (y2 < nasaDataset.resolution && needsToBeRecovered(dataArray, x, y2, nasaDataset.resolution, null)) y2++;
              
              // now Y2 contains the end of to be recovered.
              
              
              if (y1 == 0 && y2 < nasaDataset.resolution || y1 > 0 && y1 < nasaDataset.resolution && y2 == nasaDataset.resolution) {
                // Only one point at one end is inside image boundaries.
                // Use the available point to paint all missing points
                Object valueToBeReplicated = Array.get(dataArray, x + ( nasaDataset.resolution * (y1 == 0? y2 : (y1-1))));
                
                int startOffsetToBeReplicated = y1 * nasaDataset.resolution + x;
                int endOffsetToBeReplicated = y2 * nasaDataset.resolution + x;
                int offsetVertically=0;
                
                for (int offsetToBeReplicated = startOffsetToBeReplicated; offsetToBeReplicated < endOffsetToBeReplicated; offsetToBeReplicated+=nasaDataset.resolution) {
                  if (needsToBeRecovered(dataArray, x,y1 + (offsetVertically), nasaDataset.resolution, water_mask))
                  {
                  	Object newValue = null;
                  	
                  	 if (needsToBeRecovered(dataArrayTemp, x,y1 + (offsetVertically), nasaDataset.resolution, water_mask)){
                  		 // this means it is not handled in first iteration.
                  		 newValue=valueToBeReplicated;
                  	 }
                  	 else{
                  		 // need to take average between x iteration and y iteration
                  		 Object value1= Array.get(dataArrayTemp, offsetToBeReplicated);
                       	
                       	if (value1 instanceof Integer) {
                       		newValue= (Integer) (int)( (((Integer)value1) + ((Integer)valueToBeReplicated)) /2);
                       	} else if (value1 instanceof Short) {
                       		newValue= (Short)  (short)((((Short)value1) + ((Short)valueToBeReplicated)) /2);	
                       	} else {
                       	      throw new RuntimeException("Cannot interpolate values of type '"+value1.getClass()+"'");
                       	}
                  	 }
                  	
                  	
                      Array.set(dataArray, offsetToBeReplicated, newValue);
                  }
                  offsetVertically++;
                }
              } else if (y1 > 0 && y2 < nasaDataset.resolution) {
                // Two ends are available, interpolate to recover points
                Object value1 = Array.get(dataArray, x + ( nasaDataset.resolution * (y1-1)));
                Object value2 = Array.get(dataArray, x + ( nasaDataset.resolution * y2));
                
                // Recover all missing points that should be recovered in this run
                int startOffsetToBeInterpolated = x + ( nasaDataset.resolution * y1);
                int endOffsetToBeInterpolated = x +  ( nasaDataset.resolution * y2);
                int offsetVertically =0;
                
                for (int offsetToBeInterpolated = startOffsetToBeInterpolated; offsetToBeInterpolated < endOffsetToBeInterpolated; offsetToBeInterpolated+=nasaDataset.resolution) {
                  if (needsToBeRecovered(dataArray, x,y1 + (offsetVertically), nasaDataset.resolution, water_mask)) {
                  	
                    Object interpolatedValue = interpolate(value1, value2, startOffsetToBeInterpolated, endOffsetToBeInterpolated, offsetToBeInterpolated);
                    
                    Object newValue = null;
                	
                    if (needsToBeRecovered(dataArrayTemp, x, y1 + (offsetVertically), nasaDataset.resolution, water_mask)) {
               		 // this means it is not handled in first iteration.
               		 newValue=interpolatedValue;
               	 }
               	 else{
               		 // need to take average between x iteration and y iteration
              		 Object valuetemp= Array.get(dataArrayTemp, offsetToBeInterpolated);
                   	
                   	if (valuetemp instanceof Integer) {
                   		newValue=  (Integer)(int)( (((Integer)valuetemp) + ((Integer)interpolatedValue)) /2);
                   	} else if (value1 instanceof Short) {
                   		newValue= (Short)(short)((((Short)valuetemp) + ((Short)interpolatedValue)) /2);	
                   	} else {
                   	      throw new RuntimeException("Cannot interpolate values of type '"+value1.getClass()+"'");
                   	}
               	 }
                    
                    Array.set(dataArray, offsetToBeInterpolated, newValue);
                  }
                  offsetVertically++;
                }
              }
              else if ( y1 == 0 && y2 == nasaDataset.resolution){
              	corner_case_y++;
              	// we need to replace all values in arraytemp here.
              	int startOffsetToBeReplicated = x + ( nasaDataset.resolution * y1);
                int endOffsetToBeReplicated = x +  ( nasaDataset.resolution * y2);
              	int offsetVertically=0;
              	for (int offsetToBeReplicated = startOffsetToBeReplicated; offsetToBeReplicated < endOffsetToBeReplicated; offsetToBeReplicated+=nasaDataset.resolution) {
                    if (needsToBeRecovered(dataArray, x,y1 + (offsetVertically), nasaDataset.resolution, water_mask)) {
                    	Array.set(dataArray, offsetToBeReplicated, Array.get(dataArrayTemp, offsetToBeReplicated));
                    }
                    offsetVertically++;
                }
              }
            }
        }
	
}

private Object interpolate(Object value1, Object value2,
      int startOffsetToBeInterpolated, int endOffsetToBeInterpolated,
      int offsetToBeInterpolated) {
    float position = ((float) (offsetToBeInterpolated - startOffsetToBeInterpolated)) / (endOffsetToBeInterpolated - startOffsetToBeInterpolated);
    if (value1 instanceof Integer) {
      return (Integer)(int)(((Integer)value1) * (1.0f - position) + ((Integer) value2) * position);
    } else if (value1 instanceof Short) {
      return (Short)(short)(((Short)value1) * (1.0f - position) + ((Short) value2) * position);
    } else {
      throw new RuntimeException("Cannot interpolate values of type '"+value1.getClass()+"'");
    }
  }

  private boolean needsToBeRecovered(Object dataArray, int x, int y,
      int resolution, byte[] water_mask) {
    int offset = y * resolution + x;
    Object value = Array.get(dataArray, offset);
    boolean valueIsEmpty;
    if (value instanceof Integer) {
      valueIsEmpty = ((Integer)value) == fillValue;
    } else if (value instanceof Short) {
      valueIsEmpty = ((Short)value) == fillValue;
    } else {
      throw new RuntimeException("Values of type "+value.getClass()+" is not supported yet");
    }
    if (!valueIsEmpty)
      return false;
    // If water mask is null, this means we want to check only for dataArray
    if (water_mask == null)
      return true;
    int wm_x = x * 4800 / resolution;
    int wm_y = y * 4800 / resolution;
    int size = 4800 / resolution;
    byte wm_sum = 0;
    for (int xx = 0; xx < size; xx++) {
      for (int yy = 0; yy < size; yy++) {
        byte wm_value = water_mask[(yy +wm_y) * 4800 + (xx+wm_x)];
        if (wm_value == 1)
          wm_sum++; // water
      }
    }
    if (wm_sum < (size * size) / 2)
      return true;
    return false;
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
      shape.setTimestamp(key.time);
      
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

  public static void main(String[] args) throws IOException {
    Configuration job = new Configuration();
    Path file = new Path("temperature/MOD11A1.A2000065.h21v06.005.2007176155240.hdf");
    FileSystem fs = file.getFileSystem(job);
    FileStatus fstatus = fs.getFileStatus(file);
    job.set(WATER_MASK_PATH, "water_mask");
    job.setBoolean("recoverholes", true);
    job.setClass("shape", NASAPoint.class, Shape.class);
    HDFRecordReader reader = new HDFRecordReader(job, new FileSplit(file, 0, fstatus.getLen(), new String[0]), "LST_Day_1km", true);
    NASADataset key = reader.createKey();
    NASAShape value = reader.createValue();
    int count = 0;
    while (reader.next(key, value)) {
      count++;
    }
    System.out.println(count);
    reader.close();
  }
}
