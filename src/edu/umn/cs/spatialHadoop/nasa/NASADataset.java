package edu.umn.cs.spatialHadoop.nasa;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import ncsa.hdf.object.Attribute;
import ncsa.hdf.object.Group;
import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * A class that contains meta information about a NASA dataset extracted from
 * an HDF file. This information is extracted from the meta data in the HDF
 * file and includes:
 *  - Spatial boundaries of the dataset
 *  - Date time on which the data was taken
 *  - Name of the cell in the Sinusoidal grid
 *  - Name of the dataset in the HDF file
 *  - Resolution of the dataset
 * This class extends {@link Rectangle} to make it compatible to use as a key
 * with SpatialInputFormat
 * @author Ahmed Eldawy
 *
 */
public class NASADataset extends Rectangle {
  /**Time instance of this dataset as millis since the epoch*/
  public long time;
  
  /**Name of the cell in the sinusoidal grid (h:xx v:xx)*/
  public String cellName;
  
  /**Cell coordinates in MODIS Sinusoidal grid*/
  public int h, v;
  
  /**Name of the dataset as it appears in the HDF file*/
  public String datasetName;
  
  /**Resolution of the dataset in terms of number of rows/columns*/
  public int resolution;
  
  /**Minimum and maximum values for this dataset as stored in its metadata*/
  public int minValue, maxValue;
  
  public NASADataset() {}
  
  /**
   * Inititalizes the dataset from the metadata stored at the root of an HDF
   * file.
   * @param root
   */
  public NASADataset(Group root) {
    try {
      Map<String, Object> metadata = parseMetadata(root.getMetadata());
      String resolution = findMetadata(metadata, "StructMetadata.0/GridStructure/GRID_1/XDim");
      this.resolution = Integer.parseInt(resolution);

      String date = findMetadata(metadata, "CoreMetadata.0/INVENTORYMETADATA/RANGEDATETIME/RANGEBEGINNINGDATE/VALUE");
      String time = findMetadata(metadata, "CoreMetadata.0/INVENTORYMETADATA/RANGEDATETIME/RANGEBEGINNINGTIME/VALUE");
      final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      this.time = dateFormat.parse(date+" "+time).getTime();
      String granuleId = findMetadata(metadata, "CoreMetadata.0/INVENTORYMETADATA/ECSDATAGRANULE/LOCALGRANULEID/VALUE");
      this.cellName = granuleId.split("\\.")[2];
      this.h = Integer.parseInt(this.cellName.substring(1, 3));
      this.v = Integer.parseInt(this.cellName.substring(4, 6));

      String north = findMetadata(metadata, "ArchiveMetadata.0/ARCHIVEDMETADATA/BOUNDINGRECTANGLE/NORTHBOUNDINGCOORDINATE/VALUE");
      String south = findMetadata(metadata, "ArchiveMetadata.0/ARCHIVEDMETADATA/BOUNDINGRECTANGLE/SOUTHBOUNDINGCOORDINATE/VALUE");
      String west = findMetadata(metadata, "ArchiveMetadata.0/ARCHIVEDMETADATA/BOUNDINGRECTANGLE/WESTBOUNDINGCOORDINATE/VALUE");
      String east = findMetadata(metadata, "ArchiveMetadata.0/ARCHIVEDMETADATA/BOUNDINGRECTANGLE/EASTBOUNDINGCOORDINATE/VALUE");
      super.set(Double.parseDouble(west), Double.parseDouble(south),
          Double.parseDouble(east), Double.parseDouble(north));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public String toString() {
    String formattedTime = SimpleDateFormat.getInstance().format(new Date(time));
    return datasetName+": "+super.toString()+" ("+cellName+") @"+formattedTime+"-"+resolution;
  }
  
  public static Map<String, Object> parseMetadata(List<Attribute> attrs) {
    // Keep a lineage of all open groups or objects
    Stack<Map<String,Object>> lineage = new Stack<Map<String,Object>>();
    // Push the root group that contains all top-level data
    lineage.push(new Hashtable<String, Object>());
    
    for (Attribute attr : attrs) {
      String attr_name = attr.getName();
      Object values = attr.getValue();
      if (values instanceof String[]) {
        String metadata = ((String[])values)[0];
        
        Map<String, Object> metadataHash = new Hashtable<String, Object>();
        lineage.peek().put(attr_name, metadataHash);
        lineage.push(metadataHash);
        
        int start = 0;
        int end = metadata.length();
        while (start < end) {
          int line_end = metadata.indexOf('\n', start);
          if (line_end == -1)
            line_end = end;
          if (line_end > end)
            throw new RuntimeException("End is in the middle of a line");
          // Each line is in the format <key>=<value>
          int separatorIndex = metadata.indexOf('=', start);
          if (separatorIndex > start && separatorIndex < line_end) {
            String key = metadata.substring(start, separatorIndex).trim();
            String value = metadata.substring(separatorIndex+1, line_end).trim();
            if (key.equals("GROUP") || key.equals("OBJECT")) {
              String type = key;
              String name = value;
              // Identify the end of this group and parse it recursively
              // Identify the line that contains END_GROUP
              int group_end = metadata.indexOf("END_"+type+"="+name, start);
              // Group end is the first new line character after GROUP_END keyword
              group_end = metadata.indexOf('\n', group_end);
              if (group_end == -1)
                group_end = end;
              if (group_end > end)
                throw new RuntimeException("END_"+type+" spans beyond the end");
              Hashtable<String, Object> groupData = new Hashtable<String, Object>();
              lineage.peek().put(name, groupData);
              lineage.push(groupData);
            } else if (key.equals("END_GROUP") || key.equals("END_OBJECT")) {
              // TODO check that name matches with the open group or object as a
              // sanity check
              lineage.pop();
            } else {
              // This is a simple key/value pair
              if (value.startsWith("\"") || value.startsWith("\'")) {
                value = value.substring(1, value.lastIndexOf(value.charAt(0)));
              }
              lineage.peek().put(key, value);
            }
          }
          start = line_end + 1;
        }
        
        lineage.pop();
        
        if (lineage.size() != 1)
          throw new RuntimeException("Error parsing metadata: "+metadata);
      }

    }
    
    return lineage.peek();
  }
  

  public static String findMetadata(Map<String, Object> metadata, String key) {
    String[] parts = key.split("\\/");
    for (String part : parts) {
      Object value = metadata.get(part);
      if (value == null)
        return null;
      if (value instanceof String)
        return (String) value;
      metadata = (Hashtable<String, Object>) value; 
    }
    return null;
  }
  
}