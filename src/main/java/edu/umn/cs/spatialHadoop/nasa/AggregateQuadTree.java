/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.awt.Point;
import java.awt.Rectangle;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Stack;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.hdf.DDNumericDataGroup;
import edu.umn.cs.spatialHadoop.hdf.DDVDataHeader;
import edu.umn.cs.spatialHadoop.hdf.DDVGroup;
import edu.umn.cs.spatialHadoop.hdf.DataDescriptor;
import edu.umn.cs.spatialHadoop.hdf.HDFFile;
import edu.umn.cs.spatialHadoop.io.RandomCompressedInputStream;
import edu.umn.cs.spatialHadoop.io.RandomCompressedOutputStream;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

/**
 * A structure that stores all lookup tables needed to construct and work
 * with a quad tree.
 * @author Ahmed Eldawy
 *
 */
class StockQuadTree {
  
  /**
   * Stores information about a single node in the stock quad tree.
   * @author Ahmed Eldawy
   *
   */
  static class Node {
    /** A unique identifier for this node in the tree */
    int id;
    /** Position of first value under this node */
    int startPosition;
    /** Position after the last value under this node */
    int endPosition;
    
    /** Returns number of elements stored in the subtree rooted by this node */
    public int getNumOfElements() {
      return endPosition - startPosition;
    }
  }

  /** The resolution of the tile associated with this quad tree*/
  int resolution;
  
  /** The row-wise position of each value in the sorted values */
  int[] r;
  
  /** The ID of each node */
  int[] nodesID;
  
  /** The start position of each node in the list of values sorted by Z */
  int[] nodesStartPosition;
  
  /** The end position of each node in the list of values sorted by Z */
  int[] nodesEndPosition;
  
  /**
   * Constructs a stock quad tree for the given resolution
   * @param resolution
   */
  StockQuadTree(int resolution) {
    this.resolution = resolution;
    this.r = new int[resolution * resolution];
    final int[] z = new int[resolution * resolution];
    // The list of all nodes
    Vector<Node> nodes = new Vector<Node>();
    
    // Compute the Z-order of all values
    for (int i = 0; i < z.length; i++) {
      short x = (short) (i % resolution);
      short y = (short) (i / resolution);
      int zorder = AggregateQuadTree.computeZOrder(x, y);
      z[i] = zorder;
      r[i] = i;
    }
    
    // Sort ArrayToZOrder1200 by Z-Order and keep the original position of
    // each element by mirroring all swaps to ZOrderToArray1200
    new QuickSort().sort(new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        int temp;
        // Swap z-values (which are to be sorted)
        temp = z[i];
        z[i] = z[j];
        z[j] = temp;
        
        // Swap their relative positions in the other array
        temp = r[i];
        r[i] = r[j];
        r[j] = temp;
      }
      
      @Override
      public int compare(int i, int j) {
        return z[i] - z[j];
      }
    }, 0, z.length);
    
    // Construct the structure of the quad tree based on Z-values
    // Maximum number of values per node. Set it to a very small number to
    // construct as many levels as possible. Notice that when quad trees
    // are aggregated, a single value might become 366 values in the same pos.
    final int capacity = 100;
    Node root = new Node();
    root.startPosition = 0;
    root.endPosition = z.length;
    root.id = 1;
    Queue<Node> nodesToCheckForSplit = new ArrayDeque<Node>();
    nodesToCheckForSplit.add(root);
    int numOfSignificantBitsInTree = getNumOfSignificantBits(resolution * resolution - 1);
    if ((numOfSignificantBitsInTree & 1) == 1)
      numOfSignificantBitsInTree++; // Round to next even value
    int maxId = 0;
    while (!nodesToCheckForSplit.isEmpty()) {
      Node nodeToCheckForSplit = nodesToCheckForSplit.poll();
      boolean needsToSplit = nodeToCheckForSplit.getNumOfElements() > capacity;
      if (nodeToCheckForSplit.id > maxId)
        maxId = nodeToCheckForSplit.id;
      nodes.add(nodeToCheckForSplit);
      if (needsToSplit) {
        // Need to split
        // Determine split points based on the Z-order values of the first and
        // last elements in this node
        int depth = nodeToCheckForSplit.id == 0 ? 0 :
          (getNumOfSignificantBits(nodeToCheckForSplit.id - 1) / 2 + 1);
        depth = (getNumOfSignificantBits(nodeToCheckForSplit.id) - 1) / 2;
        int numOfSignificantBitsInNode = numOfSignificantBitsInTree - depth * 2;
        
        // Create four child nodes under this node
        int zOrderCommonBits = z[nodeToCheckForSplit.startPosition] & (0xffffffff << numOfSignificantBitsInNode);
        int childStartPosition = nodeToCheckForSplit.startPosition;
        for (int iChild = 0; iChild < 4; iChild++) {
          int zOrderUpperBound = zOrderCommonBits + ((iChild + 1) << (numOfSignificantBitsInNode - 2));
          int childEndPosition = Arrays.binarySearch(z, childStartPosition, nodeToCheckForSplit.endPosition, zOrderUpperBound);
          if (childEndPosition < 0)
            childEndPosition = -(childEndPosition + 1);
          Node child = new Node();
          child.startPosition = childStartPosition;
          child.endPosition = childEndPosition;
          child.id = nodeToCheckForSplit.id * 4 + iChild;
          nodesToCheckForSplit.add(child);
          // Prepare for next iteration
          childStartPosition = childEndPosition;
        }
        if (childStartPosition != nodeToCheckForSplit.endPosition)
          throw new RuntimeException();
      }
    }
    // Convert nodes to column format for memory efficiency
    nodesID = new int[nodes.size()];
    nodesStartPosition = new int[nodes.size()];
    nodesEndPosition = new int[nodes.size()];
    
    for (int i = 0; i < nodes.size(); i++) {
      Node node = nodes.get(i);
      nodesID[i] = node.id;
      nodesStartPosition[i] = node.startPosition;
      nodesEndPosition[i] = node.endPosition;
    }
  }
  
  /**
   * Returns number of significant bits in an integer.
   * number of significant bits = 32 - number of leading zeros
   * @param x
   * @return
   */
  private static int getNumOfSignificantBits(int x) {
    int n;
    
    if (x == 0) return(0);
    n = 0;
    if ((x & 0xFFFF0000) == 0) {n = n +16; x = x <<16;}
    if ((x & 0xFF000000) == 0) {n = n + 8; x = x << 8;}
    if ((x & 0xF0000000) == 0) {n = n + 4; x = x << 4;}
    if ((x & 0xC0000000) == 0) {n = n + 2; x = x << 2;}
    if ((x & 0x80000000) == 0) {n = n + 1;}
    return 32 - n;
  }
  
  /**
   * Retrieves the minimal bounding rectangle of the node with the given ID.
   * Fills in the given rectangle with the MBR if the node is found and returns
   * true, otherwise, it leaves the given mbr unchanged and returns false;
   * @param node_id
   * @param mbr
   * @return
   */
  public void getNodeMBR(int node_pos, java.awt.Rectangle mbr) {
    if (this.nodesStartPosition[node_pos] >= this.r.length) {
      // Special case for a node that falls completely outside data range
      mbr.width = mbr.height = 0;
    } else {
      mbr.x = this.r[this.nodesStartPosition[node_pos]] % resolution;
      mbr.y = this.r[this.nodesStartPosition[node_pos]] / resolution;
      int x2 = (this.r[this.nodesEndPosition[node_pos] - 1]) % resolution;
      mbr.width = x2 - mbr.x + 1;
      int y2 = (this.r[this.nodesEndPosition[node_pos] - 1]) / resolution;
      mbr.height = y2 - mbr.y + 1;
    }
  }

  /**
   * Retrieves the coordinates of a record in the original unindexed
   * two-dimensional array.
   * @param record_pos
   * @param record_coords
   */
  public void getRecordCoords(int record_pos, Point record_coords) {
    record_coords.x = this.r[record_pos] % resolution;
    record_coords.y = this.r[record_pos] / resolution;
  }
}

/**
 * Stores a quad tree of values collected from HDF files from MODIS archive.
 * 
 * @author Ahmed Eldawy
 */
public class AggregateQuadTree {
  private static final Log LOG = LogFactory.getLog(AggregateQuadTree.class);
  
  /**
   * Stock quad trees of all supported sizes (resolutions).
   */
  final static Map<Integer, StockQuadTree> StockQuadTrees = new HashMap<Integer, StockQuadTree>();
  
  static StockQuadTree getOrCreateStockQuadTree(int resolution) {
    StockQuadTree stockTree = StockQuadTrees.get(resolution);
    if (stockTree == null) {
      synchronized(StockQuadTrees) {
        stockTree = StockQuadTrees.get(resolution);
        if (stockTree == null) {
          LOG.info("Creating a stock quad tree of size "+resolution);
          stockTree = new StockQuadTree(resolution);
          StockQuadTrees.put(resolution, stockTree);
          LOG.info("Done creating the stock quad tree of size "+resolution);
        }
      }
    }
    return stockTree;
  }
  
  public static class Node implements Writable {
    public short min = Short.MAX_VALUE, max = Short.MIN_VALUE;
    public long sum = 0;
    public long count = 0;
    
    /**
     * Accumulate the values of another node
     * @param other
     */
    public void accumulate(Node other) {
      if (other.min < this.min)
        this.min = other.min;
      if (other.max > this.max)
        this.max = other.max;
      this.sum += other.sum;
      this.count += other.count;
    }
    
    /**
     * Accumulate a single value
     * @param value
     */
    public void accumulate(short value) {
      if (value < this.min)
        this.min = value;
      if (value > this.max)
        this.max = value;
      this.sum += value;
      this.count++;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.min = in.readShort();
      this.max = in.readShort();
      this.sum = in.readLong();
      this.count = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(min);
      out.writeShort(max);
      out.writeLong(sum);
      out.writeLong(count);
    }
    
    @Override
    public String toString() {
      return String.format("Sum: %d, Count: %d, Min: %d, Max: %d, Avg: %g",
          sum, count, min, max, (double)sum / count);
    }
  }
  
  /**Tree header size = resolution + fillValue + cardinality*/
  private static final int TreeHeaderSize = 4 + 2 + 4;
  /**Value size = short*/
  private static final int ValueSize = 2;
  /**Node size = min + max + sum + count*/
  private static final int NodeSize = 2 + 2 + 8 + 8;
  
  /**
   * Constructs an aggregate quad tree for an input HDF file on a selected
   * dataset identified by its name in the file.
   * @param conf The system configuration which can contain user-defined parameters.
   * @param inFile The path of the input HDF file to read
   * @param datasetName The name of the dataset to index in the HDF file
   * @param outFile The path to the index file to write
   * @throws IOException If an error happens while reading the input or writing the output
   */
  public static void build(Configuration conf, Path inFile, String datasetName,
      Path outFile) throws IOException {
    FileSystem inFs = inFile.getFileSystem(conf);
    if (inFs instanceof HTTPFileSystem) {
      // HDF files are really bad to read over HTTP due to seeks
      inFile = new Path(FileUtil.copyFile(conf, inFile));
      inFs = FileSystem.getLocal(conf);
    }
    HDFFile hdfFile = null;
    try {
      hdfFile = new HDFFile(inFs.open(inFile));
      DDVGroup dataGroup = hdfFile.findGroupByName(datasetName);

      if (dataGroup == null) 
        throw new RuntimeException("Cannot find dataset '"+datasetName+"' in file "+inFile);
      
      boolean fillValueFound = false;
      short fillValue = 0;
      short[] values = null;
      for (DataDescriptor dd : dataGroup.getContents()) {
        if (dd instanceof DDNumericDataGroup) {
          DDNumericDataGroup numericDataGroup = (DDNumericDataGroup) dd;
          values = (short[])numericDataGroup.getAsTypedArray();
        } else if (dd instanceof DDVDataHeader) {
          DDVDataHeader vheader = (DDVDataHeader) dd;
          if (vheader.getName().equals("_FillValue")) {
            fillValue = (short)(int)(Integer)vheader.getEntryAt(0);
            fillValueFound = true;
          }
        }
      }
      
      // Retrieve meta data
      String archiveMetadata = (String) hdfFile.findHeaderByName("ArchiveMetadata.0").getEntryAt(0);
      String coreMetadata = (String) hdfFile.findHeaderByName("CoreMetadata.0").getEntryAt(0);
      NASADataset nasaDataset = new NASADataset(coreMetadata, archiveMetadata);

      if (values instanceof short[]) {
        FileSystem outFs = outFile.getFileSystem(conf);
        DataOutputStream out = new DataOutputStream(
            new RandomCompressedOutputStream(outFs.create(outFile, false)));
        build(nasaDataset, (short[])values, fillValue, out);
        out.close();
      } else {
        throw new RuntimeException("Indexing of values of type "
            + "'" + Array.get(values, 0).getClass()+"' is not supported");
      }
    } finally {
      if (hdfFile != null)
        hdfFile.close();
    }
  }
  
  /**
   * Constructs an aggregate quad tree out of a two-dimensional array of values.
   * 
   * @param values
   * @param out
   *          - the output stream to write the constructed quad tree to
   * @throws IOException
   */
  public static void build(NASADataset metadata, short[] values,
      short fillValue, DataOutputStream out) throws IOException {
    int length = Array.getLength(values);
    int resolution = (int) Math.round(Math.sqrt(length));

    // Write tree header
    out.writeInt(resolution); // resolution
    out.writeShort(fillValue);
    out.writeInt(1); // cardinality
    out.writeLong(metadata.time); // Timestamp

    // Fetch the stock quad tree of the associated resolution
    StockQuadTree stockQuadTree = getOrCreateStockQuadTree(resolution);
    // Sort values by their respective Z-Order values in linear time
    short[] sortedValues = new short[length];
    for (int i = 0; i < length; i++)
      sortedValues[i] = values[stockQuadTree.r[i]];

    // Write all sorted values
    for (short v : sortedValues)
      out.writeShort(v);

    // Compute aggregate values for all nodes in the tree
    // Go in reverse ID order to ensure children are computed before parents
    Node[] nodes = new Node[stockQuadTree.nodesID.length];
    for (int iNode = stockQuadTree.nodesID.length - 1; iNode >= 0 ; iNode--) {
      // Initialize all aggregate values
      nodes[iNode] = new Node();
      
      int firstChildId = stockQuadTree.nodesID[iNode] * 4;
      int firstChildPos = Arrays.binarySearch(stockQuadTree.nodesID, firstChildId);
      boolean isLeaf = firstChildPos < 0;
      
      if (isLeaf) {
        for (int iVal = stockQuadTree.nodesStartPosition[iNode]; iVal < stockQuadTree.nodesEndPosition[iNode]; iVal++) {
          short value;
          Object val = Array.get(sortedValues, iVal);
          if (val instanceof Short) {
            value = (Short) val;
          } else {
            throw new RuntimeException("Cannot handle values of type "+val.getClass());
          }
          if (value != fillValue)
            nodes[iNode].accumulate(value);
        }
      } else {
        // Compute from the four children
        for (int iChild = 0; iChild < 4; iChild++) {
          int childPos = firstChildPos + iChild;
          nodes[iNode].accumulate(nodes[childPos]);
        }
      }
    }

    // Write nodes to file in sorted order
    for (int iNode = 0; iNode < nodes.length; iNode++)
      nodes[iNode].write(out);
  }
  
  /**
   * Returns the position of the values section relative to the beginning of the
   * tree in the file. To get the absolute position of the values section, you
   * will need to add tree start position in the file. To get the absolute
   * position of a specific value, you need to add (value_pos * ValueSize) to
   * the absolute position of the values section, where value_pos is the
   * position of the value you want to read in the list of all values sorted by
   * their respective Z-order in the file.
   * 
   * @param resolution
   * @return
   */
  private static long getValuesStartOffset(int cardinality) {
    // Skip tree header and timestamps
    return TreeHeaderSize + cardinality * 8;
  }
  
  /**
   * Returns the position of the nodes section in the tree file relative to the
   * beginning of file. To get the absolute position you need to add the start
   * position of the tree in the file. To get the absolute position of a
   * specific node, you need to add (node_pos * NodeSize) to the absolute
   * position of the nodes section.
   * @param cardinality 
   * 
   * @param node_pos
   * @return
   */
  private static long getNodesStartOffset(int resolution, int cardinality) {
    return TreeHeaderSize + cardinality * 8 + resolution * resolution * cardinality * ValueSize;
  }

  /**
   * Merges a list of aggregate trees (in the same order) and writes an output tree
   * that combines all input trees. Input trees must have been built using on of the
   * {@link #build} methods. The merged tree can be further merged using this method.
   * @param conf
   * @param inFiles
   * @param outFile
   * @throws IOException
   */
  public static void merge(Configuration conf, Path[] inFiles, Path outFile) throws IOException {
    DataInputStream[] inTrees = new DataInputStream[inFiles.length];
    for (int i = 0; i < inFiles.length; i++) {
      FileSystem inFs = inFiles[i].getFileSystem(conf);
      inTrees[i] = new FSDataInputStream(
          new RandomCompressedInputStream(inFs, inFiles[i]));
    }
    
    FileSystem outFs = outFile.getFileSystem(conf);
    DataOutputStream outTree = new DataOutputStream(
        new RandomCompressedOutputStream(outFs.create(outFile, false))); 
    
    merge(inTrees, outTree);
    
    for (int i = 0; i < inFiles.length; i++)
      inTrees[i].close();
    outTree.close();
  }
  
  /**
   * Merges multiple trees of the same spatial resolution into one tree of
   * lower temporal resolution (larger time) and the same spatial resolution.
   * @param inTrees
   * @param outTree
   * @throws IOException 
   */
  public static void merge(DataInputStream[] inTrees, DataOutputStream outTree)
      throws IOException {
    // Write the spatial resolution of the output as the same of all input trees
    int resolution = inTrees[0].readInt();
    short fillValue = inTrees[0].readShort();
    for (int iTree = 1; iTree < inTrees.length; iTree++) {
      int iResolution = inTrees[iTree].readInt();
      int iFillValue = inTrees[iTree].readShort();
      if (resolution != iResolution || fillValue != iFillValue)
        throw new RuntimeException("Tree #0 has a resolution of "+resolution
            +" not compatible with resolution"+iResolution+" of Tree #"+iTree);
    }
    outTree.writeInt(resolution);
    outTree.writeShort(fillValue);
    
    // Sum up the cardinality of all input trees
    int cardinality = 0;
    int cardinalities[] = new int[inTrees.length];
    for (int iTree = 0; iTree < inTrees.length; iTree++)
      cardinality += (cardinalities[iTree] = inTrees[iTree].readInt());
    outTree.writeInt(cardinality);
    
    // Write timestamps of all trees
    for (int iTree = 0; iTree < inTrees.length; iTree++) {
      outTree.writeLong(inTrees[iTree].readLong());
    }
    
    // Merge sorted values in all input trees
    byte[] buffer = new byte[1024*1024];
    int size = resolution * resolution;
    while (size-- > 0) {
      for (int iTree = 0; iTree < inTrees.length; iTree++) {
        int sizeToRead = ValueSize * cardinalities[iTree]; // sizeof(short) * c
        while (sizeToRead > 0) {
          int bytesRead = inTrees[iTree].read(buffer, 0,
              Math.min(sizeToRead, buffer.length));
          outTree.write(buffer, 0, bytesRead);
          sizeToRead -= bytesRead;
        }
      }
    }
    
    // Merge aggregate values of all nodes
    Node treeNode = new Node();
    StockQuadTree stockQuadTree = getOrCreateStockQuadTree(resolution);
    int numOfNodes = stockQuadTree.nodesID.length;
    for (int iNode = 0; iNode < numOfNodes; iNode++) {
      Node outputNode = new Node();
      for (int iTree = 0; iTree < inTrees.length; iTree++) {
        treeNode.readFields(inTrees[iTree]);
        outputNode.accumulate(treeNode);
      }
      outputNode.write(outTree);
    }
  }
  
  /**
   * A class that holds the value of a point in the tree. Used to report the
   * answer of selection queries with all information.
   * @author Ahmed Eldawy
   *
   */
  @SuppressWarnings("serial")
  public static class PointValue extends java.awt.Point {
    /**Value at this point*/
    public int value;
    
    /**Timestamp of the point*/
    public long timestamp;
  }

  /**
   * Performs a range query on an aggregate
   * @param fs
   * @param p
   * @param query_mbr
   * @param output
   * @return
   * @throws IOException
   */
  public static int selectionQuery(FileSystem fs, Path p, Rectangle query_mbr,
      ResultCollector<PointValue> output) throws IOException {
    FSDataInputStream inStream = null;
    try {
      inStream = new FSDataInputStream(new RandomCompressedInputStream(fs, p));
      //inStream = fs.open(p);
      return selectionQuery(inStream, query_mbr, output);
    } finally {
      if (inStream != null)
        inStream.close();
    }

  }
  
  /**
   * Perform a selection query that retrieves all points in the given range.
   * The range is specified in the two-dimensional array positions.
   * @param in
   * @param query_mbr
   * @param output
   * @return number of matched records
   * @throws IOException
   */
  public static int selectionQuery(FSDataInputStream in, Rectangle query_mbr,
      ResultCollector<PointValue> output) throws IOException {
    long treeStartPosition = in.getPos();
    int numOfResults = 0;
    int resolution = in.readInt();
    short fillValue = in.readShort();
    int cardinality = in.readInt();
    long[] timestamps = new long[cardinality];
    for (int i = 0; i < cardinality; i++)
      timestamps[i] = in.readLong();
    Vector<Integer> selectedStarts = new Vector<Integer>();
    Vector<Integer> selectedEnds = new Vector<Integer>();
    StockQuadTree stockQuadTree = getOrCreateStockQuadTree(resolution);
    // Nodes to be searched. Contains node positions in the array of nodes
    Stack<Integer> nodes_2b_searched = new Stack<Integer>();
    nodes_2b_searched.add(0); // Root node (ID=1)
    Rectangle node_mbr = new Rectangle();
    while (!nodes_2b_searched.isEmpty()) {
      int node_pos = nodes_2b_searched.pop();
      stockQuadTree.getNodeMBR(node_pos, node_mbr);
      if (query_mbr.contains(node_mbr)) {
        // Add this node to the selection list and stop this branch
        if (!selectedEnds.isEmpty()
            && selectedEnds.lastElement() == stockQuadTree.nodesStartPosition[node_pos]) {
          // Merge with an adjacent range
          selectedEnds.set(selectedEnds.size() - 1, stockQuadTree.nodesEndPosition[node_pos]);
        } else {
          // add a new range
          selectedStarts.add(stockQuadTree.nodesStartPosition[node_pos]);
          selectedEnds.add(stockQuadTree.nodesEndPosition[node_pos]);
        }
        numOfResults += stockQuadTree.nodesEndPosition[node_pos]
            - stockQuadTree.nodesStartPosition[node_pos];
      } else if (query_mbr.intersects(node_mbr)) {
        int first_child_id = stockQuadTree.nodesID[node_pos] * 4 + 0;
        int first_child_pos = Arrays.binarySearch(stockQuadTree.nodesID, first_child_id);
        if (first_child_pos < 0) {
          // No children. Hit a leaf node
          // Scan and add matching points only
          java.awt.Point record_coords = new Point();
          for (int record_pos = stockQuadTree.nodesStartPosition[node_pos];
              record_pos < stockQuadTree.nodesEndPosition[node_pos]; record_pos++) {
            stockQuadTree.getRecordCoords(record_pos, record_coords);
            if (query_mbr.contains(record_coords)) {
              // matched a record.
              if (!selectedEnds.isEmpty()
                  && selectedEnds.lastElement() == record_pos) {
                // Merge with an adjacent range
                selectedEnds.set(selectedEnds.size() - 1, record_pos + 1);
              } else {
                // Add a new range of unit width
                selectedStarts.add(record_pos);
                selectedEnds.add(record_pos+1);
              }
              numOfResults++;
            }
          }
        } else {
          // Non-leaf node. Add all children to the list of nodes to search
          // Add in reverse order to the stack so that results come in sorted order
          nodes_2b_searched.add(first_child_pos+3);
          nodes_2b_searched.add(first_child_pos+2);
          nodes_2b_searched.add(first_child_pos+1);
          nodes_2b_searched.add(first_child_pos+0);
        }
      }
    }
    if (output != null) {
      PointValue returnValue = new PointValue();
      long dataStartPosition = treeStartPosition + getValuesStartOffset(cardinality);
      // Return all values in the selected ranges
      for (int iRange = 0; iRange < selectedStarts.size(); iRange++) {
        int treeStart = selectedStarts.get(iRange);
        int treeEnd = selectedEnds.get(iRange);
        long startPosition = dataStartPosition
            + selectedStarts.get(iRange) * cardinality * 2;
        in.seek(startPosition);
        for (int treePos = treeStart; treePos < treeEnd; treePos++) {
          // Retrieve the coords for the point at treePos
          stockQuadTree.getRecordCoords(treePos, returnValue);
          // Read all entries at current position
          for (int iValue = 0; iValue < cardinality; iValue++) {
            short value = in.readShort();
            if (value != fillValue) {
              returnValue.value = value;
              returnValue.timestamp = timestamps[iValue];
              output.collect(returnValue);
            }
          }
        }
      }
    }
    return numOfResults;
  }
  
  
  public static Node aggregateQuery(FileSystem fs, Path p, Rectangle query_mbr) throws IOException {
    FSDataInputStream inStream = null;
    try {
      inStream = new FSDataInputStream(new RandomCompressedInputStream(fs, p));
      //inStream = fs.open(p);
      return aggregateQuery(inStream, query_mbr);
    } finally {
      if (inStream != null)
        inStream.close();
    }
  }
  
  /**
   * Perform a selection query that retrieves all points in the given range.
   * The range is specified in the two-dimensional array positions. 
   * @param in
   * @param query_mbr
   * @return
   * @throws IOException
   */
  public static Node aggregateQuery(FSDataInputStream in, Rectangle query_mbr) throws IOException {
    long treeStartPosition = in.getPos();
    Node result = new Node();
    int numOfSelectedRecords = 0;
    int resolution = in.readInt();
    short fillValue = in.readShort();
    int cardinality = in.readInt();
    final Vector<Integer> selectedNodesPos = new Vector<Integer>();
    final Vector<Integer> selectedStarts = new Vector<Integer>();
    final Vector<Integer> selectedEnds = new Vector<Integer>();
    StockQuadTree stockQuadTree = getOrCreateStockQuadTree(resolution);
    // Nodes to be searched. Contains node positions in the array of nodes
    Stack<Integer> nodes_2b_searched = new Stack<Integer>();
    nodes_2b_searched.add(0); // Root node (ID=1)
    Rectangle node_mbr = new Rectangle();
    while (!nodes_2b_searched.isEmpty()) {
      int node_pos = nodes_2b_searched.pop();
      stockQuadTree.getNodeMBR(node_pos, node_mbr);
      if (query_mbr.contains(node_mbr)) {
        // Add this node to the selection list and stop this branch
        selectedNodesPos.add(node_pos);
      } else if (query_mbr.intersects(node_mbr)) {
        int first_child_id = stockQuadTree.nodesID[node_pos] * 4 + 0;
        int first_child_pos = Arrays.binarySearch(stockQuadTree.nodesID, first_child_id);
        if (first_child_pos < 0) {
          // No children. Hit a leaf node
          // Scan and add matching points only
          java.awt.Point record_coords = new Point();
          for (int record_pos = stockQuadTree.nodesStartPosition[node_pos];
              record_pos < stockQuadTree.nodesEndPosition[node_pos]; record_pos++) {
            stockQuadTree.getRecordCoords(record_pos, record_coords);
            if (query_mbr.contains(record_coords)) {
              // matched a record.
              if (!selectedEnds.isEmpty()
                  && selectedEnds.lastElement() == record_pos) {
                // Merge with an adjacent range
                selectedEnds.set(selectedEnds.size() - 1, record_pos + 1);
              } else {
                // Add a new range of unit width
                selectedStarts.add(record_pos);
                selectedEnds.add(record_pos+1);
              }
              numOfSelectedRecords++;
            }
          }
        } else {
          // Non-leaf node. Add all children to the list of nodes to search
          // Add in reverse order to the stack so that results come in sorted order
          nodes_2b_searched.add(first_child_pos+3);
          nodes_2b_searched.add(first_child_pos+2);
          nodes_2b_searched.add(first_child_pos+1);
          nodes_2b_searched.add(first_child_pos+0);
        }
      }
    }
    // Result 1: Accumulate all values
    // Sort disk offsets to eliminate backward seeks
    if (!selectedStarts.isEmpty()) {
      LOG.debug("Aggregate query selected "+selectedNodesPos.size()
          +" nodes and "+numOfSelectedRecords+" records");
      
      final IndexedSortable sortable = new IndexedSortable() {
        @Override
        public int compare(int i, int j) {
          return selectedStarts.get(i) - selectedStarts.get(j);
        }
        
        @Override
        public void swap(int i, int j) {
          int temp = selectedStarts.get(i);
          selectedStarts.set(i, selectedStarts.get(j));
          selectedStarts.set(j, temp);
          
          temp = selectedEnds.get(i);
          selectedEnds.set(i, selectedEnds.get(j));
          selectedEnds.set(j, temp);
        }
      };
      new QuickSort().sort(sortable, 0, selectedStarts.size());
      
      long dataStartPosition = getValuesStartOffset(cardinality);
      Point resultCoords = new Point();
      // Return all values in the selected ranges
      for (int iRange = 0; iRange < selectedStarts.size(); iRange++) {
        int treeStart = selectedStarts.get(iRange);
        int treeEnd = selectedEnds.get(iRange);
        long startPosition = dataStartPosition
            + selectedStarts.get(iRange) * cardinality * 2;
        in.seek(startPosition);
        for (int treePos = treeStart; treePos < treeEnd; treePos++) {
          // Retrieve the coords for the point at treePos
          stockQuadTree.getRecordCoords(treePos, resultCoords);
          // Read all entries at current position
          for (int iValue = 0; iValue < cardinality; iValue++) {
            short value = in.readShort();
            if (value != fillValue)
              result.accumulate(value);
          }
        }
      }
      
    }
    
    // Result 2: Accumulate all nodes
    if (!selectedNodesPos.isEmpty()) {
      long nodesStartPosition = treeStartPosition + getNodesStartOffset(resolution, cardinality);
      // Sort node positions to eliminate backward seeks
      IndexedSortable nodeSortable = new IndexedSortable() {
        @Override
        public int compare(int i, int j) {
          return selectedNodesPos.get(i) - selectedNodesPos.get(j);
        }
        
        @Override
        public void swap(int i, int j) {
          int temp = selectedNodesPos.get(i);
          selectedNodesPos.set(i, selectedNodesPos.get(j));
          selectedNodesPos.set(j, temp);
        }
      };
      new QuickSort().sort(nodeSortable, 0, selectedNodesPos.size());
      
      Node selectedNode = new Node();
      for (int node_pos : selectedNodesPos) {
        long nodePosition = nodesStartPosition + node_pos * NodeSize;
        in.seek(nodePosition);
        selectedNode.readFields(in);
        result.accumulate(selectedNode);
      }
    }
    return result;
  }
  
  /**
   * Computes the Z-order (Morton order) of a two-dimensional point.
   * @param x
   * @param y
   * @return
   */
  public static int computeZOrder(short x, short y) {
    int morton = 0;
  
    for (int i = 0; i < 16; i++) {
      int mask = 1 << i;
      morton += (x & mask) << (i + 1);
      morton += (y & mask) << i;
    }
    return morton;
  }

  /**
   * Creates a full spatio-temporal hierarchy for a source folder
   * @throws ParseException 
   * @throws InterruptedException 
   */
  public static void directoryIndexer(final OperationsParams params)
      throws IOException, ParseException, InterruptedException {
    Path inputDir = params.getInputPath();
    FileSystem sourceFs = inputDir.getFileSystem(params);
    final Path sourceDir = inputDir.makeQualified(sourceFs);
    Path destDir = params.getOutputPath();
    final FileSystem destFs = destDir.getFileSystem(params);
    
    TimeRange timeRange = params.get("time") != null? new TimeRange(params.get("time")) : null;
    
    // Create daily indexes that do not exist
    final Path dailyIndexDir = new Path(destDir, "daily");
    FileStatus[] mathcingDays = timeRange == null?
        sourceFs.listStatus(inputDir) :
          sourceFs.listStatus(inputDir, timeRange);
    final Vector<Path> sourceFiles = new Vector<Path>();
    for (FileStatus matchingDay : mathcingDays) {
      for (FileStatus matchingTile : sourceFs.listStatus(matchingDay.getPath())) {
        sourceFiles.add(matchingTile.getPath());
      }

    }
    // Shuffle the array for better load balancing across threads
    Collections.shuffle(sourceFiles);
    final String datasetName = params.get("dataset");
    Parallel.forEach(sourceFiles.size(), new RunnableRange<Object>() {
      @Override
      public Object run(int i1, int i2) {
        LOG.info("Worker ["+i1+","+i2+") started");
        for (int i = i1; i < i2; i++) {
          Path sourceFile = sourceFiles.get(i);
          try {
            Path relativeSourceFile = makeRelative(sourceDir, sourceFile);
            Path destFilePath = new Path(dailyIndexDir, relativeSourceFile);
            if (!destFs.exists(destFilePath)) {
              LOG.info("Worker ["+i1+","+i2+") indexing: "+sourceFile.getName());
              Path tmpFile;
              do {
                tmpFile = new Path((int)(Math.random()* 1000000)+".tmp");
              } while (destFs.exists(tmpFile));
              tmpFile = tmpFile.makeQualified(destFs);
              if (datasetName == null)
                throw new RuntimeException("Please provide the name of dataset you would like to index");
              AggregateQuadTree.build(params, sourceFile, datasetName,
                  tmpFile);
              synchronized (destFs) {
                Path destDir = destFilePath.getParent();
                if (!destFs.exists(destDir))
                  destFs.mkdirs(destDir);
              }
              destFs.rename(tmpFile, destFilePath);
            }
          } catch (IOException e) {
            throw new RuntimeException("Error building an index for "+sourceFile, e);
          }
        }
        LOG.info("Worker ["+i1+","+i2+") finished");
        return null;
      }
      
    });
    LOG.info("Done generating daily indexes");
    
    // Merge daily indexes into monthly indexes
    Path monthlyIndexDir = new Path(destDir, "monthly");
    final SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy.MM.dd");
    final SimpleDateFormat monthFormat = new SimpleDateFormat("yyyy.MM");
    mergeIndexes(destFs, dailyIndexDir, monthlyIndexDir, dayFormat, monthFormat, params);
    LOG.info("Done generating monthly indexes");
    
    // Merge daily indexes into monthly indexes
    Path yearlyIndexDir = new Path(destDir, "yearly");
    final SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
    mergeIndexes(destFs, monthlyIndexDir, yearlyIndexDir, monthFormat, yearFormat, params);
    LOG.info("Done generating yearly indexes");
  }
  
  /**
   * Merges a set of indexes into larger indexes
   * @param fs
   * @param srcIndexDir
   * @param dstIndexDir
   * @param srcFormat
   * @param dstFormat
   * @param params
   * @throws IOException
   * @throws ParseException
   * @throws InterruptedException
   */
  private static void mergeIndexes(final FileSystem fs, Path srcIndexDir,
      Path dstIndexDir, SimpleDateFormat srcFormat, SimpleDateFormat dstFormat,
      final OperationsParams params)
      throws IOException, ParseException, InterruptedException {
    TimeRange timeRange = params.get("time") != null? new TimeRange(params.get("time")) : null;
    final FileStatus[] sourceIndexes = timeRange == null?
        fs.listStatus(srcIndexDir) : fs.listStatus(srcIndexDir, timeRange);
    Arrays.sort(sourceIndexes); // Alphabetical sort acts as sort-by-date here
    
    // Scan the source indexes and merge each consecutive run belonging to the
    // same unit
    int i1 = 0;
    while (i1 < sourceIndexes.length) {
      final String indexToCreate =
          dstFormat.format(srcFormat.parse(sourceIndexes[i1].getPath().getName()));
      int i2 = i1+1;
      // Keep scanning as long as the source index belongs to the same dest index
      while (i2 < sourceIndexes.length &&
          dstFormat.format(srcFormat.parse(sourceIndexes[i2].getPath().getName())).equals(indexToCreate))
        i2++;

      // Merge all source indexes in the range [i1, i2) into one dest index
      
      // Copy i1, i2 to other variables as final to be accessible from threads
      final int firstIndex = i1;
      final int lastIndex = i2;
      
      final Path destIndex = new Path(dstIndexDir, indexToCreate);
      
      // For each tile, merge all values in all source indexes
      /*A regular expression to catch the tile identifier of a MODIS grid cell*/
      final Pattern MODISTileID = Pattern.compile("^.*(h\\d\\dv\\d\\d).*$"); 
      final FileStatus[] tilesInFirstDay = fs.listStatus(sourceIndexes[i1].getPath());
      // Shuffle the array for better load balancing across threads
      Random rand = new Random();
      for (int i = 0; i < tilesInFirstDay.length - 1; i++) {
        // Swap the entry at i with any following entry
        int j = i + rand.nextInt(tilesInFirstDay.length - i - 1);
        FileStatus temp = tilesInFirstDay[i];
        tilesInFirstDay[i] = tilesInFirstDay[j];
        tilesInFirstDay[j] = temp;
      }
      Parallel.forEach(tilesInFirstDay.length, new RunnableRange<Object>() {
        @Override
        public Object run(int i_file1, int i_file2) {
          for (int i_file = i_file1; i_file < i_file2; i_file++) {
            try {
              FileStatus tileInFirstDay = tilesInFirstDay[i_file];
              
              // Extract tile ID
              Matcher matcher = MODISTileID.matcher(tileInFirstDay.getPath().getName());
              if (!matcher.matches()) {
                LOG.warn("Cannot extract tile id from file "+tileInFirstDay.getPath());
                continue;
              }

              final String tileID = matcher.group(1);
              Path destIndexFile = new Path(destIndex, tileID);
              
              PathFilter tileFilter = new PathFilter() {
                @Override
                public boolean accept(Path path) {
                  return path.getName().contains(tileID);
                }
              };

              // Find matching tiles in all source indexes to merge
              Vector<Path> filesToMerge = new Vector<Path>(lastIndex - firstIndex);
              filesToMerge.add(tileInFirstDay.getPath());
              for (int iDailyIndex = firstIndex + 1; iDailyIndex < lastIndex; iDailyIndex++) {
                FileStatus[] matchedTileFile = fs.listStatus(sourceIndexes[iDailyIndex].getPath(), tileFilter);
                if (matchedTileFile.length == 0)
                  LOG.warn("Could not find tile "+tileID+" in dir "+sourceIndexes[iDailyIndex].getPath());
                else if (matchedTileFile.length == 1)
                  filesToMerge.add(matchedTileFile[0].getPath());
              }
              
              if (fs.exists(destIndexFile)) {
                // Destination file already exists
                // Check the date of the destination and source files to see
                // whether it needs to be updated or not
                long destTimestamp = fs.getFileStatus(destIndexFile).getModificationTime();
                boolean needsUpdate = false;
                for (Path fileToMerge : filesToMerge) {
                  long sourceTimestamp = fs.getFileStatus(fileToMerge).getModificationTime();
                  if (sourceTimestamp > destTimestamp) {
                    needsUpdate = true;
                    break;
                  }
                }
                if (!needsUpdate)
                  continue;
                else
                  LOG.info("Updating file "+destIndexFile.getName());
              }

              // Do the merge
              Path tmpFile;
              do {
                tmpFile = new Path((int)(Math.random()* 1000000)+".tmp");
              } while (fs.exists(tmpFile));
              tmpFile = tmpFile.makeQualified(fs);
              LOG.info("Merging tile "+tileID+" into file "+destIndexFile);
              AggregateQuadTree.merge(params,
                  filesToMerge.toArray(new Path[filesToMerge.size()]), tmpFile);
              synchronized (fs) {
                Path destDir = destIndexFile.getParent();
                if (!fs.exists(destDir))
                  fs.mkdirs(destDir);
              }
              fs.rename(tmpFile, destIndexFile);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          return null;
        }
      });
      i1 = i2;
    }
  }

  /**
   * Make a path relative to another path by removing all common ancestors
   * @param parent
   * @param descendant
   * @return
   */
  private static Path makeRelative(Path parent, Path descendant) {
    Stack<String> components = new Stack<String>();
    while (descendant.depth() > parent.depth()) {
      components.push(descendant.getName());
      descendant = descendant.getParent();
    }
    if (!descendant.equals(parent))
      throw new RuntimeException("descendant not a child of parent");
    if (components.isEmpty())
      return new Path(".");
    Path relative = new Path(components.pop());
    while (!components.isEmpty())
      relative = new Path(relative, components.pop());
    return relative;
  }
  
  public static int getResolution(FileSystem fs, Path p) throws IOException {
    FSDataInputStream inStream = null;
    try {
      inStream = new FSDataInputStream(new RandomCompressedInputStream(fs, p));
      //inStream = fs.open(p);
      int resolution = inStream.readInt();
      return resolution;
    } finally {
      if (inStream != null)
        inStream.close();
    }
  }

  public static void main(String[] args) throws IOException, ParseException, InterruptedException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
    directoryIndexer(params);
  }
}

