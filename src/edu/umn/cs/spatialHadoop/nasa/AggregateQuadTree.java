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

import java.awt.Point;
import java.awt.Rectangle;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.Vector;

import javax.swing.tree.DefaultMutableTreeNode;

import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.util.FileUtil;

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
    System.out.println("number of nodes in "+resolution+" is "+nodes.size());
    System.out.println("Max ID in "+resolution+" is "+maxId);
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
    mbr.x = this.r[this.nodesStartPosition[node_pos]] % resolution;
    mbr.y = this.r[this.nodesStartPosition[node_pos]] / resolution;
    int x2 = (this.r[this.nodesEndPosition[node_pos] - 1]) % resolution;
    mbr.width = x2 - mbr.x + 1;
    int y2 = (this.r[this.nodesEndPosition[node_pos] - 1]) / resolution;
    mbr.height = y2 - mbr.y + 1;
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
      LOG.info("Creating a stock quad tree of size "+resolution);
      stockTree = new StockQuadTree(resolution);
      StockQuadTrees.put(resolution, stockTree);
      LOG.info("Done creating the stock quad tree of size "+resolution);
    }
    return stockTree;
  }
  
  public static class Node implements Writable {
    public short min = Short.MAX_VALUE, max = Short.MIN_VALUE;
    public int sum = 0, count = 0;
    
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
      this.sum = in.readInt();
      this.count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(min);
      out.writeShort(max);
      out.writeInt(sum);
      out.writeInt(count);
    }
  }
  
  private static final int TreeHeaderSize = 4 + 4;
  private static final int ValueSize = 2;
  private static final int NodeSize = 2 + 2 + 4 + 4;
  
  /**
   * Constructs an aggregate quad tree for an input HDF file on a selected
   * dataset identified by its name in the file.
   * @param inFile
   * @param datasetIndex
   * @param outFile
   * @throws Exception 
   */
  public static void build(Configuration conf, Path inFile, String datasetName,
      Path outFile) throws Exception {
    String localFile = FileUtil.copyFile(conf, inFile);
    // retrieve an instance of H4File format
    FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);

    FileFormat hdfFile = fileFormat.createInstance(localFile, FileFormat.READ);

    // open the file and retrieve the file structure
    hdfFile.open();
    
    Group root =
        (Group)((DefaultMutableTreeNode)hdfFile.getRootNode()).getUserObject();
    Dataset matchedDataset = HDFRecordReader.findDataset(root, datasetName, false);
    if (matchedDataset != null) {
      Object values = matchedDataset.read();
      if (values instanceof short[]) {
        FileSystem outFs = outFile.getFileSystem(conf);
        FSDataOutputStream out = outFs.create(outFile, false);
        build((short[])values, out);
        out.close();
      } else {
        throw new RuntimeException("Indexing of values of type "
            + "'" + Array.get(values, 0).getClass()+"' is not supported");
      }
    }
    hdfFile.close();
  }
  
  /**
   * Constructs an aggregate quad tree out of a two-dimensional array
   * of values.
   * @param values
   * @param out - the output stream to write the constructed quad tree to
   * @throws IOException 
   */
  public static void build(short[] values, DataOutputStream out) throws IOException {
    int length = Array.getLength(values);
    int resolution = (int) Math.round(Math.sqrt(length));

    // Write tree header
    out.writeInt(resolution); // resolution
    out.writeInt(1); // cardinality

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
  private static long getValuesStartOffset() {
    return TreeHeaderSize;
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
    return TreeHeaderSize + resolution * resolution * cardinality * ValueSize;
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
      inTrees[i] = inFs.open(inFiles[i]);
    }
    
    FileSystem outFs = outFile.getFileSystem(conf);
    DataOutputStream outTree = outFs.create(outFile, false);
    
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
    for (int iTree = 1; iTree < inTrees.length; iTree++) {
      int iResolution = inTrees[iTree].readInt();
      if (resolution != iResolution)
        throw new RuntimeException("Tree #0 has a resolution of "+resolution
            +" not compatible with resolution"+iResolution+" of Tree #"+iTree);
    }
    outTree.writeInt(resolution);
    
    // Sum up the cardinality of all input trees
    int cardinality = 0;
    int cardinalities[] = new int[inTrees.length];
    for (int iTree = 0; iTree < inTrees.length; iTree++)
      cardinality += (cardinalities[iTree] = inTrees[iTree].readInt());
    outTree.writeInt(cardinality);
    
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
   * Perform a selection query that retrieves all points in the given range.
   * The range is specified in the two-dimensional array positions.
   * @param in
   * @param r
   * @return number of matched records
   * @throws IOException 
   */
  public static int selectionQuery(FSDataInputStream in, Rectangle query_mbr,
      ResultCollector2<Point, Short> output) throws IOException {
    long treeStartPosition = in.getPos();
    int numOfResults = 0;
    int resolution = in.readInt();
    int cardinality = in.readInt();
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
      Point resultCoords = new Point();
      long dataStartPosition = treeStartPosition + getValuesStartOffset();
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
            output.collect(resultCoords, value);
          }
        }
      }
    }
    return numOfResults;
  }
  
  
  /**
   * Perform a selection query that retrieves all points in the given range.
   * The range is specified in the two-dimensional array positions.
   * @param in
   * @param r
   * @return number of matched records
   * @throws IOException 
   */
  public static Node aggregateQuery(FSDataInputStream in, Rectangle query_mbr) throws IOException {
    long treeStartPosition = in.getPos();
    Node result = new Node();
    int numOfSelectedRecords = 0;
    int resolution = in.readInt();
    int cardinality = in.readInt();
    Vector<Integer> selectedNodesPos = new Vector<Integer>();
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
    LOG.info("Aggregate query selected "+selectedNodesPos.size()
        +" nodes and "+numOfSelectedRecords+" records");
    // Result 1: Accumulate all values
    long dataStartPosition = getValuesStartOffset();
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
          result.accumulate(value);
        }
      }
    }
    
    // Result 2: Accumulate all nodes
    long nodesStartPosition = treeStartPosition + getNodesStartOffset(resolution, cardinality);
    Node selectedNode = new Node();
    for (int node_pos : selectedNodesPos) {
      long nodePosition = nodesStartPosition + node_pos * NodeSize;
      in.seek(nodePosition);
      selectedNode.readFields(in);
      result.accumulate(selectedNode);
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

  public static void main(String[] args) throws IllegalArgumentException, Exception {
    long t1, t2;
    
    AggregateQuadTree.getOrCreateStockQuadTree(1200);
    t1 = System.currentTimeMillis();
    AggregateQuadTree.build(new Configuration(), new Path("MYD11A1.A2014219.h21v06.005.2014220235833.hdf"), "LST_Day_1km", new Path("indexed.hdf"));
    t2 = System.currentTimeMillis();
    System.out.println("Elapsed time "+(t2-t1)+" millis");
    
    Path[] inFiles = new Path[10];
    for (int i = 0; i < inFiles.length; i++)
      inFiles[i] = new Path("indexed.hdf");
    t1 = System.currentTimeMillis();
    merge(new Configuration(), inFiles, new Path("merged.hdf"));
    t2 = System.currentTimeMillis();
    System.out.println("Elapsed time for merge "+(t2-t1)+" millis");

    inFiles = new Path[10];
    for (int i = 0; i < inFiles.length; i++)
      inFiles[i] = new Path("merged.hdf");
    t1 = System.currentTimeMillis();
    merge(new Configuration(), inFiles, new Path("further-merged.hdf"));
    t2 = System.currentTimeMillis();
    System.out.println("Elapsed time for merge "+(t2-t1)+" millis");

    
    if (true)
      return;
    

    short[] values = new short[1200 * 1200];
    for (int i = 0; i < values.length; i++) {
      short x = (short) (i % 1200);
      short y = (short) (i / 1200);
      values[i] = (short) (x * 10000 + y);
    }

    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("test.quad")));
    t1 = System.currentTimeMillis();
    AggregateQuadTree.build(values, out);
    out.close();
    t2 = System.currentTimeMillis();
    System.out.println("Elapsed time "+(t2-t1)+" millis");
    
    DataInputStream[] inTrees = new DataInputStream[365];
    for (int iTree = 0; iTree < inTrees.length; iTree++)
      inTrees[iTree] = FileSystem.getLocal(new Configuration()).open(new Path("test.quad"));
    out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("test-merged.quad")));

    t1 = System.currentTimeMillis();
    AggregateQuadTree.merge(inTrees,  out);
    t2 = System.currentTimeMillis();
    System.out.println("Merged "+ inTrees.length +" trees in "+(t2-t1)+" millis");
    out.close();
    for (int iTree = 0; iTree < inTrees.length; iTree++) 
      inTrees[iTree].close();

    FSDataInputStream in = FileSystem.getLocal(new Configuration()).open(new Path("test-merged.quad"));
    t1 = System.currentTimeMillis();
    int resultSize = AggregateQuadTree.selectionQuery(in, new Rectangle(0, 0, 5, 3), new ResultCollector2<Point, Short>() {
      @Override
      public void collect(Point p, Short v) {
        System.out.println("Point "+p+", value "+v);
      }
    });
    t2 = System.currentTimeMillis();
    System.out.println("Found "+resultSize+" results in "+(t2-t1)+" millis");
  }
}

