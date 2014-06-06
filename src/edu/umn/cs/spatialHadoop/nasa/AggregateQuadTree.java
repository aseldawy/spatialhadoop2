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

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

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
  
  /** Maps each point from HDF file to its position in the array of quad tree */
  int[] r2z;
  
  /** Maps each point in quad tree list to its original location in HDF file. */
  int[] z2r;
  
  /** Maps each point in Z-order to its Z value. This list should be sorted. */
  int[] z;

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
    this.r2z = new int[resolution * resolution];
    this.z2r = new int[resolution * resolution];
    this.z = new int[resolution * resolution];
    // The list of all nodes
    Vector<Node> nodes = new Vector<Node>();
    
    // Compute the Z-order of all values
    for (int i = 0; i < z.length; i++) {
      short x = (short) (i % resolution);
      short y = (short) (i / resolution);
      int zorder = AggregateQuadTree.computeZOrder(x, y);
      //System.out.println("zorder of ("+x+","+y+") = "+zorder);
      z[i] = zorder;
      z2r[i] = i;
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
        temp = z2r[i];
        z2r[i] = z2r[j];
        z2r[j] = temp;
      }
      
      @Override
      public int compare(int i, int j) {
        return z[i] - z[j];
      }
    }, 0, z.length);
    
    // Prepare the inverse lookup table (HDF Index to Quad Tree Index)
    for (int i = 0; i < z2r.length; i++) {
      r2z[z2r[i]] = i;
    }
    
    // Construct the structure of the quad tree based on Z-values
    // Maximum number of values per node. Set it to a very small number to
    // construct as many levels as possible. Notice that when quad trees
    // are aggregated, a single value might become 366 values in the same pos.
    final int capacity = 5;
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
  
  /** Values in this tree sorted by their Z-order values */
  protected Object sortedValues;
  
  /** Aggregate value of min function for each node */
  protected short[] min;
  /** Aggregate value of max function for each node */
  protected short[] max;
  /** Aggregate value of sum function for each node */
  protected int[] sum;
  /** Aggregate value of count function for each node */
  protected int[] count;
  
  /**
   * Construct an actual aggregate quad tree out of a two-dimensional array
   * of values.
   * @param values
   */
  public AggregateQuadTree(Object values) {
    int length = Array.getLength(values);
    int resolution = (int) Math.round(Math.sqrt(length));
    StockQuadTree stockQuadTree = getOrCreateStockQuadTree(resolution);
    // Sort values by their respective Z-Order values
    sortedValues = Array.newInstance(Array.get(values, 0).getClass(), length);
    for (int i = 0; i < length; i++)
      Array.set(sortedValues, stockQuadTree.r2z[i], Array.get(values, i));
    
    min = new short[stockQuadTree.nodesID.length];
    max = new short[stockQuadTree.nodesID.length];
    sum = new int[stockQuadTree.nodesID.length];
    count = new int[stockQuadTree.nodesID.length];
    // Compute aggregate values for all nodes in the tree
    // Go in reverse ID order to ensure children are computed before parents
    for (int iNode = stockQuadTree.nodesID.length - 1; iNode >= 0 ; iNode--) {
      // Initialize all aggregate values
      min[iNode] = Short.MAX_VALUE;
      max[iNode] = Short.MIN_VALUE;
      count[iNode] = 0;
      sum[iNode] = 0;
      
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
          if (value < min[iNode])
            min[iNode] = value;
          if (value > max[iNode])
            max[iNode] = value;
          count[iNode]++;
          sum[iNode] += value;
        }
      } else {
        // Compute from the four children
        for (int iChild = 0; iChild < 4; iChild++) {
          int childPos = firstChildPos + iChild;
          if (min[childPos] < min[iNode])
            min[childPos] = min[iNode];
          if (max[childPos] > max[iNode])
            max[iNode] = max[childPos];
          count[iNode] += count[childPos];
          sum[iNode] += sum[childPos];
        }
      }
    }
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

  public static void main(String[] args) {
    // Test construction of aggregate quad trees
    short[] values = new short[1200 * 1200];
    long t1 = System.currentTimeMillis();
    new AggregateQuadTree(values);
    long t2 = System.currentTimeMillis();
    System.out.println("Elapsed time "+(t2-t1)+" millis");
  }
}

