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
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

/**
 * Stores a quad tree of values collected from HDF files from MODIS archive.
 * 
 * @author Ahmed Eldawy
 */
public class AggregateQuadTree {
  
  static class StockQuadTreeNode {
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
  
  /**
   * A structure that stores all lookup tables needed to construct and work
   * with a quad tree.
   * @author Ahmed Eldawy
   *
   */
  static class StockQuadTree {
    /** Maps each point from HDF file to its position in the array of quad tree */
    int[] HDFIndexToQuadTreeIndex;
    
    /** Maps each point in quad tree list to its original location in HDF file. */
    int[] QuadTreeIndexToHDFIndex;
    
    /**
     * Maps each node ID to node structure. ID of the root is always ONE.
     * For any non-leaf node with ID NodeID, the IDs of its four children are
     * NodeID * 4 + i where 1 <= i <= 4.
     * This list is sorted by node ID for fast access.
     */
    Vector<StockQuadTreeNode> nodes;
    
    /**
     * Constructs a stock quad tree for the given resolution
     * @param resolution
     */
    StockQuadTree(int resolution) {
      HDFIndexToQuadTreeIndex = new int[resolution * resolution];
      QuadTreeIndexToHDFIndex = new int[resolution * resolution];
      // Provides the Z-order of each value in the quad tree
      final int[] QuadTreeIndexToZOrder = new int[resolution * resolution];
      nodes = new Vector<StockQuadTreeNode>();
      
      // Compute the Z-order of all values
      for (int i = 0; i < QuadTreeIndexToZOrder.length; i++) {
        short x = (short) (i % resolution);
        short y = (short) (i / resolution);
        int zorder = AggregateQuadTree.computeZOrder(x, y);
        //System.out.println("zorder of ("+x+","+y+") = "+zorder);
        QuadTreeIndexToZOrder[i] = zorder;
        QuadTreeIndexToHDFIndex[i] = i;
      }
      
      // Sort ArrayToZOrder1200 by Z-Order and keep the original position of
      // each element by mirroring all swaps to ZOrderToArray1200
      new QuickSort().sort(new IndexedSortable() {
        @Override
        public void swap(int i, int j) {
          int temp;
          // Swap z-values (which are to be sorted)
          temp = QuadTreeIndexToZOrder[i];
          QuadTreeIndexToZOrder[i] = QuadTreeIndexToZOrder[j];
          QuadTreeIndexToZOrder[j] = temp;
          
          // Swap their relative positions in the other array
          temp = QuadTreeIndexToHDFIndex[i];
          QuadTreeIndexToHDFIndex[i] = QuadTreeIndexToHDFIndex[j];
          QuadTreeIndexToHDFIndex[j] = temp;
        }
        
        @Override
        public int compare(int i, int j) {
          return QuadTreeIndexToZOrder[i] - QuadTreeIndexToZOrder[j];
        }
      }, 0, QuadTreeIndexToZOrder.length);
      
      // Prepare the inverse lookup table (HDF Index to Quad Tree Index)
      for (int i = 0; i < QuadTreeIndexToHDFIndex.length; i++) {
        HDFIndexToQuadTreeIndex[QuadTreeIndexToHDFIndex[i]] = i;
      }
      
      // Construct the structure of the quad tree based on Z-values
      // Maximum number of values per node. Set it to a very small number to
      // construct as many levels as possible. Notice that when quad trees
      // are aggregated, a single value might become 366 values in the same pos.
      final int capacity = 5;
      StockQuadTreeNode root = new StockQuadTreeNode();
      root.startPosition = 0;
      root.endPosition = QuadTreeIndexToZOrder.length;
      root.id = 1;
      Queue<StockQuadTreeNode> nodesToCheckForSplit = new ArrayDeque<StockQuadTreeNode>();
      nodesToCheckForSplit.add(root);
      int numOfSignificantBitsInTree = getNumOfSignificantBits(resolution * resolution - 1);
      if ((numOfSignificantBitsInTree & 1) == 1)
        numOfSignificantBitsInTree++; // Round to next even value
      int maxId = 0;
      while (!nodesToCheckForSplit.isEmpty()) {
        StockQuadTreeNode nodeToCheckForSplit = nodesToCheckForSplit.poll();
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
          int zOrderCommonBits = QuadTreeIndexToZOrder[nodeToCheckForSplit.startPosition] & (0xffffffff << numOfSignificantBitsInNode);
          int childStartPosition = nodeToCheckForSplit.startPosition;
          for (int iChild = 0; iChild < 4; iChild++) {
            int zOrderUpperBound = zOrderCommonBits + ((iChild + 1) << (numOfSignificantBitsInNode - 2));
            int childEndPosition = childStartPosition;
            while (childEndPosition < nodeToCheckForSplit.endPosition &&
                QuadTreeIndexToZOrder[childEndPosition] < zOrderUpperBound)
              childEndPosition++;
            StockQuadTreeNode child = new StockQuadTreeNode();
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
      System.out.println("number of nodes in "+resolution+" is "+nodes.size());
      System.out.println("Max ID in "+resolution+" is "+maxId);
    }

  }

  /**
   * Returns number of significant bits as (32 - number of leading zeros)
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
  
  final static Map<Integer, StockQuadTree> QuadTrees;
  
  static {
    QuadTrees = new HashMap<Integer, StockQuadTree>();
    long t1 = System.currentTimeMillis();
    QuadTrees.put(1200, new StockQuadTree(1200));
    System.out.println("time for 1200 "+(System.currentTimeMillis() - t1));
    t1 = System.currentTimeMillis();
    QuadTrees.put(2400, new StockQuadTree(2400));
    System.out.println("time for 2400 "+(System.currentTimeMillis() - t1));
    t1 = System.currentTimeMillis();
    QuadTrees.put(4800, new StockQuadTree(4800));
    System.out.println("time for 4800 "+(System.currentTimeMillis() - t1));
  }
  
  public AggregateQuadTree(Object values) {
    int length = Array.getLength(values);
    int resolution = (int) Math.round(Math.sqrt(length));
    StockQuadTree quadTree = QuadTrees.get(resolution);
    // Sort values by their respective Z-Order values
    Object sortedValues = Array.newInstance(Array.get(values, 0).getClass(),
        length);
    for (int i = 0; i < length; i++) {
      Array.set(sortedValues, quadTree.HDFIndexToQuadTreeIndex[i], Array.get(values, i));
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
    int[] values = new int[1200 * 1200];
    new AggregateQuadTree(values);
  }
}
