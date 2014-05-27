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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.core.Point;

/**
 * Stores a quad tree of values collected from HDF files from MODIS archive.
 * 
 * @author Ahmed Eldawy
 */
public class AggregateQuadTree {
  
  /**
   * A structure that stores all lookup tables needed to construct and work
   * with a quad tree.
   * @author Ahmed Eldawy
   *
   */
  static class QuadTreeLookupTables {
    /**
     * Maps each point from the HDF file to its position in the array of
     * quad tree.
     */
    int[] HDFIndexToQuadTreeIndex;
    
    /**
     * Maps each point in the quad tree list to its original location in the
     * HDF file.
     */
    int[] QuadTreeIndexToHDFIndex;
    
    /**
     * Provides the Z-order of each value in the quad tree
     */
    int[] QuadTreeIndexToZOrder;
    
    QuadTreeLookupTables(int resolution) {
      // Compute the Z-order of all 1200 x 1200 values
      for (int i = 0; i < QuadTreeIndexToZOrder.length; i++) {
        short x = (short) (i % resolution);
        short y = (short) (i / resolution);
        int zorder = Point.computeZOrder(x, y);
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
      
      // Prepare the inverse lookup table
      for (int i = 0; i < QuadTreeIndexToHDFIndex.length; i++) {
        HDFIndexToQuadTreeIndex[QuadTreeIndexToHDFIndex[i]] = i;
      }
    }
  }
  
  final static Map<Integer, QuadTreeLookupTables> QuadTrees;
  
  static {
    QuadTrees = new HashMap<Integer, QuadTreeLookupTables>();
    QuadTrees.put(1200, new QuadTreeLookupTables(1200));
    QuadTrees.put(2400, new QuadTreeLookupTables(2400));
    QuadTrees.put(4800, new QuadTreeLookupTables(4800));
  }
  
  public AggregateQuadTree(Object values) {
    int length = Array.getLength(values);
    int resolution = (int) Math.round(Math.sqrt(length));
    QuadTreeLookupTables quadTree = QuadTrees.get(resolution);
    // Sort values by their respective Z-Order values
    Object sortedValues = Array.newInstance(Array.get(values, 0).getClass(),
        length);
    for (int i = 0; i < length; i++) {
      Array.set(sortedValues, quadTree.HDFIndexToQuadTreeIndex[i], Array.get(values, i));
    }
    
    
  }
  
}
