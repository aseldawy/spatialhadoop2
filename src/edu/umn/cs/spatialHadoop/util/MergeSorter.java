/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.util.Vector;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progressable;


public class MergeSorter implements IndexedSorter {

  @Override
  public void sort(IndexedSortable s, int l, int r) {
    sort(s, l, r, null);
  }

  @Override
  public void sort(IndexedSortable s, final int l, final int r, Progressable rep) {
    for (int step = 2; step < (r - l) * 2; step *= 2) {
      for (int i = l; i < r; i += step) {
        // In this iteration, we need to merge the two sublists
        // [i, i + step / 2), [i + step / 2, i + step)
        
        int i1 = i;
        int j1 = i + step / 2;
        int j2 = Math.min(i + step, r);
        // In each iteration: Elements in the sublist [i, i1) are in their
        // correct position. i.e. each element in this sublist is less than
        // every element on the sublist [i1, j2)
        // The loop needs to merge the sublists [i1, j1), [j1, j2)
        
        while (j1 < j2) {
          // Step 1: Skip elements in the left list < minimum of right list
          while (i1 < j1 && s.compare(i1, j1) <= 0) {
            i1++;
          }
          
          if (i1 == j1) {
            // Range is already merged. Terminate the loop
            break;
          }
          
          // Step 2: Find all elements starting from j1 that needs to be moved
          // to i1
          // Note: We don't need to compare [j1] as we know it's less than [i1]
          int old_j1 = j1++;
          while (j1 < j2 && s.compare(i1, j1) > 0) {
            j1++;
          }
          
          // Step 3: Move the elements [old_j1, j1) to the position i1
          shift(s, old_j1, j1, i1);
          
          // Step 4: Update the indices.
          i1 += (j1 - old_j1);
          // I already compared [i1] and I know it's less than or equal to [j1]
          i1++;
          // Now the sublist [i, i1) is merged
        }
      }
    }
  }
  
  protected void shift(IndexedSortable s, int src_l, int src_r, int dst_l) {
    int max_step = src_r - src_l;
    while (src_l > dst_l) {
      // Move in steps of (src_r - src_l) but the step should not move beyond
      // the destination dst_l
      if (src_l - dst_l >= max_step) {
        for (int i = 0; i < max_step; i++) {
          s.swap(src_l + i, src_l - max_step + i);
        }
        src_l -= max_step;
        src_r -= max_step;
      } else {
        int step = src_l - dst_l;
        for (int i = 0; i < step; i++) {
          s.swap(src_l + i, dst_l + i);
        }
        src_l += step;
        dst_l += step;
        max_step = src_r - src_l;
      }
    }
  }
  
  static final Vector<Integer> x = new Vector<Integer>();
  public static void main(String[] args) {
    for (int i = 0; i < 1000000; i++) {
      x.add((int) (Math.random() * 100000));
    }
    
    IndexedSortable sortable = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Integer temp = x.elementAt(i);
        x.set(i, x.elementAt(j));
        x.set(j, temp);
      }
      
      @Override
      public int compare(int i, int j) {
        return x.elementAt(i) - x.elementAt(j);
      }
    };
    
    MergeSorter sorter = new MergeSorter();
    System.out.println(x);
    sorter.sort(sortable, 0, x.size());
    for (int i = 1; i < x.size(); i++) {
      if (x.elementAt(i - 1) > x.elementAt(i))
        System.out.println("tozzzzzzzzzzzzzzzzzzzzzz");
    }
    System.out.println(x);
  }

}
