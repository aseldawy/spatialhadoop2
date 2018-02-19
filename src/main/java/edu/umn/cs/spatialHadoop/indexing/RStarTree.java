package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

/**
 * An R*-tree implementation based on the design in the following paper.
 *
 * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
 * The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles.
 * SIGMOD Conference 1990: 322-331
 */
public class RStarTree extends RTreeGuttman {

  /**Number of entries to delete and reinsert if a forced re-insert is needed*/
  protected int p;

  /**A flag set to true while a reinserting is in action to avoid cascade reinsertions*/
  protected boolean reinserting;

  public static RTreeGuttman constructFromPoints(double[] xs, double[] ys, int minCapacity, int maxCapcity) {
    RStarTree rtree = new RStarTree(minCapacity, maxCapcity);
    rtree.initializeDataEntries(xs, ys);
    rtree.insertAllDataEntries();
    return rtree;
  }

  public RStarTree(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
    p = maxCapcity * 4 / 10;
  }

  /**
   * Treats a node that ran out of space by either forced reinsert of some
   * entries or splitting.
   * @param iLeafNode the leaf node that overflew
   */
  protected int overflowTreatment(int iLeafNode, IntArray path) {
    if (iLeafNode != iRoot && !reinserting) {
      // If the level is not the root level and this is the first call of
      // overflowTreatment in the given level during the insertion of one entry
      // invoke reInsert
      reInsert(iLeafNode, path);
      // Return -1 which indicates no split happened.
      // Although we call insert recursively which might result in another split,
      // even in the same node, the recursive call will handle its split correctly
      // As long as the ID of the given node and the path to the root do not
      // change, this method should work fine.
      return -1;
    } else {
      return split(iLeafNode, minCapacity);
    }
  }

  /**
   * Delete and reinsert p elements from the given overflowing leaf node.
   * Described in Beckmann et al'90 Page 327
   * @param iNode
   */
  protected void reInsert(int iNode, IntArray path) {
    reinserting = true;
    // RI1 For all M+1 entries of a node N, compute the distance between
    // the centers of their rectangles and the center of the MBR of N
    double nodeX = (x1s[iNode] + x2s[iNode]) / 2;
    double nodeY = (y1s[iNode] + y2s[iNode]) / 2;
    
    final double[] distances2 = new double[Node_size(iNode)];
    final IntArray nodeChildren = children.get(iNode);
    for (int i = 0; i < nodeChildren.size(); i++) {
      int iChild = nodeChildren.get(i);
      double childX = (x1s[iChild] + x2s[iChild]) / 2;
      double childY = (y1s[iChild] + y2s[iChild]) / 2;
      double dx = childX - nodeX;
      double dy = childY - nodeY;
      distances2[i] = dx * dx + dy * dy;
    }

    // RI2 Sort the entries in decreasing order of their distances
    IndexedSortable sortDistance2 = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diff = distances2[i] - distances2[j];
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
        double temp = distances2[i];
        distances2[i] = distances2[j];
        distances2[j] = temp;
      }
    };
    QuickSort quickSort = new QuickSort();
    quickSort.sort(sortDistance2, 0, nodeChildren.size());

    // RI3 Remove the first p entries from N and adjust the MBR of N
    // Eldawy: We chose to sort them by (increasing) distance and remove
    // the last p elements since deletion from the tail of the list is faster
    IntArray entriesToReInsert = new IntArray();
    entriesToReInsert.append(nodeChildren, nodeChildren.size() - p, p);
    nodeChildren.resize(nodeChildren.size() - p);

    // Eldawy: Since we're going to reinsert elements at the root, we ought to
    // adjust the MBRs of all nodes along the path to the root, including N.
    for (int i = path.size() - 1; i >= 0; i--)
      Node_recalculateMBR(path.get(i));

    // RI4: In the sort, defined in RI2, starting with the minimum distance
    // (=close reinsert), invoke Insert to reinsert the entries
    for (int iEntryToReinsert : entriesToReInsert)
      insertAnExistingDataEntry(iEntryToReinsert);
    reinserting = false;
  }

  /**
   * Adjust the tree after an insertion by making the necessary splits up to
   * the root.
   * @param iLeafNode the index of the leaf node where the insertion happened
   * @param path
   */
  protected void adjustTree(int iLeafNode, IntArray path) {
    int iNode;
    int iNewNode = -1;
    if (Node_size(iLeafNode) > maxCapcity) {
      // Node full.
      // Overflow treatment
      iNewNode = overflowTreatment(iLeafNode, path);
    }
    // AdjustTree. Ascend from the leaf node L
    while (!path.isEmpty()) {
      iNode = path.pop();
      if (path.isEmpty()) {
        // The node is the root (no parent)
        if (iNewNode != -1) {
          // If the root is split, create a new root
          iRoot = Node_createNodeWithChildren(false, iNode, iNewNode);
        }
        // If N is the root with no partner NN, stop.
      } else {
        int iParent = path.peek();
        // Adjust covering rectangle in parent entry
        Node_expand(iParent, iNode);
        if (iNewNode != -1) {
          // If N has a partner NN resulting from an earlier split,
          // create a new entry ENN and add to the parent if there is room.
          // Add Enn to P if there is room
          Node_addChild(iParent, iNewNode);
          iNewNode = -1;
          if (Node_size(iParent) >= maxCapcity) {
            // TODO call overflowTreatment if necessary
            iNewNode = split(iParent, minCapacity);
          }
        }
      }
    }
  }

  interface MultiIndexedSortable extends IndexedSortable {
    enum Axis {X1, Y1, X2, Y2};
    void setAttribute(Axis i);
    Axis getAttribute();
  }

  /**
   * The R* split algorithm operating on a leaf node as described in the
   * following paper, Page 326.
   * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
   * The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles. SIGMOD Conference 1990: 322-331
   * @param iNode the index of the node to split
   * @param minSplitSize Minimum size for each split
   * @return the index of the new node created as a result of the split
   */
  @Override
  protected int split(int iNode, int minSplitSize) {
    final IntArray nodeChildren = children.get(iNode);
    // ChooseSplitAxis
    // Sort the entries by each axis and compute S, the sum of all margin-values
    // of the different distributions

    // Sort by x1, y1, x2, y2
    MultiIndexedSortable sorter = new MultiIndexedSortable() {
      public Axis attribute;

      @Override
      public void setAttribute(Axis att) { this.attribute = att; }

      @Override
      public Axis getAttribute() { return attribute; }

      @Override
      public int compare(int i, int j) {
        double diff;
        switch (attribute) {
          case X1: diff = x1s[nodeChildren.get(i)] - x1s[nodeChildren.get(j)]; break;
          case Y1: diff = y1s[nodeChildren.get(i)] - y1s[nodeChildren.get(j)]; break;
          case X2: diff = x2s[nodeChildren.get(i)] - x2s[nodeChildren.get(j)]; break;
          case Y2: diff = y2s[nodeChildren.get(i)] - y2s[nodeChildren.get(j)]; break;
          default: diff = 0;
        }
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
      }
    };
    double minSumMargin = Double.POSITIVE_INFINITY;
    MultiIndexedSortable.Axis bestAxis = null;
    QuickSort quickSort = new QuickSort();
    for (MultiIndexedSortable.Axis sortAttr : MultiIndexedSortable.Axis.values()) {
      sorter.setAttribute(sortAttr);
      quickSort.sort(sorter, 0, nodeChildren.size());
      double sumMargin = computeSumMargin(iNode);
      if (sumMargin < minSumMargin) {
        bestAxis = sortAttr;
        minSumMargin = sumMargin;
      }
    }

    // Choose the axis with the minimum S as split axis.
    if (bestAxis != sorter.getAttribute()) {
      sorter.setAttribute(bestAxis);
      quickSort.sort(sorter, 0, nodeChildren.size());
    }

    // Along the chosen axis, choose the distribution with the minimum overlap value.
    double minOverlap = Double.POSITIVE_INFINITY;
    double minArea = Double.POSITIVE_INFINITY;
    int chosenK = -1;
    Rectangle mbr1 = new Rectangle();
    Rectangle mbr2 = new Rectangle();
    // # of possible splits = current size - 2 * minSplitSize + 1
    int numPossibleSplits = Node_size(iNode) - 2 * minSplitSize + 1;
    for (int k = 1; k <= numPossibleSplits; k++) {
      int separator = minCapacity + k - 1; // Separator = size of first group
      mbr1.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = 0; i < separator; i++) {
        int iChild = nodeChildren.get(i);
        mbr1.expand(x1s[iChild], y1s[iChild]);
        mbr1.expand(x2s[iChild], y2s[iChild]);
      }
      mbr2.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < nodeChildren.size(); i++) {
        int iChild = nodeChildren.get(i);
        mbr2.expand(x1s[iChild], y1s[iChild]);
        mbr2.expand(x2s[iChild], y2s[iChild]);
      }
      Rectangle overlapMBR = mbr1.getIntersection(mbr2);
      double overlapArea = overlapMBR == null? 0 : overlapMBR.getWidth() * overlapMBR.getHeight();
      if (overlapArea < minOverlap) {
        minOverlap = overlapArea;
        minArea = mbr1.getWidth() * mbr1.getHeight() + mbr2.getWidth() *  mbr2.getHeight();
        chosenK = k;
      } else if (overlapArea == minOverlap) {
        // Resolve ties by choosing the distribution with minimum area-value
        double area = mbr1.getWidth() * mbr1.getHeight() + mbr2.getWidth() *  mbr2.getHeight();
        if (area < minArea) {
          minArea = area;
          chosenK = k;
        }
      }
    }

    // Split at the chosenK
    int separator = minCapacity - 1 + chosenK;
    int iNewNode = Node_split(iNode, separator);
    return iNewNode;
  }

  /**
   * Compute the sum margin of the given node assuming that the children have
   * been already sorted along one of the dimensions.
   * @param iNode the index of the node to compute for
   * @return
   */
  private double computeSumMargin(int iNode) {
    IntArray nodeChildren = children.get(iNode);
    double sumMargin = 0.0;
    Rectangle mbr1 = new Rectangle();
    Rectangle mbr2 = new Rectangle();
    for (int k = 1; k <= maxCapcity - 2 * minCapacity + 2; k++) {
      int separator = minCapacity - 1 + k;
      mbr1.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = 0; i < separator; i++) {
        int iChild = nodeChildren.get(i);
        mbr1.expand(x1s[iChild], y1s[iChild]);
      }
      mbr2.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < nodeChildren.size(); i++) {
        int iChild = nodeChildren.get(i);
        mbr2.expand(x1s[iChild], y1s[iChild]);
      }
      sumMargin += mbr1.getWidth() + mbr1.getHeight();
      sumMargin += mbr2.getWidth() + mbr2.getHeight();
    }
    return sumMargin;
  }
}
