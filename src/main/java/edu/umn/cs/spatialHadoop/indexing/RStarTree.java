package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  public static RStarTree constructFromPoints(double[] xs, double[] ys, int minCapacity, int maxCapcity) {
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
    int nodeSize = Node_size(iNode);
    final int[] nodeChildren = children.get(iNode).underlyingArray();
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
          case X1: diff = x1s[nodeChildren[i]] - x1s[nodeChildren[j]]; break;
          case Y1: diff = y1s[nodeChildren[i]] - y1s[nodeChildren[j]]; break;
          case X2: diff = x2s[nodeChildren[i]] - x2s[nodeChildren[j]]; break;
          case Y2: diff = y2s[nodeChildren[i]] - y2s[nodeChildren[j]]; break;
          default: diff = 0;
        }
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        int t = nodeChildren[i];
        nodeChildren[i] = nodeChildren[j];
        nodeChildren[j] = t;
      }
    };
    double minSumMargin = Double.POSITIVE_INFINITY;
    MultiIndexedSortable.Axis bestAxis = null;
    QuickSort quickSort = new QuickSort();
    for (MultiIndexedSortable.Axis sortAttr : MultiIndexedSortable.Axis.values()) {
      sorter.setAttribute(sortAttr);
      quickSort.sort(sorter, 0, nodeSize);

      double sumMargin = computeSumMargin(iNode, minSplitSize);
      if (sumMargin < minSumMargin) {
        bestAxis = sortAttr;
        minSumMargin = sumMargin;
      }
    }

    // Choose the axis with the minimum S as split axis.
    if (bestAxis != sorter.getAttribute()) {
      sorter.setAttribute(bestAxis);
      quickSort.sort(sorter, 0, nodeSize);
    }

    // Along the chosen axis, choose the distribution with the minimum overlap value.
    Rectangle mbr1 = new Rectangle(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    // Initialize the MBR of the first group to the minimum group size
    for (int i = 0; i < minSplitSize; i++){
      int iChild = nodeChildren[i];
      mbr1.expand(x1s[iChild], y1s[iChild]);
      mbr1.expand(x2s[iChild], y2s[iChild]);
    }

    // Pre-cache the MBRs for groups that start at position i and end at the end
    Rectangle mbr2 = new Rectangle(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    double[] minX1 = new double[nodeSize];
    double[] minY1 = new double[nodeSize];
    double[] maxX2 = new double[nodeSize];
    double[] maxY2 = new double[nodeSize];
    for (int i = nodeSize - 1; i >= minSplitSize; i--) {
      int iChild = nodeChildren[i];
      mbr2.expand(x1s[iChild], y1s[iChild]);
      mbr2.expand(x2s[iChild], y2s[iChild]);
      minX1[i] = mbr2.x1;
      minY1[i] = mbr2.y1;
      maxX2[i] = mbr2.x2;
      maxY2[i] = mbr2.y2;
    }

    double minOverlap = Double.POSITIVE_INFINITY;
    double minArea = Double.POSITIVE_INFINITY;
    int chosenK = -1;

    // # of possible splits = current size - 2 * minSplitSize + 1
    int numPossibleSplits = Node_size(iNode) - 2 * minSplitSize + 1;
    for (int k = 1; k <= numPossibleSplits; k++) {
      int separator = minSplitSize + k - 1; // Separator = size of first group
      mbr1.expand(x1s[nodeChildren[separator-1]], y1s[nodeChildren[separator-1]]);
      mbr1.expand(x2s[nodeChildren[separator-1]], y2s[nodeChildren[separator-1]]);

      mbr2.set(minX1[separator], minY1[separator],
          maxX2[separator], maxY2[separator]);

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
    int separator = minSplitSize - 1 + chosenK;
    int iNewNode = Node_split(iNode, separator);
    return iNewNode;
  }

  /**
   * Compute the sum margin of the given node assuming that the children have
   * been already sorted along one of the dimensions.
   * @param iNode the index of the node to compute for
   * @param minSplitSize the minimum split size to consider
   * @return
   */
  private double computeSumMargin(int iNode, int minSplitSize) {
    IntArray nodeChildren = children.get(iNode);
    double sumMargin = 0.0;
    Rectangle mbr1 = new Rectangle();
    // Initialize the MBR of the first group to the minimum group size
    mbr1.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    for (int i = 0; i < minSplitSize; i++){
      int iChild = nodeChildren.get(i);
      mbr1.expand(x1s[iChild], y1s[iChild]);
      mbr1.expand(x2s[iChild], y2s[iChild]);
    }

    // Pre-cache the MBRs for groups that start at position i and end at the end
    Rectangle mbr2 = new Rectangle(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    double[] minX1 = new double[nodeChildren.size()];
    double[] minY1 = new double[nodeChildren.size()];
    double[] maxX2 = new double[nodeChildren.size()];
    double[] maxY2 = new double[nodeChildren.size()];
    for (int i = nodeChildren.size() - 1; i >= minSplitSize; i--) {
      int iChild = nodeChildren.get(i);
      mbr2.expand(x1s[iChild], y1s[iChild]);
      mbr2.expand(x2s[iChild], y2s[iChild]);
      minX1[i] = mbr2.x1;
      minY1[i] = mbr2.y1;
      maxX2[i] = mbr2.x2;
      maxY2[i] = mbr2.y2;
    }

    int numPossibleSplits = Node_size(iNode) - 2 * minSplitSize + 1;
    for (int k = 1; k <= numPossibleSplits; k++) {
      int separator = minSplitSize + k - 1; // Separator = size of first group
      mbr1.expand(x1s[nodeChildren.get(separator-1)], y1s[nodeChildren.get(separator-1)]);
      mbr1.expand(x2s[nodeChildren.get(separator-1)], y2s[nodeChildren.get(separator-1)]);

      mbr2.set(minX1[separator], minY1[separator],
          maxX2[separator], maxY2[separator]);
      sumMargin += mbr1.getWidth() + mbr1.getHeight();
      sumMargin += mbr2.getWidth() + mbr2.getHeight();
    }
    return sumMargin;
  }

  /**
   * Use the R*-tree improved splitting algorithm to split the given set of points
   * such that each split does not exceed the given capacity.
   * Returns the MBRs of the splits. This method is a standalone static method
   * that does not use the RStarTree class but uses a similar algorithm used
   * in {@link #split(int, int)}
   * @param xs
   * @param ys
   * @param capacity
   * @return
   */
  public static Rectangle2D.Double[] partitionPoints(final double[] xs, final double[] ys, int capacity) {
    IndexedSortable sorterX = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double dx = xs[i] - xs[j];
        if (dx < 0) return -1;
        if (dx > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        double t = xs[i];
        xs[i] = xs[j];
        xs[j] = t;
        t = ys[i];
        ys[i] = ys[j];
        ys[j] = t;
      }
    };
    IndexedSortable sorterY = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double dy = ys[i] - ys[j];
        if (dy < 0) return -1;
        if (dy > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        double t = xs[i];
        xs[i] = xs[j];
        xs[j] = t;
        t = ys[i];
        ys[i] = ys[j];
        ys[j] = t;
      }
    };
    QuickSort quickSort = new QuickSort();
    int numRangesToSplit = 0;
    long[] rangesToSplit = new long[16];
    List<Rectangle2D.Double> finalizedSplits = new ArrayList<Rectangle2D.Double>();
    rangesToSplit[numRangesToSplit++] = (((long)xs.length) << 32);
    // Temporary arrays for pre-caching min and max coordinates
    double[] group2Min = new double[xs.length];
    double[] group2Max = new double[xs.length];
    while (numRangesToSplit > 0) {
      long rangeToSplit = rangesToSplit[--numRangesToSplit];
      int rangeStart = (int) (rangeToSplit & 0xffffffffL);
      int rangeEnd = (int) (rangeToSplit >>> 32);

      if (rangeEnd - rangeStart <= capacity) {
        // No further splitting needed. Create a partition
        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;
        for (int i = rangeStart; i < rangeEnd; i++) {
          minX = Math.min(minX, xs[i]);
          maxX = Math.max(maxX, xs[i]);
          minY = Math.min(minY, ys[i]);
          maxY = Math.max(maxY, ys[i]);
        }
        finalizedSplits.add(new Rectangle2D.Double(minX, minY,
            maxX - minX, maxY - minY));
        continue;
      }

      int minSplitSize = Math.max(capacity / 2, (rangeEnd - rangeStart) / 2 - capacity);
      // ChooseSplitAxis
      // Sort the entries by each axis and compute S, the sum of all margin-values
      // of the different distributions

      // Start with x-axis. Sort then compute sum margin.
      quickSort.sort(sorterX, rangeStart, rangeEnd);
      // To compute sum margin, we need to compute MBR of each group
      // Group 1 covers the range [rangeStart, separator)
      // Group 2 covers the range [separator, rangeEnd)
      // Min X and Max X of each group can be found directly based on the sort order
      // Min and Max Y of group 1 can be computed incrementally as we scan from the start
      // Min and Max Y of group 2 is pre-computed and cached incrementally as we scan from the end

      // To compute Min & Max Y of group 1, we start the first (smallest) group
      double group1MinX, group1MaxX;
      double group1MinY = Double.POSITIVE_INFINITY;
      double group1MaxY = Double.NEGATIVE_INFINITY;
      for (int i = rangeStart; i < rangeStart + minSplitSize; i++) {
        group1MinY = Math.min(group1MinY, ys[i]);
        group1MaxY = Math.max(group1MaxY, ys[i]);
      }

      // Pre-compute Min & Max Y of group 2 incrementally from the end
      group2Min[rangeEnd - 1] = group2Max[rangeEnd - 1] = ys[rangeEnd - 1];
      for (int i = rangeEnd - 2; i >= rangeStart + minSplitSize; i--) {
        group2Min[i] = Math.min(group2Min[i + 1], ys[i]);
        group2Max[i] = Math.max(group2Max[i + 1], ys[i]);
      }

      double sumMarginX = 0.0;
      int numPossibleSplits = (rangeEnd - rangeStart) - 2 * minSplitSize + 1;
      for (int k = 1; k <= numPossibleSplits; k++) {
        // Separator is the first entry in the second group
        int separator = rangeStart + minSplitSize + k - 1;
        // Expand the MBR of group 1
        group1MinX = xs[rangeStart];
        group1MaxX = xs[separator - 1];
        group1MinY = Math.min(group1MinY, ys[separator - 1]);
        group1MaxY = Math.max(group1MaxY, ys[separator - 1]);
        // Retrieve MBR of group 2
        double group2MinX = xs[separator];
        double group2MaxX = xs[rangeEnd - 1];
        double group2MinY = group2Min[separator];
        double group2MaxY = group2Max[separator];
        // Compute the sum of the margin
        sumMarginX += (group1MaxX - group1MinX) + (group1MaxY - group1MinY);
        sumMarginX += (group2MaxX - group2MinX) + (group2MaxY - group2MinY);
      }

      // Repeat for the Y-axis
      quickSort.sort(sorterY, rangeStart, rangeEnd);
      // MBR of smallest group 1
      group1MinX = Double.POSITIVE_INFINITY;
      group1MaxX = Double.NEGATIVE_INFINITY;
      for (int i = rangeStart; i < rangeStart + minSplitSize; i++) {
        group1MinX = Math.min(group1MinX, xs[i]);
        group1MaxX = Math.max(group1MaxX, xs[i]);
      }
      // Pre-cache all MBRs of group 2
      group2Min[rangeEnd - 1] = group2Max[rangeEnd - 1] = xs[rangeEnd - 1];
      for (int i = rangeEnd - 2; i >= rangeStart + minSplitSize; i--) {
        group2Min[i] = Math.min(group2Min[i + 1], xs[i]);
        group2Max[i] = Math.max(group2Max[i + 1], xs[i]);
      }

      double sumMarginY = 0.0;
      for (int k = 1; k <= numPossibleSplits; k++) {
        // Separator is the first entry in the second group
        int separator = rangeStart + minSplitSize + k - 1;
        // Compute MBR of group 1
        group1MinY = ys[rangeStart];
        group1MaxY = ys[separator - 1];
        group1MinX = Math.min(group1MinX, xs[separator - 1]);
        group1MaxX = Math.max(group1MaxX, xs[separator - 1]);
        // Retrieve MBR of group 2
        double group2MinY = ys[separator];
        double group2MaxY = ys[rangeEnd - 1];
        double group2MinX = group2Min[separator];
        double group2MaxX = group2Max[separator];

        sumMarginY += (group1MaxX - group1MinX) + (group1MaxY - group1MinY);
        sumMarginY += (group2MaxX - group2MinX) + (group2MaxY - group2MinY);
      }

      // Along the chosen axis, choose the distribution with the minimum area
      // Note: Since we partition points, the overlap is always zero and we
      // ought to choose based on the total area only
      double minArea = Double.POSITIVE_INFINITY;
      int chosenK = -1;

      if (sumMarginX < sumMarginY) {
        // Split along the X-axis
        // Repeat sorting along the X-axis and pre-compute min & max Y
        quickSort.sort(sorterX, rangeStart, rangeEnd);
        group2Min[rangeEnd - 1] = group2Max[rangeEnd - 1] = ys[rangeEnd - 1];
        for (int i = rangeEnd - 2; i >= rangeStart + minSplitSize; i--) {
          group2Min[i] = Math.min(group2Min[i + 1], ys[i]);
          group2Max[i] = Math.max(group2Max[i + 1], ys[i]);
        }
        // MBR of smallest group 1
        group1MinY = Double.POSITIVE_INFINITY;
        group1MaxY = Double.NEGATIVE_INFINITY;
        for (int i = rangeStart; i < rangeStart + minSplitSize; i++) {
          group1MinY = Math.min(group1MinY, ys[i]);
          group1MaxY = Math.max(group1MaxY, ys[i]);
        }

        for (int k = 1; k <= numPossibleSplits; k++) {
          // Separator is the first entry in the second group
          int separator = rangeStart + minSplitSize + k - 1;

          // Expand MBR of group 1
          group1MinX = xs[rangeStart];
          group1MaxX = xs[separator - 1];
          group1MinY = Math.min(group1MinY, ys[separator - 1]);
          group1MaxY = Math.max(group1MaxY, ys[separator - 1]);
          // Retrieve MBR of group 2
          double group2MinX = xs[separator];
          double group2MaxX = xs[rangeEnd - 1];
          double group2MinY = group2Min[separator];
          double group2MaxY = group2Max[separator];

          // Overlap is always zero so we choose the distribution with min area
          double area = (group1MaxX - group1MinX) * (group1MaxY - group1MinY) +
              (group2MaxX - group2MinX) * (group2MaxY - group2MinY);
          if (area < minArea) {
            minArea = area;
            chosenK = k;
          }
        }
      } else {
        // Split along the Y-axis
        // Points are already sorted and MinX and MaxX for group 2 are precached
        // Compute MBR of smallest group 1
        group1MinX = Double.POSITIVE_INFINITY;
        group1MaxX = Double.NEGATIVE_INFINITY;
        for (int i = rangeStart; i < rangeStart + minSplitSize; i++) {
          group1MinX = Math.min(group1MinX, xs[i]);
          group1MaxX = Math.max(group1MaxX, xs[i]);
        }
        for (int k = 1; k <= numPossibleSplits; k++) {
          // Separator is the first entry in the second group
          int separator = rangeStart + minSplitSize + k - 1;

          // Expand MBR of group 1
          group1MinY = ys[rangeStart];
          group1MaxY = ys[separator - 1];
          group1MinX = Math.min(group1MinX, xs[separator - 1]);
          group1MaxX = Math.max(group1MaxX, xs[separator - 1]);
          // Retrieve MBR of group 2
          double group2MinY = ys[separator];
          double group2MaxY = ys[rangeEnd - 1];
          double group2MinX = group2Min[separator];
          double group2MaxX = group2Max[separator];

          // Choose the distribution with minimum area
          double area = (group1MaxX - group1MinX) * (group1MaxY - group1MinY) +
              (group2MaxX - group2MinX) * (group2MaxY - group2MinY);
          if (area < minArea) {
            minArea = area;
            chosenK = k;
          }
        }
      }
      // Split at the chosenK
      int separator = rangeStart + minSplitSize - 1 + chosenK;

      // Create two sub-ranges
      // Sub-range 1 covers the range [rangeStart, separator)
      // Sub-range 2 covers the range [separator, rangeEnd)
      long range1 = (((long) rangeStart) | (((long)separator) << 32));
      long range2 = (((long) separator) | (((long)rangeEnd) << 32));
      // Add to the stack and expand if necessary
      if (rangesToSplit.length < numRangesToSplit + 2) {
        // Need to expand the stack
        long[] newRangesToSplit = new long[rangesToSplit.length * 2];
        System.arraycopy(rangesToSplit, 0, newRangesToSplit, 0, numRangesToSplit);
        rangesToSplit = newRangesToSplit;
      }

      rangesToSplit[numRangesToSplit++] = range1;
      rangesToSplit[numRangesToSplit++] = range2;
    }

    return finalizedSplits.toArray(new Rectangle2D.Double[finalizedSplits.size()]);
  }

  enum Method {Incremental, BulkLoading1, BulkLoading2};
  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    FileReader testPointsIn = new FileReader(fileName);
    char[] buffer = new char[(int) new File(fileName).length()];
    testPointsIn.read(buffer);
    testPointsIn.close();

    long t1 = System.currentTimeMillis();
    String[] lines = new String(buffer).split("\\s");
    double[] xs = new double[lines.length];
    double[] ys = new double[lines.length];
    for (int iLine = 0; iLine < lines.length; iLine++) {
      String[] parts = lines[iLine].split(",");
      xs[iLine] = Double.parseDouble(parts[0]);
      ys[iLine] = Double.parseDouble(parts[1]);
    }
    int capacity = 8;
    capacity = xs.length / 2;
    capacity = 2000;

    Method method = Method.BulkLoading2;

    Rectangle2D.Double[] leaves;

    switch (method) {
      case BulkLoading1: {
        IntArray nodesToSplit = new IntArray();
        // Construct a tree with one root that contains all the points
        RStarTree rtree = RStarTree.constructFromPoints(xs, ys, xs.length, xs.length * 2);
        nodesToSplit.add(rtree.iRoot);
        int numSplits = 0;
        while (!nodesToSplit.isEmpty()) {
          int iNodeToSplit = nodesToSplit.pop();
          if (rtree.Node_size(iNodeToSplit) > capacity) {
            numSplits++;
            int minSplitSize = Math.max(capacity / 2, rtree.Node_size(iNodeToSplit) / 2 - capacity);
            int iNewNode = rtree.split(iNodeToSplit, minSplitSize);
            if (rtree.Node_size(iNodeToSplit) > capacity)
              nodesToSplit.add(iNodeToSplit);
            if (rtree.Node_size(iNewNode) > capacity)
              nodesToSplit.add(iNewNode);
          }
        }
        System.out.printf("Performed %d splits\n", numSplits);
        leaves = rtree.getAllLeaves();
        break;
      }
      case BulkLoading2: {
        leaves = RStarTree.partitionPoints(xs, ys, capacity);
        break;
      }
      case Incremental: {
        // Build the R-tree incrementally
        RStarTree rtree = RStarTree.constructFromPoints(xs, ys, capacity/2, capacity);
        leaves = rtree.getAllLeaves();
        break;
      }
      default:
        throw new RuntimeException("Unknown method "+method);
    }

    for (Rectangle2D.Double leaf : leaves) {
      System.out.println(new Rectangle(leaf.getMinX(), leaf.getMinY(), leaf.getMaxX(), leaf.getMaxY()).toWKT());
    }

    long t2 = System.currentTimeMillis();
    System.out.printf("Generated the tree using %s in %f seconds\n", method.toString(), (t2 -t1) / 1000.0);
  }
}
