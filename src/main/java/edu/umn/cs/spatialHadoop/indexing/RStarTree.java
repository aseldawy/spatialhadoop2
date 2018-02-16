package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class RStarTree extends RTreeGuttman {
  public RStarTree(double[] xs, double[] ys, int minCapacity, int maxCapcity) {
    super(xs, ys, minCapacity, maxCapcity);
  }

  /**
   * The R* split algorithm operating on a leaf node as described in the
   * following paper, Page 326.
   * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
   * The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles. SIGMOD Conference 1990: 322-331
   * @param iNode the index of the node to split
   * @return the index of the new node created as a result of the split
   */
  @Override
  protected int split(int iNode) {
    final IntArray nodeChildren = children.get(iNode);
    // ChooseSplitAxis
    // Sort the entries by each axis and compute S, the sum of all margin-values
    // of the different distributions

    // Sort by x-axis (x1 then x2)
    IndexedSortable sortX = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double dx = x1s[nodeChildren.get(i)] - x1s[nodeChildren.get(j)];
        if (dx < 0)
          return -1;
        if (dx > 0)
          return 1;
        else {
          dx = x2s[nodeChildren.get(i)] - x2s[nodeChildren.get(j)];
          if (dx < 0) return -1;
          if (dx > 0) return 1;
          return 0;
        }

      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
      }
    };
    // Sort by y-axis (y1 then y2)
    IndexedSortable sortY = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double dy = y1s[nodeChildren.get(i)] - y1s[nodeChildren.get(j)];
        if (dy < 0)
          return -1;
        if (dy > 0)
          return 1;
        else {
          dy = y2s[nodeChildren.get(i)] - y2s[nodeChildren.get(j)];
          if (dy < 0) return -1;
          if (dy > 0) return 1;
          return 0;
        }

      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
      }
    };
    QuickSort quickSort = new QuickSort();
    quickSort.sort(sortX, 0, nodeChildren.size());
    double sumMarginX = computeSumMargin(iNode);
    quickSort.sort(sortY, 0, nodeChildren.size());
    double sumMarginY = computeSumMargin(iNode);
    if (sumMarginX < sumMarginY) {
      // Choose the axis with the minimum S as split axis.
      quickSort.sort(sortX, 0, nodeChildren.size());
    }

    // Along the chosen axis, choose the distribution with the minimum overlap value.
    double minOverlap = Double.POSITIVE_INFINITY;
    double minArea = Double.POSITIVE_INFINITY;
    int chosenK = -1;
    Rectangle mbr1 = new Rectangle();
    Rectangle mbr2 = new Rectangle();
    for (int k = 1; k <= maxCapcity - 2 * minCapacity + 2; k++) {
      int separator = minCapacity - 1 + k;
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
