package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.io.*;
import java.util.*;

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

  public RStarTree(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
    p = maxCapcity * 3 / 10;
  }

  /**
   * Tests if the MBR of a node fully contains an object
   * @param node
   * @param object
   * @return
   */
  protected boolean Node_contains(int node, int object) {
    return x1s[object] >= x1s[node] && x2s[object] <= x2s[node] &&
        y1s[object] >= y1s[node] && y2s[object] <= y2s[node];
  }

  /**
   * Compute the enlargement of the overlap of two nodes if an object is added
   * to the first node.
   * In other words, it computes Area((T U O) ^ J) - Area(T ^ J),
   * where T and J are the first and second noes, respectively, and O is the object
   * to be added to T.
   * @param nodeT
   * @param object
   * @param nodeJ
   * @return
   */
  protected double Node_overlapAreaEnlargement(int nodeT, int object, int nodeJ) {
    // Compute the MBB of (t U o)
    double x1to = Math.min(x1s[nodeT], x1s[object]);
    double y1to = Math.min(y1s[nodeT], y1s[object]);
    double x2to = Math.max(x2s[nodeT], x2s[object]);
    double y2to = Math.max(y2s[nodeT], y2s[object]);

    // Compute the width and height of (t U o) ^ j
    double widthTOJ = Math.min(x2to, x2s[nodeJ]) -
        Math.max(x1to, x1s[nodeJ]);
    widthTOJ = Math.max(0.0, widthTOJ);
    double heightTOJ = Math.min(y2to, y2s[nodeJ]) -
        Math.max(y1to, y1s[nodeJ]);
    heightTOJ = Math.max(0.0, heightTOJ);

    // Compute the width and height of the intersection of nodes t and j
    double widthTJ = Math.min(x2s[nodeT], x2s[nodeJ]) -
        Math.max(x1s[nodeT], x1s[nodeJ]);
    widthTJ = Math.max(0.0, widthTJ);
    double heightTJ = Math.min(y2s[nodeT], y2s[nodeJ]) -
        Math.max(y1s[nodeT], y1s[nodeJ]);
    heightTJ = Math.max(0.0, heightTJ);

    return widthTOJ * heightTOJ - widthTJ * heightTJ;
  }

  /**
   * The ChooseSubtree algorithm of the R*-tree as described on page 325 in
   * the paper
   * @param entry
   * @param node
   * @return
   */
  @Override protected int chooseSubtree(final int entry, int node) {
    assert !isLeaf.get(node);
    // If the child pointers in N do not point to leaves,
    // determine the minimum area cost (as in regular R-tree)
    if (!isLeaf.get(children.get(node).peek()))
      return super.chooseSubtree(entry, node);

    // If the child pointers in N point ot leaves, determine the minimum
    // overlap cost
    int bestChild = -1;
    double minVolume = Double.POSITIVE_INFINITY;
    final IntArray nodeChildren = children.get(node);
    // If there are any nodes that completely covers the entry, choose the one
    // with the least area
    for (int child : nodeChildren) {
      if (Node_contains(child, entry)) {
        double volume = Node_area(child);
        if (volume < minVolume) {
          bestChild = child;
          minVolume = volume;
        }
      }
    }
    // Found a zero-enlargement child with least volume
    if (bestChild != -1)
      return bestChild;

    // From this point on, we know that ALL children have to be expanded

    // Sort the children by their increasing order of area enlargements so that
    // we can reduce the processing by considering the first P=32 children
    final double[] volumeEnlargements = new double[nodeChildren.size()];
    for (int iChild = 0; iChild < nodeChildren.size(); iChild++)
      volumeEnlargements[iChild] = Node_volumeExpansion(nodeChildren.get(iChild), entry);
    IndexedSortable volEnlargementsSortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diff = volumeEnlargements[i] - volumeEnlargements[j];
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
        double temp = volumeEnlargements[i];
        volumeEnlargements[i] = volumeEnlargements[j];
        volumeEnlargements[j] = temp;
      }
    };
    new QuickSort().sort(volEnlargementsSortable, 0, nodeChildren.size());
    // Choose the entry N whose rectangle needs least overlap enlargement
    // to include the new data rectangle.
    double minOverlapEnlargement = Double.POSITIVE_INFINITY;
    double minVolumeEnlargement = Double.POSITIVE_INFINITY;

    // For efficiency, only consider the first 32 children in the sorted order
    // of volume expansion
    for (int iChild = 0; iChild < Math.min(32, nodeChildren.size()); iChild++) {
      int child = nodeChildren.get(iChild);
      double ovlpEnlargement = 0.0;
      double volumeEnlargement = volumeEnlargements[iChild];
      // If the MBB of the node expands, there could be some enlargement
      for (int child2 : nodeChildren) {
        if (child != child2) {
          // Add the overlap and volume enlargements of this pair
          ovlpEnlargement += Node_overlapAreaEnlargement(child, entry, child2);
        }
      }
      if (ovlpEnlargement < minOverlapEnlargement) {
        // Choose the entry whose rectangle needs least overlap enlargement to
        // include the new data rectangle
        bestChild = child;
        minOverlapEnlargement = ovlpEnlargement;
        minVolumeEnlargement = volumeEnlargement;
      } else if (ovlpEnlargement == minOverlapEnlargement) {
        // Resolve ties by choosing the entry whose rectangle needs least area enlargement
        if (volumeEnlargement < minVolumeEnlargement) {
          minVolumeEnlargement = volumeEnlargement;
          bestChild = child;
        } else if (volumeEnlargement == minVolumeEnlargement) {
          // then the entry with rectangle of smallest area
          if (Node_area(child) < Node_area(bestChild))
            bestChild = child;
        }
      }
    }
    assert bestChild != -1;
    return bestChild;
  }

  /**
   * Treats a node that ran out of space by either forced reinsert of some
   * entries or splitting.
   * @param iLeafNode the leaf node that overflew
   */
  protected int overflowTreatment(int iLeafNode, IntArray path) {
    if (iLeafNode != root && !reinserting) {
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
   * @param node
   */
  protected void reInsert(int node, IntArray path) {
    reinserting = true;
    final IntArray nodeChildren = children.get(node);
    // Remove the last element (the one that caused the expansion)
    int overflowEelement = nodeChildren.pop();
    // RI1 For all M+1 entries of a node N, compute the distance between
    // the centers of their rectangles and the center of the MBR of N
    final double nodeX = (x1s[node] + x2s[node]) / 2;
    final double nodeY = (y1s[node] + y2s[node]) / 2;
    final double[] distances = new double[nodeChildren.size()];
    for (int iChild = 0; iChild < nodeChildren.size(); iChild++) {
      int child = nodeChildren.get(iChild);
      double childX = (x1s[child] + x2s[child]) / 2;
      double childY = (y1s[child] + y2s[child]) / 2;
      double dx = childX - nodeX;
      double dy = childY - nodeY;
      distances[iChild] = dx * dx + dy * dy;
    }
    // RI2 Sort the entries in decreasing order of their distances
    // Eldawy: We choose to sort them by increasing order and removing the
    // the last p entries because removing the last elements from an array
    // is simpler
    IndexedSortable distanceSortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diff = distances[i] - distances[j];
        if (diff > 0) return -1;
        if (diff < 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
        double temp = distances[i];
        distances[i] = distances[j];
        distances[j] = temp;
      }
    };
    new QuickSort().sort(distanceSortable, 0, nodeChildren.size());

    // RI3 Remove the first p entries from N and adjust the MBR of N
    // Eldawy: We chose to sort them by (increasing) distance and remove
    // the last p elements since deletion from the tail of the list is faster
    IntArray entriesToReInsert = new IntArray();
    entriesToReInsert.append(nodeChildren, nodeChildren.size() - p, p);
    nodeChildren.resize(nodeChildren.size() - p);

    // RI4: In the sort, defined in RI2, starting with the minimum distance
    // (=close reinsert), invoke Insert to reinsert the entries
    for (int iEntryToReinsert : entriesToReInsert)
      insertAnExistingDataEntry(iEntryToReinsert);
    // Now, we can reinsert the overflow element again
    insertAnExistingDataEntry(overflowEelement);
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
      // Adjust covering rectangle in parent entry
      Node_expand(iNode, children.get(iNode).peek());
      if (path.isEmpty()) {
        // The node is the root (no parent)
        if (iNewNode != -1) {
          // If the root is split, create a new root
          root = Node_createNodeWithChildren(false, iNode, iNewNode);
        }
        // If N is the root with no partner NN, stop.
      } else {
        int iParent = path.peek();
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
      quickSort.sort(sorter, 0, nodeSize-1);

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
    int nodeSize = nodeChildren.size() - 1; // -1 excludes the overflow object
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
    double[] minX1 = new double[nodeSize];
    double[] minY1 = new double[nodeSize];
    double[] maxX2 = new double[nodeSize];
    double[] maxY2 = new double[nodeSize];
    for (int i = nodeSize - 1; i >= minSplitSize; i--) {
      int iChild = nodeChildren.get(i);
      mbr2.expand(x1s[iChild], y1s[iChild]);
      mbr2.expand(x2s[iChild], y2s[iChild]);
      minX1[i] = mbr2.x1;
      minY1[i] = mbr2.y1;
      maxX2[i] = mbr2.x2;
      maxY2[i] = mbr2.y2;
    }

    int numPossibleSplits = nodeSize - 2 * minSplitSize + 1;
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
   * A class that stores an auxiliary data structure used to search through
   * the partitions created using the function
   * {@link #partitionPoints(double[], double[], int, int, boolean, AuxiliarySearchStructure)}
   */
  public static class AuxiliarySearchStructure implements Writable {
    /**The first split to consider*/
    public int rootSplit;
    /**The coordinate along where the split happens*/
    public double[] splitCoords;
    /**The axis along where the partition happened. 0 for X and 1 for Y.*/
    public BitArray splitAxis;
    /**
     * The next partition to consider if the search point is less than the
     * split line. If the number is negative, it indicates that the search is
     * terminated and a partition is matched. The partition index is (-x-1)
     **/
    public int[] partitionLessThan;
    /**
     * The next partition to consider if the search point is greater than or
     * equal to the split line.
     */
    public int[] partitionGreaterThanOrEqual;

    /**
     * Returns the ID of the partition that contain the given point
     * @param x
     * @param y
     * @return
     */
    public int search(double x, double y) {
      if (splitCoords.length == 0)
        return 0;
      int iSplit = rootSplit;
      while (iSplit >= 0) {
        // Choose which coordinate to work with depending on the split axis
        double coordToConsider = splitAxis.get(iSplit)? y : x;
        iSplit = (coordToConsider < splitCoords[iSplit] ? partitionLessThan : partitionGreaterThanOrEqual)[iSplit];
      }
      // Found the terminal partition, return its correct ID
      return -iSplit - 1;
    }

    /**
     * Find all the overlapping partitions for a given rectangular area
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @param ps
     */
    public void search(double x1, double y1, double x2, double y2, IntArray ps) {
      // We to keep a stack of splits to consider.
      // For efficiency, we reuse the given IntArray (ps) where we store the
      // the matching partitions from one end and the splits to be considered
      // from the other end
      // A negative number indicates a matching partition
      ps.clear();
      if (splitCoords.length == 0) {
        // No splits. Always return 0 which is the only partition we have
        ps.add(0);
        return;
      }
      IntArray splitsToSearch = ps;
      int iSplit = rootSplit;
      while (iSplit >= 0) {
        double coordMin = splitAxis.get(iSplit)? y1 : x1;
        double coordMax = splitAxis.get(iSplit)? y2 : x2;
        if (coordMax < splitCoords[iSplit]) {
          // Only the first half-space matches
          iSplit = partitionLessThan[iSplit];
        } else if (coordMin >= splitCoords[iSplit]) {
          // Only the second half-space matches
          iSplit = partitionGreaterThanOrEqual[iSplit];
        } else {
          // The entire space is still to be considered
          if (partitionGreaterThanOrEqual[iSplit] >= 0) {
            // A split that needs to be further considered
            splitsToSearch.add(partitionGreaterThanOrEqual[iSplit]);
          } else {
            // A terminal partition that should be matched
            splitsToSearch.insert(0, partitionGreaterThanOrEqual[iSplit]);
          }
          iSplit = partitionLessThan[iSplit];
        }
        // If iSplit reaches a terminal partitions, add it to the answer and
        // move on to the next split
        while (iSplit < 0) {
          ps.insert(0, iSplit);
          if (splitsToSearch.peek() >= 0)
            iSplit = splitsToSearch.pop();
          else
            break;
        }
      }
      // Convert the matching splits from their negative number to the correct
      // partitionID
      for (int i = 0; i < ps.size(); i++)
        ps.set(i, -ps.get(i) - 1);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(rootSplit);
      out.writeInt(splitCoords.length);
      for (int i = 0; i < splitCoords.length; i++) {
        out.writeDouble(splitCoords[i]);
        out.writeInt(partitionLessThan[i]);
        out.writeInt(partitionGreaterThanOrEqual[i]);
      }
      splitAxis.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      rootSplit = in.readInt();
      int numOfSplits = in.readInt();
      if (splitCoords == null) splitCoords = new double[numOfSplits];
      if (partitionLessThan == null) partitionLessThan = new int[numOfSplits];
      if (partitionGreaterThanOrEqual == null) partitionGreaterThanOrEqual = new int[numOfSplits];
      for (int i = 0; i < numOfSplits; i++) {
        splitCoords[i] = in.readDouble();
        partitionLessThan[i] = in.readInt();
        partitionGreaterThanOrEqual[i] = in.readInt();
      }
      if (splitAxis == null) splitAxis = new BitArray();
      splitAxis.readFields(in);
    }
  }

  /**
   * Use the R*-tree improved splitting algorithm to split the given set of points
   * such that each split does not exceed the given capacity.
   * Returns the MBRs of the splits. This method is a standalone static method
   * that does not use the RStarTree class but uses a similar algorithm used
   * in {@link #split(int, int)}
   * @param xs
   * @param ys
   * @param minPartitionSize minimum number of points to include in a partition
   * @param maxPartitionSize maximum number of point in a partition
   * @param expandToInf when set to true, the returned partitions are expanded
   *                    to cover the entire space from, Negative Infinity to
   *                    Positive Infinity, in all dimensions.
   * @param aux If not set to <code>null</code>, this will be filled with some
   *            auxiliary information to help efficiently search through the
   *            partitions.
   * @return
   */
  public static Rectangle[] partitionPoints(final double[] xs,
                                            final double[] ys,
                                            final int minPartitionSize,
                                            final int maxPartitionSize,
                                            final boolean expandToInf,
                                            final AuxiliarySearchStructure aux) {
    class SplitTask {
      /**The range of points to partition*/
      int start, end;
      /**The rectangular region that covers those points*/
      double x1, y1, x2, y2;
      /**Where the separation happens in the range [start, end)*/
      int separator;
      /**The coordinate along where the separation happened*/
      double splitCoord;
      /**The axis along where the separation happened. 0 for X and 1 for Y*/
      int axis;

      SplitTask(int s, int e, double x1, double y1, double x2, double y2) {
        this.start = s;
        this.end = e;
        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;
      }
    }
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
    Stack<SplitTask> rangesToSplit = new Stack<SplitTask>();
    Map<Long,SplitTask> rangesAlreadySplit = new HashMap<Long,SplitTask>();
    rangesToSplit.push(new SplitTask(0, xs.length, Double.NEGATIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
    List<Rectangle> finalizedSplits = new ArrayList<Rectangle>();
    // Temporary arrays for pre-caching min and max coordinates
    double[] group2Min = new double[xs.length];
    double[] group2Max = new double[xs.length];
    double fractionMinSplitSize = Math.min(minPartitionSize, maxPartitionSize-minPartitionSize) / (double)maxPartitionSize;
    fractionMinSplitSize = 0;
    while (!rangesToSplit.isEmpty()) {
      SplitTask range = rangesToSplit.pop();

      if (range.end - range.start <= maxPartitionSize) {
        Rectangle partitionMBR;
        if (expandToInf) {
          partitionMBR = new Rectangle(range.x1, range.y1, range.x2, range.y2);
        } else {
          // No further splitting needed. Create a partition
          double minX = Double.POSITIVE_INFINITY;
          double minY = Double.POSITIVE_INFINITY;
          double maxX = Double.NEGATIVE_INFINITY;
          double maxY = Double.NEGATIVE_INFINITY;
          for (int i = range.start; i < range.end; i++) {
            minX = Math.min(minX, xs[i]);
            maxX = Math.max(maxX, xs[i]);
            minY = Math.min(minY, ys[i]);
            maxY = Math.max(maxY, ys[i]);
          }
          partitionMBR = new Rectangle(minX, minY, maxX, maxY);
        }
        // Mark the range as a leaf partition by setting the separator to
        // a negative number x. The partition ID is -x-1
        range.separator = -finalizedSplits.size()-1;
        rangesAlreadySplit.put(((long)range.end << 32) | range.start, range);
        finalizedSplits.add(partitionMBR);
        continue;
      }

      int minSplitSize = Math.max(minPartitionSize, (int)((range.end - range.start) * fractionMinSplitSize));
      // ChooseSplitAxis
      // Sort the entries by each axis and compute S, the sum of all margin-values
      // of the different distributions

      // Start with x-axis. Sort then compute sum margin.
      quickSort.sort(sorterX, range.start, range.end);
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
      for (int i = range.start; i < range.start + minSplitSize; i++) {
        group1MinY = Math.min(group1MinY, ys[i]);
        group1MaxY = Math.max(group1MaxY, ys[i]);
      }

      // Pre-compute Min & Max Y of group 2 incrementally from the end
      group2Min[range.end - 1] = group2Max[range.end - 1] = ys[range.end - 1];
      for (int i = range.end - 2; i >= range.start + minSplitSize; i--) {
        group2Min[i] = Math.min(group2Min[i + 1], ys[i]);
        group2Max[i] = Math.max(group2Max[i + 1], ys[i]);
      }

      double sumMarginX = 0.0;
      int numPossibleSplits = (range.end - range.start) - 2 * minSplitSize + 1;
      for (int k = 1; k <= numPossibleSplits; k++) {
        // Separator is the first entry in the second group
        int separator = range.start + minSplitSize + k - 1;
        // Expand the MBR of group 1
        group1MinX = xs[range.start];
        group1MaxX = xs[separator - 1];
        group1MinY = Math.min(group1MinY, ys[separator - 1]);
        group1MaxY = Math.max(group1MaxY, ys[separator - 1]);
        // Retrieve MBR of group 2
        double group2MinX = xs[separator];
        double group2MaxX = xs[range.end - 1];
        double group2MinY = group2Min[separator];
        double group2MaxY = group2Max[separator];
        // Compute the sum of the margin
        sumMarginX += (group1MaxX - group1MinX) + (group1MaxY - group1MinY);
        sumMarginX += (group2MaxX - group2MinX) + (group2MaxY - group2MinY);
      }

      // Repeat for the Y-axis
      quickSort.sort(sorterY, range.start, range.end);
      // MBR of smallest group 1
      group1MinX = Double.POSITIVE_INFINITY;
      group1MaxX = Double.NEGATIVE_INFINITY;
      for (int i = range.start; i < range.start + minSplitSize; i++) {
        group1MinX = Math.min(group1MinX, xs[i]);
        group1MaxX = Math.max(group1MaxX, xs[i]);
      }
      // Pre-cache all MBRs of group 2
      group2Min[range.end - 1] = group2Max[range.end - 1] = xs[range.end - 1];
      for (int i = range.end - 2; i >= range.start + minSplitSize; i--) {
        group2Min[i] = Math.min(group2Min[i + 1], xs[i]);
        group2Max[i] = Math.max(group2Max[i + 1], xs[i]);
      }

      double sumMarginY = 0.0;
      for (int k = 1; k <= numPossibleSplits; k++) {
        // Separator is the first entry in the second group
        int separator = range.start + minSplitSize + k - 1;
        // Compute MBR of group 1
        group1MinY = ys[range.start];
        group1MaxY = ys[separator - 1];
        group1MinX = Math.min(group1MinX, xs[separator - 1]);
        group1MaxX = Math.max(group1MaxX, xs[separator - 1]);
        // Retrieve MBR of group 2
        double group2MinY = ys[separator];
        double group2MaxY = ys[range.end - 1];
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
        quickSort.sort(sorterX, range.start, range.end);
        group2Min[range.end - 1] = group2Max[range.end - 1] = ys[range.end - 1];
        for (int i = range.end - 2; i >= range.start + minSplitSize; i--) {
          group2Min[i] = Math.min(group2Min[i + 1], ys[i]);
          group2Max[i] = Math.max(group2Max[i + 1], ys[i]);
        }
        // MBR of smallest group 1
        group1MinY = Double.POSITIVE_INFINITY;
        group1MaxY = Double.NEGATIVE_INFINITY;
        for (int i = range.start; i < range.start + minSplitSize; i++) {
          group1MinY = Math.min(group1MinY, ys[i]);
          group1MaxY = Math.max(group1MaxY, ys[i]);
        }

        for (int k = 1; k <= numPossibleSplits; k++) {
          // Separator is the first entry in the second group
          int separator = range.start + minSplitSize + k - 1;

          // Expand MBR of group 1
          group1MinX = xs[range.start];
          group1MaxX = xs[separator - 1];
          group1MinY = Math.min(group1MinY, ys[separator - 1]);
          group1MaxY = Math.max(group1MaxY, ys[separator - 1]);
          
          // Skip if k is invalid (either side induce an invalid size)
          int size1 = separator - range.start;
          int minCount1 = (int) Math.ceil(size1/(double) maxPartitionSize);
          int maxCount1 = (int) Math.floor(size1 / (double) minPartitionSize);
          if (maxCount1 < minCount1)
            continue;
          
          int size2 = range.end - separator;
          int minCount2 = (int) Math.ceil(size2 / (double)maxPartitionSize);
          int maxCount2 = (int) Math.floor(size2 / (double) minPartitionSize);
          if (maxCount2 < minCount2)
            continue;
          
          // Retrieve MBR of group 2
          double group2MinX = xs[separator];
          double group2MaxX = xs[range.end - 1];
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
        for (int i = range.start; i < range.start + minSplitSize; i++) {
          group1MinX = Math.min(group1MinX, xs[i]);
          group1MaxX = Math.max(group1MaxX, xs[i]);
        }
        for (int k = 1; k <= numPossibleSplits; k++) {
          // Separator is the first entry in the second group
          int separator = range.start + minSplitSize + k - 1;

          // Expand MBR of group 1
          group1MinY = ys[range.start];
          group1MaxY = ys[separator - 1];
          group1MinX = Math.min(group1MinX, xs[separator - 1]);
          group1MaxX = Math.max(group1MaxX, xs[separator - 1]);
          // Retrieve MBR of group 2
          double group2MinY = ys[separator];
          double group2MaxY = ys[range.end - 1];
          double group2MinX = group2Min[separator];
          double group2MaxX = group2Max[separator];

          // Skip if k is invalid (either side induce an invalid size)
          int size1 = separator - range.start;
          int minCount1 = (int) Math.ceil(size1 / (double)maxPartitionSize);
          int maxCount1 = (int) Math.floor(size1 / (double)minPartitionSize);
          if (maxCount1 < minCount1)
            continue;
          
          int size2 = range.end - separator;
          int minCount2 = (int) Math.ceil(size2 / (double)maxPartitionSize);
          int maxCount2 = (int) Math.floor(size2 / (double)minPartitionSize);
          if (maxCount2 < minCount2)
            continue;

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
      range.separator = range.start + minSplitSize - 1 + chosenK;

      // Create two sub-ranges
      // Sub-range 1 covers the range [rangeStart, separator)
      // Sub-range 2 covers the range [separator, rangeEnd)
      SplitTask range1 = new SplitTask(range.start, range.separator,
          range.x1, range.y1, range.x2, range.y2);
      SplitTask range2 = new SplitTask(range.separator, range.end,
          range.x1, range.y1, range.x2, range.y2);
      // The spatial range of the input is split along the split axis
      if (sumMarginX < sumMarginY) {
        // Split was along the X-axis
        range.axis = 0;
        range.splitCoord = range1.x2 = range2.x1 = xs[range.separator];
      } else {
        // Split was along the Y-axis
        range.axis = 1;
        range.splitCoord = range1.y2 = range2.y1 = ys[range.separator];
      }
      rangesToSplit.add(range1);
      rangesToSplit.add(range2);
      rangesAlreadySplit.put(((long)range.end << 32) | range.start, range);
    }

    if (aux != null) {
      // Assign an ID for each partition
      Map<Long, Integer> partitionIDs = new HashMap<Long, Integer>();
      int seq = 0;
      for (Map.Entry<Long, SplitTask> entry : rangesAlreadySplit.entrySet()) {
        if (entry.getValue().separator >= 0)
          partitionIDs.put(entry.getKey(), seq++);
        else
          partitionIDs.put(entry.getKey(), entry.getValue().separator);
      }
      // Build the search data structure
      int numOfSplitAxis = rangesAlreadySplit.size() - finalizedSplits.size();
      assert seq == numOfSplitAxis;
      aux.partitionGreaterThanOrEqual = new int[numOfSplitAxis];
      aux.partitionLessThan = new int[numOfSplitAxis];
      aux.splitAxis = new BitArray(numOfSplitAxis);
      aux.splitCoords = new double[numOfSplitAxis];
      for (Map.Entry<Long, SplitTask> entry : rangesAlreadySplit.entrySet()) {
        if (entry.getValue().separator > 0) {
          int id = partitionIDs.get(entry.getKey());
          SplitTask range = entry.getValue();
          aux.splitCoords[id] = range.splitCoord;
          aux.splitAxis.set(id, range.axis == 1);
          long p1 = (((long)range.separator) << 32) | range.start;
          aux.partitionLessThan[id] = partitionIDs.get(p1);
          long p2 = (((long)range.end) << 32) | range.separator;
          aux.partitionGreaterThanOrEqual[id] = partitionIDs.get(p2);
          if (range.start == 0 && range.end == xs.length)
            aux.rootSplit = id;
        }
      }
    }

    return finalizedSplits.toArray(new Rectangle[finalizedSplits.size()]);
  }
}
