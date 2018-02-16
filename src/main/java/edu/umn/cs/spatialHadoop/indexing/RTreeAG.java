package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A partial implementation for the original Antonin Guttman R-tree as described
 * in the following paper.
 * Antonin Guttman: R-Trees: A Dynamic Index Structure for Spatial Searching.
 * SIGMOD Conference 1984: 47-57
 *
 * It also provides an implementation of the R*-tree as described in:
 * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
 * The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles.
 * SIGMOD Conference 1990: 322-331
 *
 * It only contain the implementation of the parts needed for the indexing
 * methods. For example, the delete operation was not implemented as it is
 * not needed. Also, this index is designed mainly to be used to index a sample
 * in memory and use it for the partitioning. So, the disk-based mapping and
 * search were not implemented for simplicity.
 */
public class RTreeAG {

  /** Maximum capacity of a node */
  private final int maxCapcity;

  /** Minimum capacity of a node. */
  private final int minCapacity;

  /**If this flag is true, the R* implementation is used*/
  private boolean rStar;

  /** The four coordinates (left, bottom, right, top) for objects (entries + nodes) */
  private double[] x1s, y1s, x2s, y2s;

  /**A bit vector that stores which nodes are leaves*/
  private BitArray isLeaf;

  /**A list of int[] that stores the children of each node*/
  private List<IntArray> children;

  /**Total number of data entries*/
  private int numEntries;

  /**Total number of nodes*/
  private int numNodes;

  /**The index of the root in the list of nodes*/
  protected int iRoot;

  /**
   * Make a room in the data structures to accommodate a new object whether
   * it is a node or a data entry.
   */
  protected void makeRoomForOneMoreObject() {
    if (x1s.length >= numEntries + numNodes) {
      // Expand the coordinate arrays in big chunks to avoid memory copy
      double[] newCoords = new double[x1s.length * 2];
      System.arraycopy(x1s, 0, newCoords, 0, x1s.length);
      x1s = newCoords;
      newCoords = new double[x2s.length * 2];
      System.arraycopy(x2s, 0, newCoords, 0, x2s.length);
      x2s = newCoords;
      newCoords = new double[y1s.length * 2];
      System.arraycopy(y1s, 0, newCoords, 0, y1s.length);
      y1s = newCoords;
      newCoords = new double[y2s.length * 2];
      System.arraycopy(y2s, 0, newCoords, 0, y2s.length);
      y2s = newCoords;

      this.isLeaf.resize(x1s.length);
    }
  }

  /**
   * Creates a new node that contains the given object and returns the ID
   * of that node.
   * @param leaf set to true to create a leaf node
   * @param iChildren the indexes of all children in this node
   * @return
   */
  protected int Node_createNodeWithChildren(boolean leaf, int ... iChildren) {
    makeRoomForOneMoreObject();
    int iNewNode = numEntries + numNodes;
    this.isLeaf.set(iNewNode, leaf);
    this.children.add(iNewNode, new IntArray());
    this.numNodes++;
    Node_reset(iNewNode, iChildren);
    return iNewNode;
  }

  /**
   * Reset a node to contain a new set of children wiping away the current
   * children.
   * @param iNode
   * @param newChildren
   */
  protected void Node_reset(int iNode, int ... newChildren) {
    children.get(iNode).clear();
    children.get(iNode).append(newChildren, 0, newChildren.length);
    Node_recalculateMBR(iNode);
  }

  protected void Node_recalculateMBR(int iNode) {
    x1s[iNode] = y1s[iNode] = Double.POSITIVE_INFINITY;
    x2s[iNode] = y2s[iNode] = Double.NEGATIVE_INFINITY;
    for (int iChild : children.get(iNode)) {
      if (x1s[iChild] < x1s[iNode])
        x1s[iNode] = x1s[iChild];
      if (y1s[iChild] < y1s[iNode])
        y1s[iNode] = y1s[iChild];
      if (x2s[iChild] > x2s[iNode])
        x2s[iNode] = x2s[iChild];
      if (y2s[iChild] > y2s[iNode])
        y2s[iNode] = y2s[iChild];
    }
  }

  /**
   * Returns the number of children for the given node.
   * @param iNode
   * @return
   */
  protected int Node_size(int iNode) {
    return children.get(iNode).size();
  }

  /**
   * Calculates the area of the node
   * @param iNode the ID of the node
   * @return
   */
  protected double Node_area(int iNode) {
    return (x2s[iNode] - x1s[iNode]) * (y2s[iNode] - y1s[iNode]);
  }

  /**
   * Calculates the expansion that will happen if the given object is added
   * to a given node.
   * @param iNode the ID of the node that would be expanded
   * @param iNewChild the ID of the object that would be added to the node
   * @return
   */
  protected double Node_expansion(int iNode, int iNewChild) {
    double widthB4Expansion = x2s[iNode] - x1s[iNode];
    double heightB4Expansion = y2s[iNewChild] - y1s[iNode];
    double widthAfterExpansion = Math.max(x2s[iNode], x2s[iNewChild]) -
        Math.min(x1s[iNode], x1s[iNewChild]);
    double heightAfterExpansion = Math.max(y2s[iNode], y2s[iNewChild]) -
        Math.min(y1s[iNode], y1s[iNewChild]);
    
    return widthAfterExpansion * heightAfterExpansion -
        widthB4Expansion * heightB4Expansion;
  }

  /**
   * Adds a new child to an existing node.
   * @param iNode
   * @param iNewChild
   */
  protected void Node_addChild(int iNode, int iNewChild) {
    this.children.get(iNode).add(iNewChild);
    Node_expand(iNode, iNewChild);
  }

  /**
   * Expand the MBR of the given node to enclose the given new object
   * @param iNode
   * @param iNewObject
   */
  protected void Node_expand(int iNode, int iNewObject) {
    // Expand the MBR to enclose the new child
    if (x1s[iNewObject] < x1s[iNode])
      x1s[iNode] = x1s[iNewObject];
    if (y1s[iNewObject] < y1s[iNode])
      y1s[iNode] = y1s[iNewObject];
    if (x2s[iNewObject] > x2s[iNode])
      x2s[iNode] = x2s[iNewObject];
    if (y2s[iNewObject] > y2s[iNode])
      y2s[iNode] = y2s[iNewObject];
  }

  /**
   * Split an existing node around the given separator. Current children from
   * indexes 1 to separator-1 (inclusive) remain in the given node. All remaining
   * children go to a new node. The ID of the new node created that contain the
   * children from separator to end.
   * @param iNode the index of the node to split
   * @param separator the index of the first child to be in the new node
   * @return the ID of the new node created after split
   */
  protected int Node_split(int iNode, int separator) {
    // Create the new node that will hold the entries from separator -> size
    makeRoomForOneMoreObject();
    int iNewNode = numNodes + numEntries;
    this.numNodes++;
    // Make room for the children of the new node
    this.children.add(iNewNode, new IntArray());
    // The new node in the same level so it follow the leaf/non-leaf status of the current node
    isLeaf.set(iNewNode, isLeaf.get(iNode));

    // Split the children around the separator
    children.get(iNewNode).append(children.get(iNode), separator,
        children.get(iNode).size() - separator);
    children.get(iNode).resize(separator);

    // Recalculate the MBRs of the two nodes
    Node_recalculateMBR(iNode);
    Node_recalculateMBR(iNewNode);

    return iNewNode;
  }

  /**
   * Construct a new RTree on the given set of points.
   * @param xs - x-coordinates for the points
   * @param ys - y-coordinates for the points
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity - Maximum capacity of a node
   * @param rStar - When set to true, R* split algorithm is applied
   */
  public RTreeAG(double[] xs, double[] ys, int minCapacity, int maxCapcity, boolean rStar) {
    this.numEntries = xs.length;
    this.isLeaf = new BitArray(numEntries);
    children = new ArrayList<IntArray>(numEntries);
    this.x1s = new double[numEntries];
    this.y1s = new double[numEntries];
    this.x2s = new double[numEntries];
    this.y2s = new double[numEntries];
    for (int i = 0; i < numEntries; i++) {
      this.x1s[i] = xs[i];
      this.y1s[i] = ys[i];
      this.x2s[i] = Math.nextUp(xs[i]);
      this.y2s[i] = Math.nextUp(ys[i]);
      children.add(null); // data entries do not have children
    }
    this.numNodes = 0; // Initially, no nodes are there
    this.maxCapcity = maxCapcity;
    this.minCapacity = minCapacity;
    this.rStar = rStar;

    iRoot = Node_createNodeWithChildren(true, 0);
    // Insert one by one
    for (int i = 1; i < xs.length; i++)
      insert(i);
  }

  /**
   * Inserts the given data entry into the tree. We assume that the coordinates
   * of this data entry are already stored in the coordinates arrays.
   * @param iEntry - The index of the point in the array of points
   */
  private void insert(int iEntry) {
    // The path from the root to the newly inserted record. Used for splitting.
    IntArray path = new IntArray();
    int iCurrentVisitedNode = iRoot;
    path.add(iCurrentVisitedNode);
    // Descend in the tree until we find a leaf node to add the object to
    while (!isLeaf.get(iCurrentVisitedNode)) {
      // Node is not leaf. Choose a child node
      // 1. Choose the child with the minimum expansion
      double minExpansion = Double.POSITIVE_INFINITY;
      int iBestChild = 0;
      for (int iCandidateChild : children.get(iCurrentVisitedNode)) {
        double expansion = Node_expansion(iCandidateChild, iEntry);
        if (expansion < minExpansion) {
          minExpansion = expansion;
          iBestChild = iCandidateChild;
        } else if (expansion == minExpansion) {
          // Resolve ties by choosing the entry with the rectangle of smallest area
          if (Node_area(iCandidateChild) < Node_area(iBestChild))
            iBestChild = iCandidateChild;
        }
      }
      // Descend to the best child found
      iCurrentVisitedNode = iBestChild;
      path.add(iCurrentVisitedNode);
    }

    // Now we have a child node. Insert the current element to it and split
    // if necessary
    Node_addChild(iCurrentVisitedNode, iEntry);
    adjustTree(iCurrentVisitedNode, path);
  }

  /**
   * Adjust the tree after an insertion by making the necessary splits up to
   * the root.
   * @param iLeafNode the index of the leaf node where the insertion happened
   * @param path
   */
  private void adjustTree(int iLeafNode, IntArray path) {
    int iNode;
    int iNewNode = -1;
    if (Node_size(iLeafNode) > maxCapcity) {
      // Node full. Split into two
      iNewNode = rStar ? rStarSplit(iLeafNode) : quadraticSplit(iLeafNode);
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
            iNewNode = rStar? rStarSplit(iParent) : quadraticSplit(iParent);
          }
        }
      }
    }
  }

  /**
   * The R* split algorithm operating on a leaf node as described in the
   * following paper, Page 326.
   * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
   * The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles. SIGMOD Conference 1990: 322-331
   * @param iNode the index of the node to split
   * @return the index of the new node created as a result of the split
   */
  protected int rStarSplit(int iNode) {
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

  /**
   * Split an overflow leaf node into two using the Quadratic Split method described
   * in Guttman'86 page 52.
   * @param iNode the index of the node to split
   * @return
   */
  protected int quadraticSplit(int iNode) {
    IntArray nodeChildren = children.get(iNode);
    // Pick seeds
    // Indexes of the objects to be picked as seeds in the arrays xs and ys
    // Select two entries to be the first elements of the groups
    int seed1 = -1, seed2 = -1;
    double maxD = Double.NEGATIVE_INFINITY;
    for (int i1 = 0; i1 < nodeChildren.size(); i1++) {
      int iChild1 = nodeChildren.get(i1);
      for (int i2 = i1 + 1; i2 < nodeChildren.size(); i2++) {
        int iChild2 = nodeChildren.get(i2);
        // For each pair of entries, compose a rectangle J including both of
        // them and calculate d = area(J) - area(entry1) - area(entry2)
        // Choose the most wasteful pair. Choose the pair with the largest d
        double jx1 = Math.min(x1s[iChild1], x1s[iChild2]);
        double jx2 = Math.max(x2s[iChild1], x2s[iChild2]);
        double jy1 = Math.min(y1s[iChild1], y1s[iChild2]);
        double jy2 = Math.max(y2s[iChild1], y2s[iChild2]);
        double d = (jx2 - jx1) * (jy2 - jy1) - Node_area(iChild1) - Node_area(iChild2);
        if (d > maxD) {
          maxD = d;
          seed1 = iChild1;
          seed2 = iChild2;
        }
      }
    }

    // After picking the seeds, we will start picking next elements one-by-one
    IntArray nonAssignedNodes = nodeChildren.clone();
    Node_reset(iNode, seed1);
    int iNewNode = Node_createNodeWithChildren(isLeaf.get(iNode), seed2);
    nonAssignedNodes.remove(seed1);
    nonAssignedNodes.remove(seed2);
    int group1 = iNode;
    int group2 = iNewNode;
    while (nonAssignedNodes.size() > 0) {
      // If one group has so few entries that all the rest must be assigned to it
      // in order to have the minimum number m, assign them and stop
      if (nonAssignedNodes.size() + Node_size(group1) == minCapacity) {
        // Assign all the rest to group1
        for (int iObject : nonAssignedNodes)
          Node_addChild(group1, iObject);
        nonAssignedNodes.clear();
      } else if (nonAssignedNodes.size() + Node_size(group1) == minCapacity) {
        // Assign all the rest to group2
        for (int iObject : nonAssignedNodes)
          Node_addChild(group2, iObject);
        nonAssignedNodes.clear();
      } else {
        // Invoke the algorithm  PickNext to choose the next entry to assign.
        int nextEntry = -1;
        double maxDiff = Double.NEGATIVE_INFINITY;
        for (int nonAssignedEntry : nonAssignedNodes) {
          double d1 = Node_expansion(group1, nonAssignedEntry);
          double d2 = Node_expansion(group2, nonAssignedEntry);
          double diff = Math.abs(d1 - d2);
          if (nextEntry == -1 || Math.abs(diff) > Math.abs(maxDiff)) {
            maxDiff = diff;
            nextEntry = nonAssignedEntry;
          }
        }

        // Choose which node to add the next entry to
        int iChosenNode;
        // Add it to the group whose covering rectangle will have to be enlarged
        // least to accommodate it
        if (maxDiff < 0) {
          iChosenNode = group1;
        } else if (maxDiff > 0) {
          iChosenNode = group2;
        } else {
          // Resolve ties by adding the entry to the group with smaller area
          double diffArea = Node_area(group1) - Node_area(group2);
          if (diffArea < 0) {
            iChosenNode = group1;
          } else if (diffArea > 0) {
            iChosenNode = group2;
          } else {
            // ... then to the one with fewer entries
            double diffSize = Node_size(group1) - Node_size(group2);
            if (diffSize < 0) {
              iChosenNode = group1;
            } else if (diffSize > 0) {
              iChosenNode = group2;
            } else {
              // ... then to either
              iChosenNode = Math.random() < 0.5? group1 : group2;
            }
          }
        }
        Node_addChild(iChosenNode, nextEntry);
        nonAssignedNodes.remove(nextEntry);
      }
    }
    // Add the new node to the list of nodes and return its index
    return iNewNode;
  }

  /**
   * Total number of objects in the tree.
   * @return
   */
  public int numOfDataEntries() {
    return numEntries;
  }

  /**
   * Returns number of nodes in the tree.
   * @return
   */
  public int numOfNodes() {
    return numNodes;
  }

  /**
   * Computes the height of the tree which is defined as the number of edges
   * on the path from the root to the deepest node. Sine the R-tree is perfectly
   * balanced, it is enough to measure the length of the path from the root to
   * any node, e.g., the left-most node.
   * @return
   */
  public int getHeight() {
    if (numNodes == 0)
      return 0;
    // Compute the height of the tree by traversing any path from the root
    // to the leaf.
    // Since the tree is balanced, any path would work
    int height = 1;
    int iNode = iRoot;
    while (!isLeaf.get(iNode)) {
      height++;
      iNode = children.get(iNode).get(0);
    }
    return height;
  }

  /**
   * Retrieve all the leaf nodes in the tree.
   * @return
   */
  public Rectangle[] getAllLeaves() {
    int numOfLeaves = (int) isLeaf.countOnes();
    Rectangle[] leaves = new Rectangle[numOfLeaves];
    for (int i = 0; i < numEntries + numNodes; i++) {
      if (isLeaf.get(i)) {
        Rectangle rect = new Rectangle(x1s[i], y1s[i], x2s[i], y2s[i]);
        leaves[--numOfLeaves] = rect;
      }
    }
    return leaves;
  }
}
