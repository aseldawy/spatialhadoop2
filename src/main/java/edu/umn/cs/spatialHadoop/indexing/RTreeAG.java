package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.util.ArrayList;
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

  /** The four coordinates (left, bottom, right, top) for data entries */
  private double[] x1s, y1s, x2s, y2s;

  /** Maximum capacity of a node */
  private final int maxCapcity;

  /** Minimum capacity of a node. */
  private final int minCapacity;

  /**If this flag is true, the R* implementation is used*/
  private boolean rStar;

  /**
   * A data structure for a node that works for both leaf and non-leaf nodes.
   * For non-leaf nodes, children contains indexes to child nodes in a bigger
   * array of nodes.
   * For leaf nodes, children contain indexes to objects in a bigger array of
   * objects.
   */
  static class Node extends Rectangle {
    boolean leaf;
    IntArray children;

    private Node() {}

    static Node createLeaf(int iEntry, double x1, double y1, double x2, double y2) {
      return new Node().resetLeafNode(iEntry, x1, y1, x2, y2);
    }

    static Node createNonLeaf(int iNode1, int iNode2, Node n1, Node n2) {
      Node nonLeaf = new Node();
      nonLeaf.children = new IntArray();
      nonLeaf.expand(n1);
      nonLeaf.expand(n2);
      nonLeaf.children.add(iNode1);
      nonLeaf.children.add(iNode2);
      return nonLeaf;
    }

    public static Node createNonLeafNode(int iNode, Node node) {
      return new Node().resetNonLeafNode(iNode, node);
    }

    public Node resetLeafNode(int iEntry, double x1, double y1, double x2, double y2) {
      this.children = new IntArray();
      this.children.add(iEntry);
      this.set(x1, y2, x2, y2);
      this.leaf = true;
      return this;
    }

    public Node resetNonLeafNode(int iNode, Node node) {
      this.children = new IntArray();
      this.children.add(iNode);
      this.set(node);
      this.leaf = false;
      return this;
    }

    public int size() { return children.size();}

    /**
     * Calculates the area of the node
     * @return
     */
    public double area() { return getWidth() * getHeight();}

    /**
     * Calculates the expansion that will happen if the given point is added
     * to this node
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @return
     */
    public double expansion(double x1, double y1, double x2, double y2) {
      double newWidth = this.getWidth();
      double newHeight = this.getHeight();
      if (x1 < this.x1)
        newWidth += (this.x1 - x1);
      else if (x2 > this.x2)
        newWidth += (x2 - this.x2);
      if (y1 < this.y1)
        newHeight += (this.y1 - y1);
      else if (y2 > this.y2)
        newHeight += (this.y2 - y2);

      return newWidth * newHeight - getWidth() * getHeight();
    }

    /**
     * Calculates the expansion when the given MBR is added to this node
     * @param mbr
     * @return
     */
    public double expansion(Rectangle mbr) {
      double newWidth = this.getWidth();
      double newHeight = this.getHeight();
      if (mbr.x1 < this.x1)
        newWidth += (this.x1 - mbr.x1);
      else if (mbr.x2 > this.x2)
        newWidth += (mbr.x2 - this.x2);
      if (mbr.y1 < this.y1)
        newHeight += (this.y1 - mbr.y1);
      else if (mbr.y2 > this.y2)
        newHeight += (this.y2 - mbr.y2);

      return newWidth * newHeight - getWidth() * getHeight();
    }

    private void addEntry(int iEntry, double x1, double y1, double x2, double y2) {
      this.children.add(iEntry);
      // Expand the MBR to enclose the given point
      this.expand(x1, y1);
      this.expand(x2, y2);
    }

    private void addChildNode(int iNode, Rectangle mbr) {
      this.children.add(iNode);
      this.expand(mbr);
    }

    /**
     * Split the node along the given separator and return the newly created
     * node.
     * @param separator
     * @param xs
     * @param ys
     * @return
     */
    public Node splitLeafNode(int separator, double[] xs, double[] ys) {
      // Create the new node that will hold the entries from separator -> size
      Node newNode = new Node();
      newNode.leaf = true;
      // Recompute the two MBRs at the cut line
      this.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = 0; i < separator; i++) {
        int iChild = this.children.get(i);
        this.expand(xs[iChild], ys[iChild]);
      }
      newNode.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < this.size(); i++) {
        int iChild = this.children.get(i);
        newNode.expand(xs[iChild], ys[iChild]);
      }

      // Adjust the children at each node
      newNode.children = new IntArray();
      newNode.children.append(children, separator, children.size() - separator);
      children.resize(separator);

      return newNode;
    }

    public Node splitNonLeafNode(int separator, List<Node> nodes) {
      // Create the new node that will hold the entries from separator -> size
      Node newNode = new Node();
      newNode.leaf = false;
      // Recompute the two MBRs at the cut line
      this.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = 0; i < separator; i++) {
        int iChild = this.children.get(i);
        this.expand(nodes.get(iChild));
      }
      newNode.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < this.size(); i++) {
        int iChild = this.children.get(i);
        newNode.expand(nodes.get(iChild));
      }

      // Adjust the children at each node
      newNode.children = new IntArray();
      newNode.children.append(children, separator, children.size() - separator);
      children.resize(separator);

      return newNode;
    }

  }

  /**
   * All nodes in the tree.
   */
  protected List<Node> objects;
  /**The index of the root in the list of nodes*/
  protected int iRoot;

  /**
   * Construct a new RTree on the given set of points.
   * @param xs - x-coordinates for the points
   * @param ys - y-coordinates for the points
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity - Maximum capacity of a node
   * @param rStar - When set to true, R* split algorithm is applied
   */
  public RTreeAG(double[] xs, double[] ys, int minCapacity, int maxCapcity, boolean rStar) {
    this.x1s = new double[xs.length];
    this.y1s = new double[xs.length];
    this.x2s = new double[xs.length];
    this.y2s = new double[xs.length];
    objects = new ArrayList<Node>(xs.length);
    for (int i = 0; i < xs.length; i++) {
      this.x1s[i] = xs[i];
      this.y1s[i] = ys[i];
      this.x2s[i] = Math.nextUp(xs[i]);
      this.y2s[i] = Math.nextUp(ys[i]);
      objects.add(null); // Add a null placeholder for nodes
    }
    this.maxCapcity = maxCapcity;
    this.minCapacity = minCapacity;
    this.rStar = rStar;

    Node rootNode = Node.createLeaf(0, x1s[0], y1s[0], x2s[0], y2s[0]);
    iRoot = objects.size();
    objects.add(rootNode);
    // Insert one by one
    for (int i = 1; i < xs.length; i++)
      insert(i);
  }

  /**
   * Inserts the given point to the tree.
   * @param iEntry - The index of the point in the array of points
   */
  private void insert(int iEntry) {
    // The path from the root to the newly inserted record. Used for splitting.
    IntArray path = new IntArray();
    int iNode = iRoot;
    path.add(iNode);
    Node leafNode;
    while (!(leafNode = objects.get(iNode)).leaf) {
      double minExpansion = Double.POSITIVE_INFINITY;
      int iChildWithMinExpansion = 0;
      // Node is not leaf. Choose a child node
      for (int iChild : leafNode.children) {
        Node child = objects.get(iChild);
        double expansion = child.expansion(x1s[iEntry], y1s[iEntry], x2s[iEntry], y2s[iEntry]);
        if (expansion < minExpansion) {
          // Choose the child with the minimum expansion
          minExpansion = expansion;
          iChildWithMinExpansion = iChild;
        } else if (expansion == minExpansion) {
          // Resolve ties by choosing the entry with the rectangle of smallest area
          if (child.area() < objects.get(iChildWithMinExpansion).area())
            iChildWithMinExpansion = iChild;
        }
      }
      iNode = iChildWithMinExpansion;
      path.add(iNode);
    }

    // Now we have a child node. Insert the current element to it and split
    // if necessary
    leafNode.addEntry(iEntry, x1s[iEntry], y1s[iEntry], x2s[iEntry], y2s[iEntry]);
    adjustTree(leafNode, path);
  }

  /**
   * Adjust the tree after an insertion by making the necessary splits up to
   * the root.
   * @param leafNode
   * @param path
   */
  private void adjustTree(Node leafNode, IntArray path) {
    int iNode;
    int iNewNode = -1;
    if (leafNode.size() >= maxCapcity) {
      // Node full. Split into two
      iNewNode = rStar ? rStarSplitLeaf(leafNode) : quadraticSplitLeaf(leafNode);
    }
    // AdjustTree. Ascend from the leaf node L
    while (!path.isEmpty()) {
      iNode = path.pop();
      if (path.isEmpty()) {
        // The node is the root (no parent)
        if (iNewNode != -1) {
          // If the root is split, create a new root
          Node newRoot = Node.createNonLeaf(iNode, iNewNode, objects.get(iNode), objects.get(iNewNode));
          iRoot = objects.size();
          objects.add(newRoot);
        }
        // If N is the root with no partner NN, stop.
      } else {
        Node parent = objects.get(path.peek());
        // Adjust covering rectangle in parent entry
        Node node = objects.get(iNode);
        parent.expand(node);
        if (iNewNode != -1) {
          // If N has a partner NN resulting from an earlier split,
          // create a new entry ENN and add to the parent if there is room.
          // Add Enn to P if there is room
          parent.addChildNode(iNewNode, objects.get(iNewNode));
          iNewNode = -1;
          if (parent.size() >= maxCapcity) {
            iNewNode = rStar? rStarSplitNonLeaf(parent) : quadraticSplitNonLeaf(parent);
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
   * @param node
   * @return the index of the new node created as a result of the split
   */
  protected int rStarSplitLeaf(final Node node) {
    // ChooseSplitAxis
    // Sort the entries by each axis and compute S, the sum of all margin-values
    // of the different distributions

    // Sort by x-axis
    QuickSort quickSort = new QuickSort();
    IndexedSortable sortX = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diffX = x1s[node.children.get(i)] - x1s[node.children.get(j)];
        if (diffX < 0) return -1;
        if (diffX > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        node.children.swap(i, j);
      }
    };
    quickSort.sort(sortX, 0, node.size());
    double sumMarginX = computeSumMarginLeaf(node);

    IndexedSortable sortY = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diffY = y1s[node.children.get(i)] - y1s[node.children.get(j)];
        if (diffY < 0) return -1;
        if (diffY > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        node.children.swap(i, j);
      }
    };
    quickSort.sort(sortY, 0, node.size());
    double sumMarginY = computeSumMarginLeaf(node);
    if (sumMarginX < sumMarginY) {
      // Choose the axis with the minimum S as split axis.
      quickSort.sort(sortX, 0, node.size());
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
        int iChild = node.children.get(i);
        mbr1.expand(x1s[iChild], y1s[iChild]);
      }
      mbr2.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < node.size(); i++) {
        int iChild = node.children.get(i);
        mbr2.expand(x1s[iChild], y1s[iChild]);
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
    Node newNode = node.splitLeafNode(separator, x1s, y1s);
    objects.add(newNode);
    return objects.size() - 1;
  }

  /**
   * Compute the sum margin of the given node assuming that the children have
   * been already sorted along one of the dimensions.
   * @param node
   * @return
   */
  private double computeSumMarginLeaf(Node node) {
    double sumMargin = 0.0;
    Rectangle mbr1 = new Rectangle();
    Rectangle mbr2 = new Rectangle();
    for (int k = 1; k <= maxCapcity - 2 * minCapacity + 2; k++) {
      int separator = minCapacity - 1 + k;
      mbr1.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = 0; i < separator; i++) {
        int iChild = node.children.get(i);
        mbr1.expand(x1s[iChild], y1s[iChild]);
      }
      mbr2.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < node.size(); i++) {
        int iChild = node.children.get(i);
        mbr2.expand(x1s[iChild], y1s[iChild]);
      }
      sumMargin += mbr1.getWidth() + mbr1.getHeight();
      sumMargin += mbr2.getWidth() + mbr2.getHeight();
    }
    return sumMargin;
  }

  /**
   * Compute the sum margin of the given node assuming that the children have
   * been already sorted along one of the dimensions.
   * @param node
   * @return
   */
  private double computeSumMarginNonLeaf(Node node) {
    double sumMargin = 0.0;
    Rectangle mbr1 = new Rectangle();
    Rectangle mbr2 = new Rectangle();
    for (int k = 1; k <= maxCapcity - 2 * minCapacity + 2; k++) {
      int separator = minCapacity - 1 + k;
      mbr1.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = 0; i < separator; i++) {
        int iChild = node.children.get(i);
        mbr1.expand(objects.get(iChild));
      }
      mbr2.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < node.size(); i++) {
        int iChild = node.children.get(i);
        mbr2.expand(objects.get(iChild));
      }
      sumMargin += mbr1.getWidth() + mbr1.getHeight();
      sumMargin += mbr2.getWidth() + mbr2.getHeight();
    }
    return sumMargin;
  }

  /**
   * The R* split algorithm operating on a non-leaf node as described in the
   * paper, Page 326.
   * @param node
   * @return the index of the new node created as a result of the split
   */
  protected int rStarSplitNonLeaf(final Node node) {
    // ChooseSplitAxis
    // Sort the entries by each axis and compute S, the sum of all margin-values
    // of the different distributions

    // Sort by x-axis
    QuickSort quickSort = new QuickSort();
    IndexedSortable sortX = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diffX1 = objects.get(node.children.get(i)).x1 - objects.get(node.children.get(j)).x1;
        if (diffX1 < 0)
          return -1;
        else if (diffX1 > 0)
          return 1;
        else {
          // Same x1, sort on x2
          double diffX2 = objects.get(node.children.get(i)).x2 - objects.get(node.children.get(j)).x2;
          if (diffX1 < 0)
            return -1;
          else if (diffX1 > 0)
            return 1;
          else
            return 0;
        }
      }

      @Override
      public void swap(int i, int j) {
        node.children.swap(i, j);
      }
    };
    quickSort.sort(sortX, 0, node.size());
    double sumMarginX = computeSumMarginNonLeaf(node);

    IndexedSortable sortY = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diffY1 = objects.get(node.children.get(i)).y1 - objects.get(node.children.get(j)).y1;
        if (diffY1 < 0)
          return -1;
        else if (diffY1 > 0)
          return 1;
        else {
          // Same y1, sort on y2
          double diffY2 = objects.get(node.children.get(i)).y2 - objects.get(node.children.get(j)).y2;
          if (diffY1 < 0)
            return -1;
          else if (diffY1 > 0)
            return 1;
          else
            return 0;
        }
      }

      @Override
      public void swap(int i, int j) {
        node.children.swap(i, j);
      }
    };
    quickSort.sort(sortY, 0, node.size());
    double sumMarginY = computeSumMarginNonLeaf(node);
    if (sumMarginX < sumMarginY) {
      // Choose the axis with the minimum S as split axis.
      quickSort.sort(sortX, 0, node.size());
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
        int iChild = node.children.get(i);
        mbr1.expand(objects.get(iChild));
      }
      mbr2.set(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
      for (int i = separator; i < node.size(); i++) {
        int iChild = node.children.get(i);
        mbr2.expand(objects.get(iChild));
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
    Node newNode = node.splitNonLeafNode(separator, objects);
    objects.add(newNode);
    return objects.size() - 1;
  }

  /**
   * Split an overflow leaf node into two using the Quadratic Split method described
   * in Guttman'86 page 52.
   * @param oldNode
   * @return the index of the new node that resulted of the split
   */
  protected int quadraticSplitLeaf(Node oldNode) {
    // Pick seeds
    // Indexes of the objects to be picked as seeds in the arrays xs and ys
    // Select two entries to be the first elements of the groups
    int seed1 = -1, seed2 = -1;
    double maxD = Double.NEGATIVE_INFINITY;
    for (int i1 = 0; i1 < oldNode.size(); i1++) {
      int iEntry1 = oldNode.children.get(i1);
      for (int i2 = i1 + 1; i2 < oldNode.size(); i2++) {
        int iEntry2 = oldNode.children.get(i2);
        // For each pair of entries, compose a rectangle J including both of
        // them and calculate d = area(J) - area(entry1) - area(entry2)
        // Choose the most wasteful pair. Choose the pair with the largest d
        double jx1 = Math.min(x1s[iEntry1], x1s[iEntry2]);
        double jx2 = Math.max(x2s[iEntry1], x2s[iEntry2]);
        double jy1 = Math.min(y1s[iEntry1], y1s[iEntry2]);
        double jy2 = Math.max(y2s[iEntry1], y2s[iEntry2]);
        double d = (jx2 - jx1) * (jy2 - jy1) -
            (x2s[iEntry1] - x1s[iEntry1]) * (y2s[iEntry1] - y1s[iEntry1]) -
            (x2s[iEntry2] - x1s[iEntry2]) * (y2s[iEntry2] - y1s[iEntry2]);
        if (d > maxD) {
          maxD = d;
          seed1 = iEntry1;
          seed2 = iEntry2;
        }
      }
    }

    // After picking the seeds, we will start picking next elements one-by-one
    IntArray nonAssignedEntries = oldNode.children;
    oldNode.resetLeafNode(seed1, x1s[seed1], y1s[seed1], x2s[seed1], y2s[seed1]);
    Node newNode = Node.createLeaf(seed2, x1s[seed2], y1s[seed2], x2s[seed2], y2s[seed2]);
    Node group1 = oldNode;
    Node group2 = newNode;
    nonAssignedEntries.remove(seed1);
    nonAssignedEntries.remove(seed2);
    while (nonAssignedEntries.size() > 0) {
      // If one group has so few entries that all the rest must be assigned to it
      // in order to have the minimum number m, assign them and stop
      if (nonAssignedEntries.size() + group1.size() == minCapacity) {
        // Assign all the rest to group1
        for (int iEntry : nonAssignedEntries)
          group1.addEntry(iEntry, x1s[iEntry], y1s[iEntry], x2s[iEntry], y2s[iEntry]);
        nonAssignedEntries.clear();
      } else if (nonAssignedEntries.size() + group2.size() == minCapacity) {
        // Assign all the rest to newNode
        for (int iEntry : nonAssignedEntries)
          group2.addEntry(iEntry, x1s[iEntry], y1s[iEntry], x2s[iEntry], y2s[iEntry]);
        nonAssignedEntries.clear();
      } else {
        // Invoke the algorithm  PickNext to choose the next entry to assign.
        int nextEntry = -1;
        double maxDiff = Double.NEGATIVE_INFINITY;
        for (int nonAssignedEntry : nonAssignedEntries) {
          double d1 = group1.expansion(x1s[nonAssignedEntry], y2s[nonAssignedEntry],
              x2s[nonAssignedEntry], y2s[nonAssignedEntry]);
          double d2 = group2.expansion(x1s[nonAssignedEntry], y2s[nonAssignedEntry],
              x2s[nonAssignedEntry], y2s[nonAssignedEntry]);
          double diff = d1 - d2;
          if (nextEntry == -1 || Math.abs(diff) > Math.abs(maxDiff)) {
            maxDiff = diff;
            nextEntry = nonAssignedEntry;
          }
        }

        Node chosenNode;
        // Add it to the group whose covering rectangle will have to be enlarged
        // least to accommodate it
        if (maxDiff < 0) {
          chosenNode = group1;
        } else if (maxDiff > 0) {
          chosenNode = group2;
        } else {
          // Resolve ties by adding the entry to the group with smaller area
          double diffArea = group1.area() - group2.area();
          if (diffArea < 0) {
            chosenNode = group1;
          } else if (diffArea > 0) {
            chosenNode = group2;
          } else {
            // ... then to the one with fewer entries
            double diffSize = group1.size() - group2.size();
            if (diffSize < 0) {
              chosenNode = group1;
            } else if (diffSize > 0) {
              chosenNode = group2;
            } else {
              // ... then to either
              chosenNode = Math.random() < 0.5? group1 : group2;
            }
          }
        }
        chosenNode.addEntry(nextEntry, x1s[nextEntry], y1s[nextEntry],
            x2s[nextEntry], y2s[nextEntry]);
        nonAssignedEntries.remove(nextEntry);
      }
    }
    // Add the new node to the list of nodes and return its index
    objects.add(newNode);
    return objects.size() - 1;
  }

  /**
   * Split an overflow leaf node into two using the Quadratic Split method described
   * in Guttman'86 page 52.
   * @param oldNode
   * @return
   */
  protected int quadraticSplitNonLeaf(Node oldNode) {
    // Pick seeds
    // Indexes of the objects to be picked as seeds in the arrays xs and ys
    // Select two entries to be the first elements of the groups
    int seed1 = -1, seed2 = -1;
    double maxD = Double.NEGATIVE_INFINITY;
    for (int i1 = 0; i1 < oldNode.size(); i1++) {
      int iNode1 = oldNode.children.get(i1);
      Node node1 = objects.get(iNode1);
      for (int i2 = i1 + 1; i2 < oldNode.size(); i2++) {
        int iNode2 = oldNode.children.get(i2);
        Node node2 = objects.get(iNode2);
        // For each pair of entries, compose a rectangle J including both of
        // them and calculate d = area(J) - area(entry1) - area(entry2)
        // Choose the most wasteful pair. Choose the pair with the largest d
        double jx1 = Math.min(node1.x1, node2.x1);
        double jx2 = Math.max(node1.x2, node2.x2);
        double jy1 = Math.min(node1.y1, node2.y1);
        double jy2 = Math.max(node1.y2, node2.y2);
        double d = (jx2 - jx1) * (jy2 - jy1) - node1.area() - node2.area();
        if (d > maxD) {
          maxD = d;
          seed1 = iNode1;
          seed2 = iNode2;
        }
      }
    }

    // After picking the seeds, we will start picking next elements one-by-one
    IntArray nonAssignedNodes = oldNode.children;
    oldNode.resetNonLeafNode(seed1, objects.get(seed1));
    Node newNode = Node.createNonLeafNode(seed2, objects.get(seed2));
    Node group1 = oldNode;
    Node group2 = newNode;
    nonAssignedNodes.remove(seed1);
    nonAssignedNodes.remove(seed2);
    while (nonAssignedNodes.size() > 0) {
      // If one group has so few entries that all the rest must be assigned to it
      // in order to have the minimum number m, assign them and stop
      if (nonAssignedNodes.size() + group1.size() == minCapacity) {
        // Assign all the rest to group1
        for (int iEntry : nonAssignedNodes)
          group1.addChildNode(iEntry, objects.get(iEntry));
        nonAssignedNodes.clear();
      } else if (nonAssignedNodes.size() + group2.size() == minCapacity) {
        // Assign all the rest to newNode
        for (int iEntry : nonAssignedNodes)
          group2.addChildNode(iEntry, objects.get(iEntry));
        nonAssignedNodes.clear();
      } else {
        // Invoke the algorithm  PickNext to choose the next entry to assign.
        int nextEntry = -1;
        double maxDiff = Double.NEGATIVE_INFINITY;
        for (int nonAssignedEntry : nonAssignedNodes) {
          double d1 = group1.expansion(objects.get(nonAssignedEntry));
          double d2 = group2.expansion(objects.get(nonAssignedEntry));
          double diff = Math.abs(d1 - d2);
          if (nextEntry == -1 || Math.abs(diff) > Math.abs(maxDiff)) {
            maxDiff = diff;
            nextEntry = nonAssignedEntry;
          }
        }

        // Choose which node to add the next entry to
        Node chosenNode;
        // Add it to the group whose covering rectangle will have to be enlarged
        // least to accommodate it
        if (maxDiff < 0) {
          chosenNode = group1;
        } else if (maxDiff > 0) {
          chosenNode = group2;
        } else {
          // Resolve ties by adding the entry to the group with smaller area
          double diffArea = group1.area() - group2.area();
          if (diffArea < 0) {
            chosenNode = group1;
          } else if (diffArea > 0) {
            chosenNode = group2;
          } else {
            // ... then to the one with fewer entries
            double diffSize = group1.size() - group2.size();
            if (diffSize < 0) {
              chosenNode = group1;
            } else if (diffSize > 0) {
              chosenNode = group2;
            } else {
              // ... then to either
              chosenNode = Math.random() < 0.5? group1 : group2;
            }
          }
        }
        chosenNode.addChildNode(nextEntry, objects.get(nextEntry));
        nonAssignedNodes.remove(nextEntry);
      }
    }
    // Add the new node to the list of nodes and return its index
    objects.add(newNode);
    return objects.size() - 1;
  }

  /**
   * Total number of objects in the tree.
   * @return
   */
  public int numOfDataEntries() {
    return x1s.length;
  }

  /**
   * Returns number of nodes in the tree.
   * @return
   */
  public int numOfNodes() {
    return objects.size() - numOfDataEntries();
  }

  /**
   * Computes the height of the tree which is defined as the number of edges
   * on the path from the root to the deepest node. Sine the R-tree is perfectly
   * balanced, it is enough to measure the length of the path from the root to
   * any node, e.g., the left-most node.
   * @return
   */
  public int getHeight() {
    if (objects.isEmpty())
      return 0;
    // Compute the height of the tree by traversing any path from the root
    // to the leaf.
    // Since the tree is balanced, any path would work
    int height = 1;
    int iNode = iRoot;
    while (!objects.get(iNode).leaf) {
      height++;
      iNode = objects.get(iNode).children.get(0);
    }
    return height;
  }

  /**
   * Retreive all the leaf nodes in the tree.
   * @return
   */
  public Rectangle[] getAllLeaves() {
    int numOfLeaves = 0;
    for (int i = numOfDataEntries(); i < objects.size(); i++) {
      if (objects.get(i).leaf)
        numOfLeaves++;
    }
    Rectangle[] leaves = new Rectangle[numOfLeaves];
    for (int i = numOfDataEntries(); i < objects.size(); i++) {
      if (objects.get(i).leaf) {
        leaves[--numOfLeaves] = objects.get(i);
      }
    }
    return leaves;
  }
}
