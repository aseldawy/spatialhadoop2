package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import sun.security.krb5.internal.crypto.Des;

import java.io.*;
import java.util.*;

/**
 * A partial implementation for the original Antonin Guttman R-tree as described
 * in the following paper.
 * Antonin Guttman: R-Trees: A Dynamic Index Structure for Spatial Searching.
 * SIGMOD Conference 1984: 47-57
 *
 * It only contain the implementation of the parts needed for the indexing
 * methods. For example, the delete operation was not implemented as it is
 * not needed. Also, this index is designed mainly to be used to index a sample
 * in memory and use it for the partitioning. So, the disk-based mapping and
 * search were not implemented for simplicity.
 */
public class RTreeGuttman implements Closeable {
  /** Maximum capacity of a node */
  protected final int maxCapcity;

  /** Minimum capacity of a node. */
  protected final int minCapacity;

  /** The four coordinates (left, bottom, right, top) for objects (entries + nodes) */
  protected double[] x1s, y1s, x2s, y2s;

  /**A bit vector that stores which nodes are leaves*/
  protected BitArray isLeaf;

  /**A list of int[] that stores the children of each node*/
  protected List<IntArray> children;

  /**Total number of data entries*/
  protected int numEntries;

  /**Total number of nodes*/
  protected int numNodes;

  /**The index of the root in the list of nodes*/
  protected int root;

  /**Only when processing an on-disk tree. Stores the offset of each data entry in the file*/
  protected int[] entryOffsets;

  /**A deserializer that reads objects stored on disk*/
  private Deserializer<?> deser;

  /**The input stream that points to the underlying file*/
  private FSDataInputStream in;

  /**When reading the tree from disk. The offset of the beginning of the tree*/
  private long treeStartOffset;

  /**The total size of the data chunk*/
  private int totalDataSize;

  /**
   * Make a room in the data structures to accommodate a new object whether
   * it is a node or a data entry.
   */
  protected void makeRoomForOneMoreObject() {
    if (x1s.length <= numEntries + numNodes) {
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
   * Calculates the volume (area) expansion that will happen if the given object
   * is added to a given node.
   * @param iNode the ID of the node that would be expanded
   * @param iNewChild the ID of the object that would be added to the node
   * @return
   */
  protected double Node_volumeExpansion(int iNode, int iNewChild) {
    double widthB4Expansion = x2s[iNode] - x1s[iNode];
    double heightB4Expansion = y2s[iNode] - y1s[iNode];
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
  }

  /**
   * Expand the MBR of the given node to enclose the given new object
   * @param node
   * @param newObject
   */
  protected void Node_expand(int node, int newObject) {
    // Expand the MBR to enclose the new child
    x1s[node] = Math.min(x1s[node], x1s[newObject]);
    y1s[node] = Math.min(y1s[node], y1s[newObject]);
    x2s[node] = Math.max(x2s[node], x2s[newObject]);
    y2s[node] = Math.max(y2s[node], y2s[newObject]);
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
   * Initialize the current R-tree from given data entries
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   */
  public void initializeFromRects(double[] x1, double[] y1, double[] x2, double[] y2) {
    this.initializeDataEntries(x1, y1, x2, y2);
    this.insertAllDataEntries();
  }

  /**
   * Initialize the tree from a set of points
   * @param xs
   * @param ys
   */
  public void initializeFromPoints(double[] xs, double[] ys) {
    this.initializeDataEntries(xs, ys);
    this.insertAllDataEntries();
  }


  /**
   * Construct a new empty R-tree with the given parameters.
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity - Maximum capacity of a node
   */
  public RTreeGuttman(int minCapacity, int maxCapcity) {
    this.minCapacity = minCapacity;
    this.maxCapcity = maxCapcity;
  }

  protected void insertAllDataEntries() {
    root = Node_createNodeWithChildren(true, 0);
    // Insert one by one
    for (int i = 1; i < numEntries; i++)
      insertAnExistingDataEntry(i);
  }

  /**
   * Initialize the data entries to a set of point coordinates without actually
   * inserting them into the tree structure.
   * @param xs
   * @param ys
   */
  protected void initializeDataEntries(double[] xs, double[] ys) {
    this.numEntries = xs.length;
    this.numNodes = 0; // Initially, no nodes are there
    this.isLeaf = new BitArray(numEntries);
    children = new ArrayList<IntArray>(numEntries);
    this.x1s = new double[numEntries];
    this.y1s = new double[numEntries];
    this.x2s = new double[numEntries];
    this.y2s = new double[numEntries];
    for (int i = 0; i < numEntries; i++) {
      this.x1s[i] = xs[i];
      this.y1s[i] = ys[i];
      this.x2s[i] = xs[i];
      this.y2s[i] = ys[i];
      children.add(null); // data entries do not have children
    }
  }

  /**
   * Initialize the data entries to a set of rectangular coordinates without
   * actually inserting them into the tree structure.
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   */
  protected void initializeDataEntries(double[] x1, double[] y1, double[] x2, double[] y2) {
    this.numEntries = x1.length;
    this.numNodes = 0; // Initially, no nodes are there
    this.isLeaf = new BitArray(numEntries);
    children = new ArrayList<IntArray>(numEntries);
    this.x1s = new double[numEntries];
    this.y1s = new double[numEntries];
    this.x2s = new double[numEntries];
    this.y2s = new double[numEntries];
    for (int i = 0; i < numEntries; i++) {
      this.x1s[i] = x1[i];
      this.y1s[i] = y1[i];
      this.x2s[i] = x2[i];
      this.y2s[i] = y2[i];
      children.add(null); // data entries do not have children
    }
  }


  /**
   * Inserts the given data entry into the tree. We assume that the coordinates
   * of this data entry are already stored in the coordinates arrays.
   * @param iEntry - The index of the point in the array of points
   */
  protected void insertAnExistingDataEntry(int iEntry) {
    // The path from the root to the newly inserted record. Used for splitting.
    IntArray path = new IntArray();
    int iCurrentVisitedNode = root;
    path.add(iCurrentVisitedNode);
    // Descend in the tree until we find a leaf node to add the object to
    while (!isLeaf.get(iCurrentVisitedNode)) {
      // Node is not leaf. Choose a child node
      // Descend to the best child found
      int iBestChild = chooseSubtree(iEntry, iCurrentVisitedNode);
      iCurrentVisitedNode = iBestChild;
      path.add(iCurrentVisitedNode);
    }

    // Now we have a child node. Insert the current element to it and split
    // if necessary
    Node_addChild(iCurrentVisitedNode, iEntry);
    adjustTree(iCurrentVisitedNode, path);
  }

  /**
   * Choose the best subtree to add a data entry to.
   * According to the original R-tree paper, this function chooses the node with
   * the minimum volume expansion, then the one with the smallest volume,
   * then the one with the least number of records, then randomly to any one.
   * @param iEntry
   * @param iNode
   * @return
   */
  protected int chooseSubtree(int iEntry, int iNode) {
    // 1. Choose the child with the minimum expansion
    double minExpansion = Double.POSITIVE_INFINITY;
    int iBestChild = 0;
    for (int iCandidateChild : children.get(iNode)) {
      double expansion = Node_volumeExpansion(iCandidateChild, iEntry);
      if (expansion < minExpansion) {
        minExpansion = expansion;
        iBestChild = iCandidateChild;
      } else if (expansion == minExpansion) {
        // Resolve ties by choosing the entry with the rectangle of smallest area
        if (Node_area(iCandidateChild) < Node_area(iBestChild))
          iBestChild = iCandidateChild;
      }
    }
    return iBestChild;
  }

  /**
   * Adjust the tree after an insertion by making the necessary splits up to
   * the root.
   * @param leafNode the index of the leaf node where the insertion happened
   * @param path
   */
  protected void adjustTree(int leafNode, IntArray path) {
    int iNode;
    int newNode = -1;
    if (Node_size(leafNode) > maxCapcity) {
      // Node full. Split into two
      newNode = split(leafNode, minCapacity);
    }
    // AdjustTree. Ascend from the leaf node L
    while (!path.isEmpty()) {
      iNode = path.pop();
      // Adjust covering rectangle in the node
      Node_expand(iNode, children.get(iNode).peek());
      if (path.isEmpty()) {
        // The node is the root (no parent)
        if (newNode != -1) {
          // If the root is split, create a new root
          root = Node_createNodeWithChildren(false, iNode, newNode);
        }
        // If N is the root with no partner NN, stop.
      } else {
        int parent = path.peek();
        if (newNode != -1) {
          // If N has a partner NN resulting from an earlier split,
          // create a new entry ENN and add to the parent if there is room.
          // Add Enn to P if there is room
          Node_addChild(parent, newNode);
          newNode = -1;
          if (Node_size(parent) >= maxCapcity) {
            newNode = split(parent, minCapacity);
          }
        }
      }
    }
  }

  /**
   * Split an overflow leaf node into two using the Quadratic Split method described
   * in Guttman'84 page 52.
   * @param iNode the index of the node to split
   * @param minSplitSize Minimum size of each split, typically, {@link #minCapacity}
   * @return
   */
  protected int split(int iNode, int minSplitSize) {
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
      // in order to have the minimum number minSplitSize, assign them and stop
      if (nonAssignedNodes.size() + Node_size(group1) <= minSplitSize) {
        // Assign all the rest to group1
        for (int iObject : nonAssignedNodes) {
          Node_addChild(group1, iObject);
          Node_expand(group1, iObject);
        }
        nonAssignedNodes.clear();
      } else if (nonAssignedNodes.size() + Node_size(group2) <= minSplitSize) {
        // Assign all the rest to group2
        for (int iObject : nonAssignedNodes) {
          Node_addChild(group2, iObject);
          Node_expand(group2, iObject);
        }
        nonAssignedNodes.clear();
      } else {
        // Invoke the algorithm  PickNext to choose the next entry to assign.
        int nextEntry = -1;
        double maxDiff = Double.NEGATIVE_INFINITY;
        for (int nonAssignedEntry : nonAssignedNodes) {
          double d1 = Node_volumeExpansion(group1, nonAssignedEntry);
          double d2 = Node_volumeExpansion(group2, nonAssignedEntry);
          double diff = d1 - d2;
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
        Node_expand(iChosenNode, nextEntry);
        nonAssignedNodes.remove(nextEntry);
      }
    }
    // Add the new node to the list of nodes and return its index
    return iNewNode;
  }

  /**
   * Search for all the entries that overlap a given query rectangle
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @param results the results as a list of entry IDs as given in the construction
   *                function
   */
  public void search(double x1, double y1, double x2, double y2, IntArray results) {
    results.clear();
    IntArray nodesToSearch = new IntArray();
    nodesToSearch.add(root);
    while (!nodesToSearch.isEmpty()) {
      int nodeToSearch = nodesToSearch.pop();
      if (isLeaf.get(nodeToSearch)) {
        // Search and return all the entries in the leaf node
        for (int iEntry : children.get(nodeToSearch)) {
          if (Object_overlaps(iEntry, x1, y1, x2, y2))
            results.add(iEntry);
        }
      } else {
        // A non-leaf node, expand the search to all overlapping children
        for (int iChild : children.get(nodeToSearch)) {
          if (Object_overlaps(iChild, x1, y1, x2, y2))
            nodesToSearch.add(iChild);
        }
      }
    }
  }

  public Iterable<Entry> search(double x1, double y1, double x2, double y2) {
    return new SearchIterator(x1, y1, x2, y2);
  }

  /**
   * Tests if an object (entry or node) overlaps with a rectangle
   * @param iEntry
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @return
   */
  protected boolean Object_overlaps(int iEntry, double x1, double y1, double x2, double y2) {
    return !(x2 < x1s[iEntry] || x2s[iEntry] < x1 ||
             y2 < y1s[iEntry] || y2s[iEntry] < y1);
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
    int height = 0;
    int iNode = root;
    while (!isLeaf.get(iNode)) {
      height++;
      iNode = children.get(iNode).get(0);
    }
    return height;
  }

  /**
   * The total number of leaf nodes.
   * @return
   */
  public int getNumLeaves() {
    return (int) isLeaf.countOnes();
  }

  /**
   * Retrieve all the leaf nodes in the tree.
   * @return
   */
  public Iterable<Node> getAllLeaves() {
    return new LeafNodeIterable();
  }

  /**
   * Creates an R-tree that contains only nodes (no data entries). The given
   * coordinates are used for the leaf nodes. This tree is used to model an R-tree
   * and use the different R-tree algorithms to test where an entry would end up
   * in the R-tree (without really inserting it).
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @see #noInsert(double, double, double, double)
   */
  protected void initializeHollowRTree(double[] x1, double[] y1, double[] x2, double[] y2) {
    // Create a regular R-tree with the given rectangles as data entries.
    initializeFromRects(x1, y1, x2, y2);

    // Make sure we have a room for an extra object which will be used in noInsert
    makeRoomForOneMoreObject();
  }

  /**
   * Simulates an insertion of a record and returns the ID of the object that either
   * contains the given boundaries or will be its sibling.
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @return
   */
  protected int noInsert(double x1, double y1, double x2, double y2) {
    int i = numEntries + numNodes;
    x1s[i] = x1;
    y1s[i] = y1;
    x2s[i] = x2;
    y2s[i] = y2;

    // Descend from the root until reaching a data entry
    // The range of IDs for data entries is [0, numEntries[
    // All node IDs is in the rnage [numEntries, numEntries + numNodes[
    int p = root;
    while (p >= numOfDataEntries())
      p = chooseSubtree(i, p);

    // Return the index of the leaf node without really inserting the element
    return p;
  }

  /**
   * Only when the tree is read from file, return the total size of the data part
   * in bytes.
   * @return
   */
  public int getTotalDataSize() {
    return totalDataSize;
  }

  /**
   * A class used to iterate over the data entries in the R-tree
   */
  public class Entry {
    public int id;
    public double x1, y1, x2, y2;

    protected Entry() {}

    @Override
    public String toString() {
      return String.format("Entry #%d (%f, %f, %f, %f)", id, x1, y1, x2, y2);
    }

    public Object getObject() throws IOException {
      if (deser == null)
        return null;
      in.seek(treeStartOffset + entryOffsets[id]);
      return deser.deserialize(in, entryOffsets[id+1] - entryOffsets[id]);
    }
  }

  /**
   * An iterable and iterator that traverses all data entries in the tree.
   */
  protected class EntryIterator implements Iterable<Entry>, Iterator<Entry> {
    private int iNextEntry = 0;
    private final Entry entry = new Entry();

    protected EntryIterator() {}

    @Override
    public Iterator<Entry> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return iNextEntry < RTreeGuttman.this.numEntries;
    }

    @Override
    public Entry next() {
      entry.id = iNextEntry;
      entry.x1 = x1s[iNextEntry];
      entry.y1 = y1s[iNextEntry];
      entry.x2 = x2s[iNextEntry];
      entry.y2 = y2s[iNextEntry];
      iNextEntry++;
      return entry;
    }

    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }

  /**
   * Returns an iterable on all data entries in the tree.
   * @return
   */
  public Iterable<Entry> entrySet() {
    return new EntryIterator();
  }

  /**
   * An iterator for range query search results
   */
  protected class SearchIterator implements Iterable<Entry>, Iterator<Entry> {
    /**The list of nodes yet to be searched*/
    private IntArray nodesToSearch;

    /**The ID of the entry to return on the next call*/
    private int iNextEntry;

    /**The object used to return all search results*/
    private Entry entry;

    /**The search range*/
    private double x1, y1, x2, y2;

    protected SearchIterator(double x1, double y1, double x2, double y2) {
      this.x1 = x1; this.y1 = y1; this.x2 = x2; this.y2 = y2;
      searchFirst();
    }

    /**
     * Search for the first element in the result
     */
    protected void searchFirst() {
      nodesToSearch = new IntArray();
      nodesToSearch.add(root);
      entry = new Entry();
      while (!nodesToSearch.isEmpty()) {
        // We keep the top of the stack for the subsequent next calls
        int iNodeToSearch = nodesToSearch.peek();
        if (isLeaf.get(iNodeToSearch)) {
          for (iNextEntry = 0; iNextEntry < Node_size(iNodeToSearch); iNextEntry++) {
            // Found a matching element in a leaf node
            if (Object_overlaps(children.get(iNodeToSearch).get(iNextEntry), x1, y1, x2, y2))
              return;
          }
        } else {
          // Found a matching non-leaf node, visit its children
          nodesToSearch.pop(); // No longer needed
          for (int iChild : children.get(iNodeToSearch)) {
            if (Object_overlaps(iChild, x1, y1, x2, y2))
              nodesToSearch.add(iChild);
          }
        }
      }
      iNextEntry = -1;
    }

    protected void prefetchNext() {
      int iNodeToSearch = nodesToSearch.peek();
      while (++iNextEntry < Node_size(iNodeToSearch)) {
        if (Object_overlaps(children.get(iNodeToSearch).get(iNextEntry), x1, y1, x2, y2))
          return;
      }
      // Done with the current leaf node. Continue searching for the next leaf
      nodesToSearch.pop();
      while (!nodesToSearch.isEmpty()) {
        iNodeToSearch = nodesToSearch.peek();
        if (isLeaf.get(iNodeToSearch)) {
          for (iNextEntry = 0; iNextEntry < Node_size(iNodeToSearch); iNextEntry++) {
            // Found a matching element in a leaf node
            if (Object_overlaps(children.get(iNodeToSearch).get(iNextEntry), x1, y1, x2, y2))
              return;
          }
        } else {
          // Found a matching non-leaf node, visit its children
          nodesToSearch.pop(); // No longer needed
          for (int iChild : children.get(iNodeToSearch)) {
            if (Object_overlaps(iChild, x1, y1, x2, y2))
              nodesToSearch.add(iChild);
          }
        }
      }
      iNextEntry = -1; // No more entries to search
    }

    @Override
    public Iterator<Entry> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return iNextEntry != -1;
    }

    @Override
    public Entry next() {
      int iEntry = children.get(nodesToSearch.peek()).get(iNextEntry);
      entry.id = iEntry;
      entry.x1 = x1s[iEntry];
      entry.y1 = y1s[iEntry];
      entry.x2 = x2s[iEntry];
      entry.y2 = y2s[iEntry];
      prefetchNext();
      return entry;
    }

    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }

  /**
   * A class that holds information about one node in the tree.
   */
  public static class Node {
    /**The internal ID of the node*/
    public int id;

    /**Whether this is a leaf node or not*/
    public boolean isLeaf;

    /**The boundaries of the node*/
    public double x1, y1, x2, y2;

    protected Node(){}
  }

  protected class NodeIterable implements Iterable<Node>, Iterator<Node> {
    /**The ID of the next node to be returned*/
    protected int iNextNode;

    /**Current node pointed by the iterator*/
    protected Node currentNode;

    protected NodeIterable() {
      currentNode = new Node();
      iNextNode = numEntries - 1;
      prefetchNext();
    }

    protected void prefetchNext() {
      if (iNextNode >= numEntries + numNodes)
        return;
      iNextNode++;
    }

    @Override
    public Iterator<Node> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return iNextNode < numEntries + numNodes;
    }

    @Override
    public Node next() {
      currentNode.x1 = x1s[iNextNode];
      currentNode.y1 = y1s[iNextNode];
      currentNode.x2 = x2s[iNextNode];
      currentNode.y2 = y2s[iNextNode];
      currentNode.isLeaf = isLeaf.get(iNextNode);
      currentNode.id = iNextNode;
      prefetchNext();
      return currentNode;
    }

    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }

  protected class LeafNodeIterable extends NodeIterable {
    protected void prefetchNext() {
      if (iNextNode >= numEntries + numNodes)
        return;
      do {
        iNextNode++;
      } while (iNextNode < numEntries + numNodes && !isLeaf.get(iNextNode));
    }
  }

  /**
   * An interface for serializing objects given their entry number
   */
  public interface Serializer {
    int serialize(DataOutput out, int iObject) throws IOException;
  }

  public interface Deserializer<O> {
    O deserialize(DataInput in, int length) throws IOException;
  }

  /**
   * Serializes the tree and its data entries to an output stream. Notice that
   * this is not supposed to be used as an external tree format where you can insert
   * and delete entries. Rather, it is like a static copy of the tree where you
   * can search or load back in memory. The format of the tree on disk is as
   * described below.
   * <ul>
   *   <li>
   *     Data Entries: First, all data entries are written in an order that is consistent
   *   with the R-tree structure. This order will guarantee that all data entries
   *   under any node (from the root to leaves) will be adjacent in that order.
   *   </li>
   *   <li>
   *     Tree structure: This part contains the structure of the tree represented
   *   by its nodes. The nodes are stored in a level order traversal. This guarantees
   *   that the root will always be the first node and that all siblings will be
   *   stored consecutively. Each node contains the following information:
   *   (1) (n) Number of children as a 32-bit integer,
   *   (2) n Pairs of (child offset, MBR=(x1, y1, x2, y2). The child offset is
   *   the offset of the beginning of the child data (node or data entry) in the
   *   tree where 0 is the offset of the first data entry.
   *   </li>
   *   <li>
   *     Tree footer: This section contains some meta data about the tree as
   *     follows. All integers are 32-bits.
   *     (1) MBR of the root as (x1, y1, x2, y2),
   *     (2) Number of data entries,
   *     (3) Number of non-leaf nodes,
   *     (4) Number of leaf nodes,
   *     (5) Tree structure offset: offset of the beginning of the tree structure section
   *     (6) Footer offset: offset of the beginning of the footer as a 32-bit integer.
   *     (7) Tree size: Total tree size in bytes including data+structure+footer
   *   </li>
   *
   * </ul>
   * @param out
   * @throws IOException
   */
  public void write(DataOutput out, Serializer ser) throws IOException {
    // Tree data: write the data entries in the tree order
    // Since we write the data first, we will have to traverse the tree twice
    // first time to visit and write the data entries in the tree order,
    // and second time to visit and write the tree nodes in the tree order.
    Deque<Integer> nodesToVisit = new ArrayDeque<Integer>();
    nodesToVisit.add(root);
    int[] objectOffsets = new int[numOfDataEntries() + numOfNodes()];
    // Keep track of the offset of each data object from the beginning of the
    // data section
    int dataOffset = 0;
    // Keep track of the offset of each node from the beginning of the tree
    // structure section
    int nodeOffset = 0;
    while (!nodesToVisit.isEmpty()) {
      int node = nodesToVisit.removeFirst();
      // The node is supposed to be written in this order.
      // Measure its offset and accumulate its size
      objectOffsets[node] = nodeOffset;
      nodeOffset += 4 + (4 + 8 * 4) * Node_size(node);

      if (isLeaf.get(node)) {
        // Leaf node, write the data entries in order
        for (int child : children.get(node)) {
          objectOffsets[child] = dataOffset;
          if (ser != null)
            dataOffset += ser.serialize(out, child);
        }
      } else {
        // Internal node, recursively traverse its children
        for (int child : children.get(node))
          nodesToVisit.addLast(child);
      }
    }
    // Update node offsets as they are written after the data entries
    for (int i = 0; i < numNodes; i++)
      objectOffsets[i + numEntries] += dataOffset;

    // Tree structure: Write the nodes in tree order
    nodesToVisit.add(root);
    while (!nodesToVisit.isEmpty()) {
      int node = nodesToVisit.removeFirst();
      // (1) Number of children
      out.writeInt(Node_size(node));
      for (int child : children.get(node)) {
        // (2) Write the offset of the child
        out.writeInt(objectOffsets[child]);
        // (3) Write the MBR of each child
        out.writeDouble(x1s[child]);
        out.writeDouble(y1s[child]);
        out.writeDouble(x2s[child]);
        out.writeDouble(y2s[child]);
      }
      // If node is internal, add its children to the nodes to be visited
      if (!isLeaf.get(node)) {
        for (int child : children.get(node))
          nodesToVisit.addLast(child);
      }
    }

    // Tree footer
    int footerOffset = dataOffset + nodeOffset;
    // (1) MBR of the root
    out.writeDouble(x1s[root]);
    out.writeDouble(y1s[root]);
    out.writeDouble(x2s[root]);
    out.writeDouble(y2s[root]);
    // (2) Number of data entries
    out.writeInt(numOfDataEntries());
    // (3) Number of non-leaf nodes
    out.writeInt((int) (numOfNodes() - isLeaf.countOnes()));
    // (4) Number of leaf nodes
    out.writeInt((int) isLeaf.countOnes());
    // (5) Offset of the tree structure section
    out.writeInt(dataOffset);
    // (6) Offset of the footer
    out.writeInt(footerOffset);
    // (7) Size of the entire tree
    int footerSize = 4 * 8 + 6 * 4;
    out.writeInt(footerOffset + footerSize);
  }

  /**
   * Read an R-tree stored using the method {@link #write(DataOutput, Serializer)}
   * @param in
   * @param length
   * @throws IOException
   */
  public void readFields(FSDataInputStream in, long length, Deserializer<?> deser) throws IOException {
    this.in = in;
    this.deser = deser;
    this.treeStartOffset = in.getPos();
    in.seek(treeStartOffset + length - 8);
    int footerOffset = in.readInt();
    in.seek(treeStartOffset + footerOffset);
    double rootx1 = in.readDouble();
    double rooty1 = in.readDouble();
    double rootx2 = in.readDouble();
    double rooty2 = in.readDouble();
    this.numEntries = in.readInt();
    int numNonLeaves = in.readInt();
    int numLeaves = in.readInt();
    this.numNodes = numNonLeaves + numLeaves;
    int treeStructureOffset = in.readInt();
    this.totalDataSize = treeStructureOffset;

    // Initialize the data structures to store objects
    this.x1s = new double[numEntries + numNodes];
    this.y1s = new double[numEntries + numNodes];
    this.x2s = new double[numEntries + numNodes];
    this.y2s = new double[numEntries + numNodes];
    this.isLeaf = new BitArray(numEntries + numNodes);
    this.children = new ArrayList<IntArray>(numEntries + numNodes);
    for (int i = 0; i < numEntries + numNodes; i++)
      this.children.add(null);

    // Read the tree structure and keep it all in memory
    // First, scan the nodes once to map node offsets to IDs
    // Map the offset of some nodes to their index in the node list
    Map<Integer, Integer> nodeOffsetToIndex = new HashMap<Integer, Integer>();
    in.seek(treeStartOffset + treeStructureOffset);
    int nodeID = numEntries;
    this.root = nodeID; // First node is always the root
    // Store root MBR
    x1s[root] = rootx1;
    y1s[root] = rooty1;
    x2s[root] = rootx2;
    y2s[root] = rooty2;

    while (nodeID < this.numNodes + this.numEntries) {
      int nodeOffset = (int) (in.getPos() - treeStartOffset);
      nodeOffsetToIndex.put(nodeOffset, nodeID);
      int nodeSize = in.readInt();
      in.skipBytes(nodeSize * (4 + 8 * 4)); // Skip offset and MBR
      nodeID++;
    }
    // Second, read nodes data and store them in the object
    in.seek(treeStartOffset + treeStructureOffset);
    nodeID = numEntries;
    int entryID = 0; // Number entries starting at zero
    entryOffsets = new int[numEntries+1];
    while (nodeID < this.numNodes + numEntries) {
      boolean leafNode = nodeID >= (numEntries + numNonLeaves);
      isLeaf.set(nodeID, leafNode);
      // (1) Node size
      int nodeSize = in.readInt();
      // (2) Offset of the first child
      IntArray nodeChildren = new IntArray();
      children.set(nodeID, nodeChildren);
      for (int i = 0; i < nodeSize; i++) {
        int childOffset = in.readInt();
        int childID = leafNode ? entryID++ : nodeOffsetToIndex.get(childOffset);
        if (leafNode)
          entryOffsets[childID] = childOffset;
        nodeChildren.add(childID);
        // (3) Child MBR
        x1s[childID] = in.readDouble();
        y1s[childID] = in.readDouble();
        x2s[childID] = in.readDouble();
        y2s[childID] = in.readDouble();
      }
      nodeID++;
    }
    entryOffsets[entryID] = treeStructureOffset;
  }

  public void close() throws IOException {
    if (in != null)
      in.close();
  }

  /**
   * Writes all nodes of the tree in a WKT format to be visualized in QGIS.
   * @param out
   */
  public void toWKT(PrintStream out) {
    for (Node node : new NodeIterable()) {
      out.printf("%d\tPOLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))\n",
          node.id,
          node.x1, node.y1,
          node.x1, node.y2,
          node.x2, node.y2,
          node.x2, node.y1,
          node.x1, node.y1
      );
    }
  }
}
