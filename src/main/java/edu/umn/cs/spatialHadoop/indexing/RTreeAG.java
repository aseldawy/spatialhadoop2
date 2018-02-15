package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.IntArray;

import java.util.ArrayList;
import java.util.List;

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
public class RTreeAG {

  /**
   * x-coordinates of all points inserted into the tree.
   */
  private double[] xs;
  /**
   * y-coordinates of all points inserted into the tree.
   */
  private double[] ys;

  /**
   * Maximum capacity of a node
   */
  private final int maxCapcity;

  /**
   * Minimum capacity of a node.
   */
  private final int minCapacity;

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

    static Node createLeaf(int iEntry, double x, double y) {
      return new Node().reset(iEntry, x, y);
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

    public Node reset(int iEntry, double x, double y) {
      this.children = new IntArray();
      this.children.add(iEntry);
      this.set(x, y, Math.nextUp(x), Math.nextUp(y));
      this.leaf = true;
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
     * @param x
     * @param y
     * @return
     */
    public double expansion(double x, double y) {
      double newWidth = this.getWidth();
      double newHeight = this.getHeight();
      if (x < this.x1)
        newWidth += (this.x1 - x);
      else if (x > this.x2)
        newWidth += (x - this.x2);
      if (y < this.y1)
        newHeight += (this.y1 - y);
      else if (y > this.y2)
        newHeight += (this.y2 - y);

      return newWidth * newHeight - getWidth() * getHeight();
    }

    private void addEntry(int iEntry, double x, double y) {
      this.children.add(iEntry);
      // Expand the MBR to enclose the given point
      this.expand(x, y);
    }

    private void addChildNode(int iNode, Rectangle mbr) {
      this.children.add(iNode);
      this.expand(mbr);
    }
  }

  /**
   * All nodes in the tree.
   */
  protected List<Node> nodes;
  /**The index of the root in the list of nodes*/
  protected int root;

  /**
   * Construct a new RTree that contains points.
   * @param xs - x-coordinates for the points
   * @param ys - y-coordinates for the points
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity - Maximum capacity of a node
   */
  public RTreeAG(double[] xs, double[] ys, int minCapacity, int maxCapcity) {
    this.xs = xs;
    this.ys = ys;
    this.maxCapcity = maxCapcity;
    this.minCapacity = minCapacity;
    nodes = new ArrayList<Node>();
    nodes.add(Node.createLeaf(0, xs[0], ys[0]));

    for (int i = 1; i < xs.length; i++) {
      insert(i);
    }
  }

  /**
   * Inserts the given point to the tree.
   * @param iPoint - The index of the point in the array of points
   */
  private void insert(int iPoint) {
    double x = xs[iPoint];
    double y = ys[iPoint];
    // The path from the root to the newly inserted record. Used for splitting.
    IntArray path = new IntArray();
    int iNode = root;
    path.add(iNode);
    Node leafNode;
    while (!(leafNode = nodes.get(iNode)).leaf) {
      double minExpansion = Double.POSITIVE_INFINITY;
      int iChildWithMinExpansion = 0;
      // Node is not leaf. Choose a child node
      for (int iChild : leafNode.children) {
        Node child = nodes.get(iChild);
        double expansion = child.expansion(x, y);
        if (expansion < minExpansion) {
          // Choose the child with the minimum expansion
          minExpansion = expansion;
          iChildWithMinExpansion = iChild;
        } else if (expansion == minExpansion) {
          // Resolve ties by choosing the entry with the rectangle of smallest area
          if (child.area() < nodes.get(iChildWithMinExpansion).area())
            iChildWithMinExpansion = iChild;
        }
      }
      iNode = iChildWithMinExpansion;
      path.add(iNode);
    }

    // Now we have a child node. Insert the current element to it and split
    // if necessary
    leafNode.addEntry(iPoint, x, y);
    int iNewNode = -1;
    if (leafNode.size() >= maxCapcity) {
      // Node full. Split into two
      iNewNode = quadraticSplitLeaf(leafNode);
    }
    // AdjustTree. Ascend from the leaf node L
    while (!path.isEmpty()) {
      iNode = path.pop();
      if (path.isEmpty()) {
        // The node is the root (no parent)
        if (iNewNode != -1) {
          // If the root is split, create a new root
          Node newRoot = Node.createNonLeaf(iNode, iNewNode, nodes.get(iNode), nodes.get(iNewNode));
          root = nodes.size();
          nodes.add(newRoot);
        }
        // If N is the root with no partner NN, stop.
      } else {
        Node parent = nodes.get(path.peek());
        // Adjust covering rectangle in parent entry
        Node node = nodes.get(iNode);
        parent.expand(node);
        if (iNewNode != -1) {
          // If N has a partner NN resulting from an earlier split,
          // create a new entry ENN and add to the parent if there is room.
          // Add Enn to P if there is room
          parent.addChildNode(iNewNode, nodes.get(iNewNode));
          iNewNode = -1;
          if (parent.size() >= maxCapcity) {
            // TODO Otherwise, invoke SplitNode to produce P and PP containing
            // ENN and all P's old entries
          }
        }
      }
    }

  }

  /**
   * Split an overflow leaf node into two using the Quadratic Split method described
   * in Guttman'86 page 52.
   * @param oldNode
   * @return
   */
  private int quadraticSplitLeaf(Node oldNode) {
    // Pick seeds
    // Indexes of the objects to be picked as seeds in the arrays xs and ys
    // Select two entries to be the first elements of the groups
    int seed1 = -1, seed2 = -1;
    double maxD = Double.NEGATIVE_INFINITY;
    for (int i1 = 0; i1 < oldNode.size(); i1++) {
      int entry1 = oldNode.children.get(i1);
      for (int i2 = i1 + 1; i2 < oldNode.size(); i2++) {
        int entry2 = oldNode.children.get(i2);
        // For each pair of entries, compose a rectangle J including both of
        // them and calculate d = area(J) - area(entry1) - area(entry2)
        // Since both entries are points, d = area(J)
        // Choose the most wasteful pair. Choose the pair with the largest d
        double d = Math.abs((xs[entry1] - xs[entry2]) * (ys[entry1] - ys[entry2]));
        if (d > maxD) {
          maxD = d;
          seed1 = entry1;
          seed2 = entry2;
        }
      }
    }

    // After picking the seeds, we will start picking next elements one-by-one
    IntArray nonAssignedEntries = oldNode.children;
    oldNode.reset(seed1, xs[seed1], ys[seed1]);
    Node newNode = Node.createLeaf(seed2, xs[seed2], ys[seed2]);
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
          group1.addEntry(iEntry, xs[iEntry], ys[iEntry]);
        nonAssignedEntries.clear();
      } else if (nonAssignedEntries.size() + group2.size() == minCapacity) {
        // Assign all the rest to newNode
        for (int iEntry : nonAssignedEntries)
          group2.addEntry(iEntry, xs[iEntry], ys[iEntry]);
        nonAssignedEntries.clear();
      } else {
        // Invoke the algorithm  PickNext to choose the next entry to assign.
        int nextEntry = -1;
        double maxDiff = Double.NEGATIVE_INFINITY;
        for (int nonAssignedEntry : nonAssignedEntries) {
          double d1 = group1.expansion(xs[nonAssignedEntry], ys[nonAssignedEntry]);
          double d2 = group2.expansion(xs[nonAssignedEntry], ys[nonAssignedEntry]);
          double diff = Math.abs(d1 - d2);
          if (diff > maxDiff) {
            maxDiff = diff;
            nextEntry = nonAssignedEntry;
          }
        }

        // Choose which node to add the next entry to
        double diffExpansion = group1.expansion(xs[nextEntry], ys[nextEntry]) -
            group2.expansion(xs[nextEntry], ys[nextEntry]);
        Node chosenNode;
        // Add it to the group whose covering rectangle will have to be enlarged
        // least to accommodate it
        if (diffExpansion < 0) {
          chosenNode = group1;
        } else if (diffExpansion > 0) {
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
        chosenNode.addEntry(nextEntry, xs[nextEntry], ys[nextEntry]);
        nonAssignedEntries.remove(nextEntry);
      }
    }
    // Add the new node to the list of nodes and return its index
    nodes.add(newNode);
    return nodes.size() - 1;
  }

  /**
   * Total number of objects in the tree.
   * @return
   */
  public int numOfObjects() {
    return xs.length;
  }

  /**
   * Returns number of nodes in the tree.
   * @return
   */
  public int numOfNodes() {
    return nodes.size();
  }

  public int getHeight() {
    if (nodes.isEmpty())
      return 0;
    // Compute the height of the tree by traversing any path from the root
    // to the leaf.
    // Since the tree is balanced, any path would work
    int height = 1;
    int iNode = root;
    while (!nodes.get(iNode).leaf) {
      height++;
      iNode = nodes.get(iNode).children.get(0);
    }
    return height;
  }
}
