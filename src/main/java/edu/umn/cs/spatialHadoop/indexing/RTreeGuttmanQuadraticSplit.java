package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * R-Tree with quadratic split
 */
public class RTreeGuttmanQuadraticSplit extends RTreeGuttman {

  /**
   * Construct a new empty R-tree with the given parameters.
   *
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity  - Maximum capacity of a node
   */
  public RTreeGuttmanQuadraticSplit(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
  }



  /**
   * Split an overflow leaf node into two using the Quadratic Split method described
   * in Guttman'84 page 52.
   * @param iNode the index of the node to split
   * @param minSplitSize Minimum size of each split, typically, {@link #minCapacity}
   * @return
   */
  @Override protected int split(int iNode, int minSplitSize) {
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
}
