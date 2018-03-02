package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.Aggregate;
import edu.umn.cs.spatialHadoop.util.IntArray;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

/**
 * An implementation of the RR*-tree as described in the paper below.
 * Norbert Beckmann, Bernhard Seeger,
 * A Revised R*-tree in Comparison with Related Index Structures. SIGMOD 2009: 799-812
 *
 * It makes the following two changes to the original R-tree by Guttman.
 * <ol>
 *   <li>While inserting, it uses a new strategy for selecting the subtree at
 *   each level which takes into account the area increase, perimiter, and
 *   overlap</li>
 *   <li>It uses a new splitting strategy which takes into account the deviation
 *   of the MBR of a node since it was first created.</li>
 * </ol>
 *
 * Notice that this implementation is closes to the original R-tree rather than
 * the R*-tree and this is why it extends directly from the
 * {@link RTreeGuttman} class rather than the {@link RStarTree}.
 */
public class RRStarTree extends RTreeGuttman {
  /**
   * Construct a new empty RR*-tree with the given parameters.
   *
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity  - Maximum capacity of a node
   */
  public RRStarTree(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
  }

  /**
   * Chooses the best subtree to add a new data entry.
   * This function implements the CSRevised algorithm on Page 802 in the paper
   * @param object
   * @param node
   * @return
   */
  @Override protected int chooseSubtree(final int object, int node) {
    // cov, the set of entries that entirely cover the new object
    IntArray cov = new IntArray();
    for (int child : children.get(node)) {
      if (Node_covers(child, object))
        cov.add(child);
    }

    if (!cov.isEmpty()) {
      // There are some nodes that do not need to be expanded to accommodate the object
      // If there are some children with zero volume (area), return the one with the smallest perimeter
      // Otherwise, return the child with the minimum volume (area)
      int bestChild = -1;
      double minVol = Double.POSITIVE_INFINITY;
      double minPerim = Double.POSITIVE_INFINITY;
      for (int iChild : cov) {
        double vol = Node_area(iChild);
        if (vol < minVol) {
          minVol = vol;
          minPerim = Node_perimeter(iChild);
          bestChild = iChild;
        } else if (vol == minVol) {
          // This also covers the case of vol == minVol == 0
          if (Node_perimeter(iChild) < minPerim) {
            minPerim = Node_perimeter(iChild);
            bestChild = iChild;
          }
        }
      }
      return bestChild;
    }

    // A node has to be enlarged to accommodate the object
    // Sort the children of the node in ascending order of their delta_perim
    // For simplicity, we use insertion sort since the node size is small
    children.get(node).insertionSort(new Comparator<Integer>() {
      @Override
      public int compare(Integer child1, Integer child2) {
        double dPerim1 = Node_dPerimeter(child1, object);
        double dPerim2 = Node_dPerimeter(child2, object);
        if (dPerim1 < dPerim2) return -1;
        if (dPerim1 > dPerim2) return +1;
        return 0;
      }
    });

    // If dOvlpPerim = 0 between the first entry and all remaining entries
    // return the first entry
    IntArray nodeChildren = children.get(node);
    boolean dOvlpPerimZero = true;
    for (int iChild = 1; iChild < nodeChildren.size() && dOvlpPerimZero; iChild++) {
      dOvlpPerimZero = dOvlp(nodeChildren.get(0), object,
          nodeChildren.get(iChild), AggregateFunction.PERIMETER) == 0.0;
    }
    if (dOvlpPerimZero)
      return nodeChildren.get(0);

    // Try to achieve an overlap optimized choice
    int p = 0;
    for (int iChild = 1; iChild < nodeChildren.size(); iChild++) {
      if (dOvlp(nodeChildren.get(0), object, nodeChildren.get(iChild), AggregateFunction.PERIMETER) > 0)
        p = iChild;
    }
    IntArray cand = new IntArray();
    int c;
    // If there is an index i with vol(MBB(Ri U object)) = 0
    int iChildWithZeroVolExpansion = -1;
    for (int iChild = 0; iChild < nodeChildren.size(); iChild++) {
      if (Node_expansion(nodeChildren.get(iChild), object) == 0) {
        iChildWithZeroVolExpansion = iChild;
        break;
      }
    }
    if (iChildWithZeroVolExpansion != -1) {
      c = checkComp(0, AggregateFunction.PERIMETER, cand, p, object, nodeChildren);
    } else {
      c = checkComp(0, AggregateFunction.VOLUME, cand, p, object, nodeChildren);
    }
    if (c != -1) // if (success)
      return nodeChildren.get(c);

    int iMinDeltaOverlap = -1;
    double minDeltaOverlap = Double.POSITIVE_INFINITY;
    for (int i : cand) {
      double deltaOverlap = Node_expansion(nodeChildren.get(i), object);
      if (deltaOverlap < minDeltaOverlap) {
        minDeltaOverlap = deltaOverlap;
        iMinDeltaOverlap = i;
      }
    }
    assert iMinDeltaOverlap != -1;
    return nodeChildren.get(iMinDeltaOverlap);
  }

  enum AggregateFunction {PERIMETER, VOLUME};

  /**
   *
   * @param t
   * @param f
   * @param cand
   * @param p
   * @param object
   * @param nodeChildren
   * @return
   */
  protected int checkComp(int t, AggregateFunction f, IntArray cand, int p, int object, IntArray nodeChildren) {
    cand.add(t);
    double sumDOvlpT = 0; // the accumulation of dOvlp(t, [0, p))
    for (int j = 0; j < p; j++) {
      if (j == t)
        continue;
      double ovlpPerimTJ = dOvlp(nodeChildren.get(t), object, nodeChildren.get(j), f);
      sumDOvlpT += ovlpPerimTJ;
      if (ovlpPerimTJ != 0 && !cand.contains(j)) {
        int c = checkComp(j, f, cand, p, object, nodeChildren);
        if (c != -1)
          break;
      }
    }

    if (sumDOvlpT == 0) // i.e. delta Ovlp f t, [0, p) = 0
      return t;
    return -1;
  }

  /**
   * Tests whether the MBR of a node completely covers the MBR of an object
   * @param node
   * @param object
   * @return
   */
  protected boolean Node_covers(int node, int object) {
    return x1s[object] >= x1s[node] && x2s[object] <= x2s[node] &&
        y1s[object] >= y1s[node] && y2s[object] <= y2s[node];
  }

  /**
   * Compute the perimeter of a node (or an object)
   * @param node
   * @return
   */
  protected double Node_perimeter(int node) {
    return (x2s[node] - x1s[node]) + (y2s[node] - y1s[node]);
  }

  /**
   * Computes the difference in the perimeter if the given object is added to the node
   * @param node
   * @param newChild
   * @return
   */
  protected double Node_dPerimeter(int node, int newChild) {
    double widthB4Expansion = x2s[node] - x1s[node];
    double heightB4Expansion = y2s[node] - y1s[node];
    double widthAfterExpansion = Math.max(x2s[node], x2s[newChild]) -
        Math.min(x1s[node], x1s[newChild]);
    double heightAfterExpansion = Math.max(y2s[node], y2s[newChild]) -
        Math.min(y1s[node], y1s[newChild]);

    return (widthAfterExpansion + heightAfterExpansion) -
        (widthB4Expansion + heightB4Expansion);
  }

  /**
   * Computes the increase of the function f of the common overlap between two
   * nodes (t, j) if a new object (iObject) is added to the node t
   * @param nodeT
   * @param object
   * @param nodeJ
   * @return
   */
  protected double dOvlp(int nodeT, int object, int nodeJ, AggregateFunction f) {
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

    return f == AggregateFunction.PERIMETER ?
        (widthTOJ + heightTOJ) - (widthTJ + heightTJ) :
        (widthTOJ * heightTOJ) - (widthTJ * heightTJ);
  }

  enum RTreeType {Guttman, RStar, RRStar};
  public static void main(String[] args) throws IOException {
    //double[][] tweets = readFile("src/test/resources/test2.points");
    //int capacity = 12;
    long t1 = System.currentTimeMillis();
    double[][] tweets = readFile("tweets_1m");
    int capacity = 2000;
    RTreeType type = RTreeType.Guttman;
    RTreeGuttman rtree;
    switch (type) {
      case Guttman : rtree = new RTreeGuttman(capacity/2, capacity); break;
      case RStar: rtree = new RStarTree(capacity/2, capacity); break;
      case RRStar: rtree = new RRStarTree(capacity/2, capacity); break;
      default: throw new RuntimeException("Unknown tree type "+type);
    }
    rtree.initializeFromPoints(tweets[2], tweets[1]);
    long t2 = System.currentTimeMillis();
    for (RTreeGuttman.Node leaf : rtree.getAllLeaves()) {
      System.out.println(new Rectangle(leaf.x1, leaf.y1, leaf.x2, leaf.y2).toWKT());
    }
    System.out.printf("Built %s with %d entries in %f seconds\n",
                type.toString(), rtree.numEntries, (t2-t1)*1E-3);
  }

  /**
   * Read a CSV file that contains one point per line in the format "x,y".
   * The points are returned as a 2D array where the first index indicates the
   * coordinate (0 for x and 1 for y) and the second index indicates the point
   * number.
   * @param fileName
   * @return
   * @throws IOException
   */
  private static double[][] readFile(String fileName) throws IOException {
    FileReader testPointsIn = new FileReader(fileName);
    char[] buffer = new char[(int) new File(fileName).length()];
    testPointsIn.read(buffer);
    testPointsIn.close();

    String[] lines = new String(buffer).split("\\s");
    int numDimensions = lines[0].split(",").length;
    double[][] coords = new double[numDimensions][lines.length];

    for (int iLine = 0; iLine < lines.length; iLine++) {
      String[] parts = lines[iLine].split(",");
      for (int iDim = 0; iDim < parts.length; iDim++)
        coords[iDim][iLine] = Double.parseDouble(parts[iDim]);
    }
    return coords;
  }

}
