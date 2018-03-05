package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.operations.Aggregate;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.util.QuickSort;

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

  /**The coordinates of the center of each node at the time it was created*/
  protected double[] xOBox, yOBox;

  /**
   * Construct a new empty RR*-tree with the given parameters.
   *
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity  - Maximum capacity of a node
   */
  public RRStarTree(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
  }

  @Override
  protected int Node_createNodeWithChildren(boolean leaf, int... iChildren) {
    int nodeID = super.Node_createNodeWithChildren(leaf, iChildren);
    xOBox[nodeID] = (x1s[nodeID] + x2s[nodeID]) / 2;
    yOBox[nodeID] = (y1s[nodeID] + y2s[nodeID]) / 2;
    return nodeID;
  }

  @Override
  protected int Node_split(int nodeID, int separator) {
    int newNodeID = super.Node_split(nodeID, separator);
    xOBox[nodeID] = (x1s[nodeID] + x2s[nodeID]) / 2;
    yOBox[nodeID] = (y1s[nodeID] + y2s[nodeID]) / 2;
    xOBox[newNodeID] = (x1s[newNodeID] + x2s[newNodeID]) / 2;
    yOBox[newNodeID] = (y1s[newNodeID] + y2s[newNodeID]) / 2;
    return newNodeID;
  }

  @Override
  protected void makeRoomForOneMoreObject() {
    super.makeRoomForOneMoreObject();
    if (x1s.length != xOBox.length) {
      double[] newCoords = new double[x1s.length];
      System.arraycopy(xOBox, 0, newCoords, 0, xOBox.length);
      xOBox = newCoords;
      newCoords = new double[y1s.length];
      System.arraycopy(yOBox, 0, newCoords, 0, yOBox.length);
      yOBox = newCoords;
    }
  }

  @Override
  protected void initializeDataEntries(double[] x1, double[] y1, double[] x2, double[] y2) {
    super.initializeDataEntries(x1, y1, x2, y2);
    xOBox = new double[x1s.length];
    yOBox = new double[y1s.length];
  }

  @Override
  protected void initializeDataEntries(double[] xs, double[] ys) {
    super.initializeDataEntries(xs, ys);
    xOBox = new double[x1s.length];
    yOBox = new double[y1s.length];
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
      // This is effectively the same as returning the first child when sorted
      // lexicographically by (volume, perimeter)
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
    // TODO we can speed this step up by precaching delta_perim values
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
    // Try to achieve an overlap optimized choice
    int p = 0;
    for (int iChild = 1; iChild < nodeChildren.size(); iChild++) {
      if (dOvlp(nodeChildren.get(0), object, nodeChildren.get(iChild), AggregateFunction.PERIMETER) > 0)
        p = iChild;
    }
    if (p == 0) {
      // dOvlpPerim = 0 between the first entry and all remaining entries.
      // return the first entry
      return nodeChildren.get(0);
    }
    assert cov.isEmpty();
    int c;
    // If there is an index i with vol(MBB(Ri U object)) = 0
    int iChildWithZeroVolExpansion = -1;
    for (int iChild = 0; iChild < nodeChildren.size(); iChild++) {
      if (Node_volumeExpansion(nodeChildren.get(iChild), object) == 0) {
        iChildWithZeroVolExpansion = iChild;
        break;
      }
    }
    IntArray cand = cov; // reuse the same IntArray for efficiency
    // checkComp will fill in the deltaOverlap array with the computed value
    // for each candidate
    double[] sumDeltaOverlap = new double[nodeChildren.size()];
    if (iChildWithZeroVolExpansion != -1) {
      c = checkComp(0, AggregateFunction.PERIMETER, cand, sumDeltaOverlap, p, object, nodeChildren);
    } else {
      c = checkComp(0, AggregateFunction.VOLUME, cand, sumDeltaOverlap, p, object, nodeChildren);
    }
    if (c != -1) // if (success)
      return nodeChildren.get(c);

    int iMinDeltaOverlap = -1;
    double minDeltaOverlap = Double.POSITIVE_INFINITY;
    for (int i : cand) {
      if (sumDeltaOverlap[i] < minDeltaOverlap) {
        minDeltaOverlap = sumDeltaOverlap[i];
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
  protected int checkComp(int t, AggregateFunction f, IntArray cand, double[] sumDeltaOverlap,
                          int p, int object, IntArray nodeChildren) {
    cand.add(t);
    sumDeltaOverlap[t] = 0; // the accumulation of dOvlp(t, [0, p))
    int c = -1;
    for (int j = 0; j < p; j++) {
      if (j == t)
        continue;
      double ovlpPerimTJ = dOvlp(nodeChildren.get(t), object, nodeChildren.get(j), f);
      if (ovlpPerimTJ != 0 && !cand.contains(j)) {
        sumDeltaOverlap[t] += ovlpPerimTJ;
        c = checkComp(j, f, cand, sumDeltaOverlap, p, object, nodeChildren);
        if (c != -1)
          break;
      }
    }

    if (sumDeltaOverlap[t] == 0) // i.e. delta Ovlp f t, [0, p) = 0
      return t;
    return c;
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

  @Override protected int split(int iNode, int minSplitSize) {
    if (isLeaf.get(iNode))
      return splitLeaf(iNode, minSplitSize);
    else
      return splitNonLeaf(iNode, minSplitSize);
  }

  protected int splitLeaf(int iNode, int minSplitSize) {
    int nodeSize = Node_size(iNode);
    final int[] nodeChildren = children.get(iNode).underlyingArray();
    // ChooseSplitAxis
    // Sort the entries by each axis and compute S, the sum of all margin-values
    // of the different distributions

    // Sort by x1, y1, x2, y2
    RStarTree.MultiIndexedSortable sorter = new RStarTree.MultiIndexedSortable() {
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
    RStarTree.MultiIndexedSortable.Axis bestAxis = null;
    QuickSort quickSort = new QuickSort();
    for (RStarTree.MultiIndexedSortable.Axis sortAttr : RStarTree.MultiIndexedSortable.Axis.values()) {
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

    // Compute the partial terms used to compute wf
    final double s = 0.5;
    final double y1 = Math.exp(-1 / (s * s));
    final double ys = 1 / (1 - y1);

    double nodeCenter, nodeOrigin, nodeLength;
    switch (bestAxis) {
      case X1: case X2:
        // Split is along the x-axis
        nodeCenter = (x1s[iNode] + x2s[iNode]) / 2;
        nodeOrigin = xOBox[iNode];
        nodeLength = (x2s[iNode] - x1s[iNode]);
        break;
      case Y1: case Y2:
        // Split is along the y-axis
        nodeCenter = (y1s[iNode] + y2s[iNode]) / 2;
        nodeOrigin = yOBox[iNode];
        nodeLength = (y2s[iNode] - y1s[iNode]);
        break;
      default:
        throw new RuntimeException("Unknown sort attribute " + bestAxis);
    }
    double asym = 2 * (nodeCenter - nodeOrigin) / nodeLength;
    double mu = (1- 2 * minSplitSize / (maxCapcity + 1)) * asym;
    double sigma = s * (1 + Math.abs(mu));

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

    boolean overlapFreeCandidates = false;
    double minWeight = Double.POSITIVE_INFINITY;
    int chosenK = -1;

    // # of possible splits = current size - 2 * minSplitSize + 1
    int numPossibleSplits = Node_size(iNode) - 2 * minSplitSize + 1;
    for (int k = 1; k <= numPossibleSplits; k++) {
      int separator = minSplitSize + k - 1; // Separator = size of first group
      // Compute wf for this value of k (referred to as i in the RR*-tree paper)
      double xi = 2 * k / (maxCapcity + 1) - 1;
      double gaussianTerm = (xi - mu) / sigma;
      double wf = ys * (Math.exp(-gaussianTerm * gaussianTerm) - y1);

      mbr1.expand(x1s[nodeChildren[separator-1]], y1s[nodeChildren[separator-1]]);
      mbr1.expand(x2s[nodeChildren[separator-1]], y2s[nodeChildren[separator-1]]);

      mbr2.set(minX1[separator], minY1[separator], maxX2[separator], maxY2[separator]);

      Rectangle overlapMBR = mbr1.getIntersection(mbr2);
      double wg;
      if (!overlapFreeCandidates && overlapMBR == null) {
        // First overlap-free candidate to encounter, use it
        overlapFreeCandidates = true;
        chosenK = k;
        wg = mbr1.getWidth() + mbr1.getHeight() + mbr2.getWidth() + mbr2.getHeight();
        minWeight = wg * wf;
      } else if (overlapFreeCandidates && overlapMBR == null) {
        // Not the first overlap-free candidate to encounter.
        // Compute wg as the perimeter and compare it to the minWeight
        wg = mbr1.getWidth() + mbr1.getHeight() + mbr2.getWidth() + mbr2.getHeight();
        double w = wg * wf;
        if (w < minWeight) {
          chosenK = k;
          minWeight = w;
        }
      } else if (!overlapFreeCandidates) {
        // Never encountered an overlap-free candidate, wg is the volume of the overlap
        wg = overlapMBR.getWidth() * overlapMBR.getHeight();
        double w = wg * wf;
        if (w < minWeight) {
          chosenK = k;
          minWeight = w;
        }
      }
    }

    // Split at the chosenK
    int separator = minSplitSize - 1 + chosenK;
    int iNewNode = Node_split(iNode, separator);
    return iNewNode;
  }

  protected int splitNonLeaf(int iNode, int minSplitSize) {
    throw new RuntimeException("Not yet supported");
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
