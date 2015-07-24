package edu.umn.cs.spatialHadoop.delaunay;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * The divide and conquer Delaunay Triangulation (DT) algorithm as proposed in
 * L. J Guibas and J. Stolfi, "Primitives for the manipulation of general
 * subdivisions and the computation of Voronoi diagrams",
 * ACM Transactions on Graphics, 4(1985), 74-123,
 * and as further illustrated in
 * http://www.geom.uiuc.edu/~samuelp/del_project.html
 * @author Ahmed Eldawy
 *
 */
public class GuibasStolfiDelaunayAlgorithm {
  
  /** The original input set of points */
  Point[] points;

  /** Coordinates of all sites */
  double[] xs, ys;

  /**
   * All neighboring sites. Two neighbor sites have a common edge in the DT
   */
  IntArray[] neighbors;

  /**
   * Stores the final answer that contains the complete DT
   */
  private IntermediateTriangulation finalAnswer;

  /**
   * Use to report progress to avoid mapper or reducer timeout
   */
  private Progressable progress;

  /**
   * A class that stores a set of triangles for part of the sites.
   * @author Ahmed Eldawy
   *
   */
  class IntermediateTriangulation {
    /**
     * The contiguous range of sites stored at this triangulation.
     * The range is inclusive of both site1 and site2
     */
    int site1, site2;
    /**Sites on the convex null of the triangulation*/
    int[] convexHull;

    /**
     * Constructs an empty triangulation to be used for merging
     */
    IntermediateTriangulation() {}
    
    /**
     * Initialize a triangulation with two sites only. The triangulation consists
     * of one line connecting the two sites and no triangles at all.
     * @param s1
     * @param s2
     */
    IntermediateTriangulation(int s1, int s2) {
      site1 = s1;
      site2 = s2;
      neighbors[s1].add(s2);
      neighbors[s2].add(s1);
      convexHull = new int[] {s1, s2};
    }
    
    /**
     * Initialize a triangulation with three sites. The trianglation consists
     * of a single triangles connecting the three points.
     * @param s1
     * @param s2
     * @param s3
     */
    IntermediateTriangulation(int s1, int s2, int s3) {
      site1 = s1;
      site2 = s3;
      neighbors[s1].add(s2); neighbors[s1].add(s3);
      neighbors[s2].add(s1); neighbors[s2].add(s3);
      neighbors[s3].add(s1); neighbors[s3].add(s2);
      convexHull = new int[] {s1, s2, s3};
    }

    /**
     * Create an intermediate triangulation out of a triangulation created
     * somewhere else (may be another machine). It stores all the edges in the
     * neighbors array and adjusts the node IDs in edges to match their new
     * position in the {@link GuibasStolfiDelaunayAlgorithm#points} array
     * 
     * @param t
     *          The triangulation that needs to be added
     * @param pointShift
     *          How much shift should be added to each edge to adjust it to the
     *          new points array
     */
    public IntermediateTriangulation(Triangulation t, int pointShift) {
      // Assume that points have already been copied
      this.site1 = pointShift;
      this.site2 = pointShift + t.sites.length - 1;
      
      // Calculate the convex hull
      int[] thisPoints = new int[t.sites.length];
      for (int i = 0; i < thisPoints.length; i++)
        thisPoints[i] = i + pointShift;
      this.convexHull = convexHull(thisPoints);
      
      // Copy all edges to the neighbors list
      for (int i = 0; i < t.edgeStarts.length; i++) {
        int adjustedStart = t.edgeStarts[i] + pointShift;
        int adjustedEnd = t.edgeEnds[i] + pointShift;
        neighbors[adjustedStart].add(adjustedEnd);
        neighbors[adjustedEnd].add(adjustedStart);
      }
    }
  }

  /**
   * Constructs a triangulation that merges two existing triangulations.
   * @param L
   * @param R
   */
  public <P extends Point> GuibasStolfiDelaunayAlgorithm(P[] points, Progressable progress) {
    this.progress = progress;
    this.points = new Point[points.length];
    System.arraycopy(points, 0, this.points, 0, points.length);
    Arrays.sort(this.points, new Comparator<Point>() {
      @Override
      public int compare(Point o1, Point o2) {
        double dx = o1.x - o2.x;
        if (dx < 0) return -1;
        if (dx > 0) return 1;
        double dy = o1.y - o2.y;
        if (dy < 0) return -1;
        if (dy > 0) return 1;
        return 0;
      }
    });
    this.xs = new double[this.points.length];
    this.ys = new double[this.points.length];
    this.neighbors = new IntArray[this.points.length];
    for (int i = 0; i < this.points.length; i++) {
      xs[i] = this.points[i].x;
      ys[i] = this.points[i].y;
      neighbors[i] = new IntArray();
    }
    
    // Compute the answer
    IntermediateTriangulation[] triangulations = new IntermediateTriangulation[points.length / 3 + (points.length % 3 == 0 ? 0 : 1)];
    // Compute the trivial Delaunay triangles of every three consecutive points
    int i, t=0;
    for (i = 0; i < points.length - 4; i += 3) {
      // Compute DT for three points
      triangulations[t++] =  new IntermediateTriangulation(i, i+1, i+2);
    }
    if (points.length - i == 4) {
      // Compute DT for every two points
       triangulations[t++] = new IntermediateTriangulation(i, i+1);
       triangulations[t++] = new IntermediateTriangulation(i+2, i+3);
    } else if (points.length - i == 3) {
      // Compute for three points
      triangulations[t++] = new IntermediateTriangulation(i, i+1, i+2);
    } else if (points.length - i == 2) {
      // Two points, connect with a line
      triangulations[t++] = new IntermediateTriangulation(i, i+1);
    } else {
      throw new RuntimeException("Cannot happen");
    }
    
    if (progress != null)
      progress.progress();
    this.finalAnswer = mergeAllTriangulations(triangulations);
  }
  
  /**
   * Compute the DT by merging existing triangulations created at different
   * machines
   * @param ts
   * @param progress
   */
  public GuibasStolfiDelaunayAlgorithm(Triangulation[] ts, Progressable progress) {
    this.progress = progress;
    // Copy all triangulations
    int totalPointCount = 0;
    for (Triangulation t : ts)
      totalPointCount += t.sites.length;
    
    this.points = new Point[totalPointCount];
    // Initialize xs, ys and neighbors array
    this.xs = new double[totalPointCount];
    this.ys = new double[totalPointCount];
    this.neighbors = new IntArray[totalPointCount];
    for (int i = 0; i < this.neighbors.length; i++)
      this.neighbors[i] = new IntArray();
    
    IntermediateTriangulation[] triangulations = new IntermediateTriangulation[ts.length];
    int currentPointsCount = 0;
    for (int it = 0; it < ts.length; it++) {
      Triangulation t = ts[it];
      // Copy sites from that triangulation
      System.arraycopy(t.sites, 0, this.points, currentPointsCount, t.sites.length);
      
      // Set xs and ys for all points
      for (int i = currentPointsCount; i < currentPointsCount + t.sites.length; i++) {
        this.xs[i] = points[i].x;
        this.ys[i] = points[i].y;
      }
      
      // Create a corresponding partial answer
      triangulations[it] = new IntermediateTriangulation(t, currentPointsCount);
      
      currentPointsCount += t.sites.length;
    }

    
    if (progress != null)
      progress.progress();
    this.finalAnswer = mergeAllTriangulations(triangulations);
  }
  
  /**
   * Merge two adjacent triangulations into one
   * @param L
   * @param R
   * @return
   */
  protected IntermediateTriangulation merge(IntermediateTriangulation L, IntermediateTriangulation R) {
    IntermediateTriangulation merged = new IntermediateTriangulation();
    // Compute the convex hull of the result
    int[] bothHulls = new int[L.convexHull.length + R.convexHull.length];
    System.arraycopy(L.convexHull, 0, bothHulls, 0, L.convexHull.length);
    System.arraycopy(R.convexHull, 0, bothHulls, L.convexHull.length, R.convexHull.length);
    merged.convexHull = convexHull(bothHulls);
    
    // Find the base LR-edge (lowest edge of the convex hull that crosses from L to R)
    int[] baseEdge = findBaseEdge(merged.convexHull, L, R);
    int baseL = baseEdge[0];
    int baseR = baseEdge[1];
  
    // Add the first base edge
    neighbors[baseL].add(baseR);
    neighbors[baseR].add(baseL);
    // Trace the base LR edge up to the top
    boolean finished = false;
    do {
      // Search for the potential candidate on the right
      double anglePotential = -1, angleNextPotential = -1;
      int potentialCandidate = -1, nextPotentialCandidate = -1;
      for (int rNeighbor : neighbors[baseR]) {
        if (rNeighbor >= R.site1 && rNeighbor <= R.site2) {
          // Check this RR edge
          double cwAngle = calculateCWAngle(baseL, baseR, rNeighbor);
          if (potentialCandidate == -1 || cwAngle < anglePotential) {
            // Found a new potential candidate
            angleNextPotential = anglePotential;
            nextPotentialCandidate = potentialCandidate;
            anglePotential = cwAngle;
            potentialCandidate = rNeighbor;
          } else if (nextPotentialCandidate == -1 || cwAngle < angleNextPotential) {
            angleNextPotential = cwAngle;
            nextPotentialCandidate = rNeighbor;
          }
        }
      }
      int rCandidate = -1;
      if (anglePotential < Math.PI) {
        if (nextPotentialCandidate != -1) {
          // Check if the circumcircle of the base edge with the potential
          // candidate contains the next potential candidate
          Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
          double dx = circleCenter.x - xs[nextPotentialCandidate];
          double dy = circleCenter.y - ys[nextPotentialCandidate];
          double d1 = dx * dx + dy * dy;
          dx = circleCenter.x - xs[potentialCandidate];
          dy = circleCenter.y - ys[potentialCandidate];
          double d2 = dx * dx + dy * dy;
          if (d1 < d2) {
            // Delete the RR edge between baseR and rPotentialCandidate and restart
            neighbors[baseR].remove(potentialCandidate);
            neighbors[potentialCandidate].remove(baseR);
            continue;
          } else {
            rCandidate = potentialCandidate;
          }
        } else {
          rCandidate = potentialCandidate;
        }
      }
      
      // Search for the potential candidate on the left
      anglePotential = -1; angleNextPotential = -1;
      potentialCandidate = -1; nextPotentialCandidate = -1;
      for (int lNeighbor : neighbors[baseL]) {
        if (lNeighbor >= L.site1 && lNeighbor <= L.site2) {
          // Check this LL edge
          double ccwAngle = Math.PI * 2 - calculateCWAngle(baseR, baseL, lNeighbor);
          if (potentialCandidate == -1 || ccwAngle < anglePotential) {
            // Found a new potential candidate
            angleNextPotential = anglePotential;
            nextPotentialCandidate = potentialCandidate;
            anglePotential = ccwAngle;
            potentialCandidate = lNeighbor;
          } else if (nextPotentialCandidate == -1 || ccwAngle < angleNextPotential) {
            angleNextPotential = ccwAngle;
            nextPotentialCandidate = lNeighbor;
          }
        }
      }
      int lCandidate = -1;
      if (anglePotential < Math.PI) {
        if (nextPotentialCandidate != -1) {
          // Check if the circumcircle of the base edge with the potential
          // candidate contains the next potential candidate
          Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
          double dx = circleCenter.x - xs[nextPotentialCandidate];
          double dy = circleCenter.y - ys[nextPotentialCandidate];
          double d1 = dx * dx + dy * dy;
          dx = circleCenter.x - xs[potentialCandidate];
          dy = circleCenter.y - ys[potentialCandidate];
          double d2 = dx * dx + dy * dy;
          if (d1 < d2) {
            // Delete the LL edge between baseR and rPotentialCandidate and restart
            neighbors[baseL].remove(potentialCandidate);
            neighbors[potentialCandidate].remove(baseL);
            continue;
          } else {
            lCandidate = potentialCandidate;
          }
        } else {
          lCandidate = potentialCandidate;
        }
      }
      
      // Choose the right candidate
      if (lCandidate != -1 && rCandidate != -1) {
        // Two candidates, choose the correct one
        Point circumCircleL = calculateCircumCircleCenter(lCandidate, baseL, baseR);
        double dx = circumCircleL.x - xs[lCandidate];
        double dy = circumCircleL.y - ys[lCandidate];
        double lCandidateDistance = dx * dx + dy * dy;
        dx = circumCircleL.x - xs[rCandidate];
        dy = circumCircleL.y - ys[rCandidate];
        double rCandidateDistance = dx * dx + dy * dy;
        if (lCandidateDistance < rCandidateDistance) {
          // rCandidate is outside the circumcircle, lCandidate is correct
          rCandidate = -1;
        } else {
          // rCandidate is inside the circumcircle, lCandidate is incorrect
          lCandidate = -1;
        }
      }
      
      if (lCandidate != -1) {
        // Left candidate has been chosen
        // Make lPotentialCandidate and baseR the new base line
        baseL = lCandidate;
        // Add the new base edge
        neighbors[baseL].add(baseR);
        neighbors[baseR].add(baseL);
      } else if (rCandidate != -1) {
        // Right candidate has been chosen
        // Make baseL and rPotentialCandidate the new base line
        baseR = rCandidate;
        // Add the new base edge
        neighbors[baseL].add(baseR);
        neighbors[baseR].add(baseL);
      } else {
        // No candidates, merge finished
        finished = true;
      }
    } while (!finished);
    
    // Merge sites of both L and R
    merged.site1 = L.site1;
    merged.site2 = R.site2;
    return merged;
  }

  /**
   * Computes the base edge that is used to merge two triangulations.
   * @param mergedConvexHull
   * @param L
   * @param R
   * @return
   */
  private int[] findBaseEdge(int[] mergedConvexHull, IntermediateTriangulation L,
      IntermediateTriangulation R) {
    int base1L = -1, base1R = -1;
    int base2L = -1, base2R = -1;
    for (int i = 0; i < mergedConvexHull.length; i++) {
      int p1 = mergedConvexHull[i];
      int p2 = i == mergedConvexHull.length - 1 ? mergedConvexHull[0] : mergedConvexHull[i+1];
      if (inArray(L.convexHull, p1) && inArray(R.convexHull, p2)) {
        // Found an LR edge, store it
        if (base1L == -1) {
          base1L = p1;
          base1R = p2;
        } else {
          base2L = p1;
          base2R = p2;
        }
      } else if (inArray(L.convexHull, p2) && inArray(R.convexHull, p1)) {
        if (base1L == -1) {
          base1L = p2;
          base1R = p1;
        } else {
          base2L = p2;
          base2R = p1;
        }
      }
    }
    
    // Choose the right LR edge. The one which makes an angle [0, 180] with
    // each point in both partitions
    double cwAngle = calculateCWAngle(base1L, base1R, base2R);
    
    return cwAngle < Math.PI * 2 ? new int[] {base1L, base1R} :
      new int[] {base2L, base2R};
  }

  /**
   * Merge all triangulations into one
   * @param triangulations
   * @return 
   */
  protected IntermediateTriangulation mergeAllTriangulations(IntermediateTriangulation[] triangulations) {
    // Start the merge process
    while (triangulations.length > 1) {
      // Merge every pair of DTs
      IntermediateTriangulation[] newTriangulations = new IntermediateTriangulation[triangulations.length / 2 + (triangulations.length & 1)];
      int t2 = 0;
      int t1;
      for (t1 = 0; t1 < triangulations.length - 1; t1 += 2) {
        IntermediateTriangulation dt1 = triangulations[t1];
        IntermediateTriangulation dt2 = triangulations[t1+1];
        newTriangulations[t2++] = merge(dt1, dt2);
        if (progress != null)
          progress.progress();
      }
      if (t1 < triangulations.length)
        newTriangulations[t2++] = triangulations[t1];
      triangulations = newTriangulations;
    }
    return triangulations[0];
  }

  public Triangulation getFinalAnswer() {
    return new Triangulation(this);
  }

  /**
   * Splits the answer into final and non-final parts. Final parts can be safely
   * written to the output as they are not going to be affected by a future
   * merge step. Non-final parts cannot be written to the output yet as they
   * might be affected (i.e., some edges are pruned) by a future merge step.
   * 
   * @param mbr
   *          The MBR used to check for final and non-final edges. It is assumed
   *          that no more points can be introduced in this MBR but more points
   *          can appear later outside that MBR.
   * @param safe
   *          The set of safe edges
   * @param unsafe
   *          The set of unsafe edges
   */
  public void splitIntoFinalAndNonFinalParts(Rectangle mbr,
      Triangulation finalPart, Triangulation nonfinalPart) {
    // Nodes that has its adjacency list sorted by node ID to speed up the merge
    BitArray sortedNodes = new BitArray(points.length);
    // Sites that has been found to be unsafe
    BitArray unsafeNodes = new BitArray(points.length);
    // Sites that need to be checked whether they have unsafe triangles or not
    IntArray nodesToCheck = new IntArray();
    
    // Initially, add all sites on the convex hull to unsafe edges
    for (int convexHullPoint : finalAnswer.convexHull) {
      nodesToCheck.add(convexHullPoint);
      unsafeNodes.set(convexHullPoint, true);
    }
    
    while (!nodesToCheck.isEmpty()) {
      if (progress != null)
        progress.progress();
      int unsafeNode = nodesToCheck.pop();
      IntArray neighbors1 = neighbors[unsafeNode];
      // Sort the array to speedup merging neighbors
      if (!sortedNodes.get(unsafeNode)) {
        neighbors1.sort();
        sortedNodes.set(unsafeNode, true);
      }
      for (int neighborID : neighbors1) {
        IntArray neighbors2 = neighbors[neighborID];
        // Sort neighbor nodes, if needed
        if (!sortedNodes.get(neighborID)) {
          neighbors2.sort();
          sortedNodes.set(neighborID, true);
        }
        // Find common nodes which form triangles
        int i1 = 0, i2 = 0;
        while (i1 < neighbors1.size() && i2 < neighbors2.size()) {
          if (neighbors1.get(i1) == neighbors2.get(i2)) {
            boolean safeTriangle = true;
            // Found a triangle between unsafeNode, neighborID and neighbors1[i1]
            Point emptyCircle = calculateCircumCircleCenter(unsafeNode, neighborID, neighbors1.get(i1));
            if (!mbr.contains(emptyCircle)) {
              // The center is outside the MBR, unsafe
              safeTriangle = false;
            } else {
              // If the empty circle is not completely contained in the MBR,
              // the triangle is unsafe as the circle might become non-empty
              // when the merge process happens
              double dx = emptyCircle.x - xs[unsafeNode];
              double dy = emptyCircle.y - ys[unsafeNode];
              double r2 = dx * dx + dy * dy;
              double dist = Math.min(Math.min(emptyCircle.x - mbr.x1, mbr.x2 - emptyCircle.x),
                  Math.min(emptyCircle.y - mbr.y1, mbr.y2 - emptyCircle.y));
              if (dist * dist <= r2)
                safeTriangle = false;
            }
            if (!safeTriangle) {
              // The three nodes are unsafe and need to be further checked
              if (!unsafeNodes.get(neighborID)) {
                nodesToCheck.add(neighborID);
                unsafeNodes.set(neighborID, true);
              }
              if (!unsafeNodes.get(neighbors1.get(i1))) {
                nodesToCheck.add(neighbors1.get(i1));
                unsafeNodes.set(neighbors1.get(i1), true);
              }
            }
            i1++;
            i2++;
          } else if (neighbors1.get(i1) < neighbors2.get(i2)) {
            i1++;
          } else {
            i2++;
          }
        }
      }
    }
    
    IntArray finalEdgeStarts = new IntArray();
    IntArray finalEdgeEnds = new IntArray();
    IntArray nonfinalEdgeStarts = new IntArray();
    IntArray nonfinalEdgeEnds = new IntArray();

    // Prune all edges that connect two safe nodes
    for (int i = 0; i < points.length; i++) {
      if (progress != null)
        progress.progress();
      if (unsafeNodes.get(i)) {
        // An unsafe node, all of its adjacent edges are also unsafe
        for (int n : neighbors[i]) {
          if (i < n) {
            nonfinalEdgeStarts.add(i);
            nonfinalEdgeEnds.add(n);
          }
        }
      } else {
        // Found a safe node, all edges that are adjacent to another safe node
        // are final edges
        
        // Whether an adjacent edge has been written or not
        for (int n : neighbors[i]) {
          if (i < n) {
            if (!unsafeNodes.get(n)) {
              // Found a final edge
              finalEdgeStarts.add(i);
              finalEdgeEnds.add(n);
            } else {
              nonfinalEdgeStarts.add(i);
              nonfinalEdgeEnds.add(n);
            }
          }
        }
      }
    }
    
    finalPart.sites = this.points;
    finalPart.edgeStarts = finalEdgeStarts.toArray();
    finalPart.edgeEnds = finalEdgeEnds.toArray();
    finalPart.compact();

    nonfinalPart.sites = this.points;
    nonfinalPart.edgeStarts = nonfinalEdgeStarts.toArray();
    nonfinalPart.edgeEnds = nonfinalEdgeEnds.toArray();
    nonfinalPart.compact();
  }

  public boolean test() {
    final double threshold = 1E-6;
    List<Point> starts = new Vector<Point>();
    List<Point> ends = new Vector<Point>();
    for (int s1 = 0; s1 < points.length; s1++) {
      for (int s2 : neighbors[s1]) {
        if (s1 < s2) {
          starts.add(new Point(xs[s1], ys[s1]));
          ends.add(new Point(xs[s2], ys[s2]));
        }
      }
    }
    
    for (int i = 0; i < starts.size(); i++) {
      double x1 = starts.get(i).x;
      double y1 = starts.get(i).y;
      double x2 = ends.get(i).x;
      double y2 = ends.get(i).y;
      for (int j = i + 1; j < starts.size(); j++) {
        double x3 = starts.get(j).x;
        double y3 = starts.get(j).y;
        double x4 = ends.get(j).x;
        double y4 = ends.get(j).y;
  
        double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
        double ix = (x1 * y2 - y1 * x2) * (x3 - x4) / den - (x1 - x2) * (x3 * y4 - y3 * x4) / den;
        double iy = (x1 * y2 - y1 * x2) * (y3 - y4) / den - (y1 - y2) * (x3 * y4 - y3 * x4) / den;
        double minx1 = Math.min(x1, x2);
        double maxx1 = Math.max(x1, x2); 
        double miny1 = Math.min(y1, y2);
        double maxy1 = Math.max(y1, y2); 
        double minx2 = Math.min(x3, x4);
        double maxx2 = Math.max(x3, x4); 
        double miny2 = Math.min(y3, y4);
        double maxy2 = Math.max(y3, y4); 
        if ((ix - minx1 > threshold && ix - maxx1 < -threshold) && (iy - miny1 > threshold && iy - maxy1 < -threshold) &&
            (ix - minx2 > threshold && ix - maxx2 < -threshold) && (iy - miny2 > threshold && iy - maxy2 < -threshold)) {
          System.out.printf("line %f, %f, %f, %f\n", x1, y1, x2, y2);
          System.out.printf("line %f, %f, %f, %f\n", x3, y3, x4, y4);
          System.out.printf("circle %f, %f, 0.5\n", ix, iy);
          throw new RuntimeException("error");
        }
      }
    }
    return true;
  }

  boolean inArray(int[] array, int objectToFind) {
    for (int objectToCompare : array)
      if (objectToFind == objectToCompare)
        return true;
    return false;
  }
  
  double calculateCWAngle(int s1, int s2, int s3) {
    double angle1 = Math.atan2(ys[s1] - ys[s2], xs[s1] - xs[s2]);
    double angle2 = Math.atan2(ys[s3] - ys[s2], xs[s3] - xs[s2]);
    return angle1 > angle2 ? (angle1 - angle2) : (Math.PI * 2 + (angle1 - angle2));
  }

  Point calculateCircumCircleCenter(int s1, int s2, int s3) {
    // Calculate the perpendicular bisector of the first two points
    double x1 = (xs[s1] + xs[s2]) / 2;
    double y1 = (ys[s1] + ys[s2]) /2;
    double x2 = x1 + ys[s2] - ys[s1];
    double y2 = y1 + xs[s1] - xs[s2];
    // Calculate the perpendicular bisector of the second two points 
    double x3 = (xs[s3] + xs[s2]) / 2;
    double y3 = (ys[s3] + ys[s2]) / 2;
    double x4 = x3 + ys[s2] - ys[s3];
    double y4 = y3 + xs[s3] - xs[s2];
    
    // Calculate the intersection of the two new lines
    // See https://en.wikipedia.org/wiki/Line%E2%80%93line_intersection
    double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
    double ix = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
    double iy = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
    
    // We can also use the following equations from
    // http://mathforum.org/library/drmath/view/62814.html
//    double v12x = x2 - x1;
//    double v12y = y2 - y1;
//    double v34x = x4 - x3;
//    double v34y = y4 - y3;
//    
//    double a = ((x3 - x1) * v34y - (y3 - y1) * v34x) / (v12x * v34y - v12y * v34x);
//    double ix = x1 + a * v12x;
//    double iy = y1 + a * v12y;
    
    return new Point(ix, iy);
  }
  
  int[] convexHull(int[] points) {
    Stack<Integer> lowerChain = new Stack<Integer>();
    Stack<Integer> upperChain = new Stack<Integer>();

    // Sort sites by increasing x-axis
    // Sorting by Site ID is equivalent to sorting by x-axis as the original
    // array of site is sorted by x in the Delaunay Triangulation algorithm
    Arrays.sort(points);
    
    // Lower chain
    for (int i = 0; i < points.length; i++) {
      while(lowerChain.size() > 1) {
        int s1 = lowerChain.get(lowerChain.size() - 2);
        int s2 = lowerChain.get(lowerChain.size() - 1);
        int s3 = points[i];
        double crossProduct = (xs[s2] - xs[s1]) * (ys[s3] - ys[s1]) - (ys[s2] - ys[s1]) * (xs[s3] - xs[s1]);
        if (crossProduct <= 0) lowerChain.pop();
        else break;
      }
      lowerChain.push(points[i]);
    }
    
    // Upper chain
    for (int i = points.length - 1; i >= 0; i--) {
      while(upperChain.size() > 1) {
        int s1 = upperChain.get(upperChain.size() - 2);
        int s2 = upperChain.get(upperChain.size() - 1);
        int s3 = points[i];
        double crossProduct = (xs[s2] - xs[s1]) * (ys[s3] - ys[s1]) - (ys[s2] - ys[s1]) * (xs[s3] - xs[s1]);
        if (crossProduct <= 0) upperChain.pop();
        else break;
      }
      upperChain.push(points[i]);
    }
    
    lowerChain.pop();
    upperChain.pop();
    int[] result = new int[lowerChain.size() + upperChain.size()];
    for (int i = 0; i < lowerChain.size(); i++)
      result[i] = lowerChain.get(i);
    for (int i = 0; i < upperChain.size(); i++)
      result[i + lowerChain.size()] = upperChain.get(i);
    return result;    
  }
}
