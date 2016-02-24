package edu.umn.cs.spatialHadoop.delaunay;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.QuickSort;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

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
public class GSDTAlgorithm {
  
  static final Log LOG = LogFactory.getLog(GSDTAlgorithm.class);
  
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
      neighbors[s1].add(s2); neighbors[s2].add(s1); // edge: s1 -- s2
      neighbors[s2].add(s3); neighbors[s3].add(s2); // edge: s3 -- s3
      if (calculateCircumCircleCenter(s1, s2, s3) == null) {
        // Degenerate case, three points are collinear
        convexHull = new int[] {s1, s3};
      } else {
        // Normal case
        neighbors[s1].add(s3); neighbors[s3].add(s1); // edge: s1 -- s3
        convexHull = new int[] {s1, s2, s3};
      }
    }

    /**
     * Create an intermediate triangulation out of a triangulation created
     * somewhere else (may be another machine). It stores all the edges in the
     * neighbors array and adjusts the node IDs in edges to match their new
     * position in the {@link GSDTAlgorithm#points} array
     * 
     * @param t
     *          The triangulation that needs to be added
     * @param pointShift
     *          How much shift should be added to each edge to adjust it to the
     *          new points array
     */
    public IntermediateTriangulation(SimpleGraph t, int pointShift) {
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
   * @param points
   * @param progress
   */
  public <P extends Point> GSDTAlgorithm(P[] points, Progressable progress) {
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
    int i, t = 0;
    for (i = 0; i < points.length - 4; i += 3) {
      // Compute DT for three points
      triangulations[t++] =  new IntermediateTriangulation(i, i+1, i+2);
      if (progress != null && (t & 0xff) == 0)
        progress.progress();
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
   * machines. The given triangulations should be sorted correctly such that
   * they can be merged in their current sort order.
   * @param ts
   * @param progress
   */
  public GSDTAlgorithm(SimpleGraph[] ts, Progressable progress) {
    this.progress = progress;
    // Copy all triangulations
    int totalPointCount = 0;
    for (SimpleGraph t : ts)
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
      SimpleGraph t = ts[it];
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
   * Merges a set of triangulations in any sort order. First, the triangulations
   * are sorted in columns and each column is merged separately. After that,
   * columns are merged together to produce one final answer.
   * @param triangulations
   * @param progress
   * @return
   */
  static GSDTAlgorithm mergeTriangulations(
      List<SimpleGraph> triangulations, Progressable progress) {
    // Arrange triangulations column-by-column
    List<List<SimpleGraph>> columns = new ArrayList<List<SimpleGraph>>();
    int numTriangulations = 0;
    for (SimpleGraph t : triangulations) {
      double x1 = t.mbr.x1, x2 = t.mbr.x2;
      List<SimpleGraph> selectedColumn = null;
      int iColumn = 0;
      while (iColumn < columns.size() && selectedColumn == null) {
        Rectangle cmbr = columns.get(iColumn).get(0).mbr;
        double cx1 = cmbr.x1;
        double cx2 = cmbr.x2;
        if (x2 > cx1 && cx2 > x1) {
          selectedColumn = columns.get(iColumn);
        } else {
          iColumn++;
        }
      }
      if (selectedColumn == null) {
        // Create a new column
        selectedColumn = new ArrayList<SimpleGraph>();
        columns.add(selectedColumn);
      }
      selectedColumn.add(t);
      numTriangulations++;
    }
    
    LOG.info("Merging "+numTriangulations+" triangulations in "+columns.size()+" columns" );
    
    List<SimpleGraph> mergedColumns = new ArrayList<SimpleGraph>();
    // Merge all triangulations together column-by-column
    for (List<SimpleGraph> column : columns) {
      // Sort this column by y-axis
      Collections.sort(column, new Comparator<SimpleGraph>() {
        @Override
        public int compare(SimpleGraph t1, SimpleGraph t2) {
          double dy = t1.mbr.y1 - t2.mbr.y1;
          if (dy < 0)
            return -1;
          if (dy > 0)
            return 1;
          return 0;
        }
      });
  
      LOG.info("Merging "+column.size()+" triangulations vertically");
      GSDTAlgorithm algo =
          new GSDTAlgorithm(column.toArray(new SimpleGraph[column.size()]), progress);
      mergedColumns.add(algo.getFinalAnswerAsGraph());
    }
    
    // Merge the result horizontally
    Collections.sort(mergedColumns, new Comparator<SimpleGraph>() {
      @Override
      public int compare(SimpleGraph t1, SimpleGraph t2) {
        double dx = t1.mbr.x1 - t2.mbr.x1;
        if (dx < 0)
          return -1;
        if (dx > 0)
          return 1;
        return 0;
      }
    });
    LOG.info("Merging "+mergedColumns.size()+" triangulations horizontally");
    GSDTAlgorithm algo = new GSDTAlgorithm(
        mergedColumns.toArray(new SimpleGraph[mergedColumns.size()]),
        progress);
    return algo;
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
    do { // Until the finished flag is raised
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
      if (anglePotential < Math.PI && potentialCandidate != -1) {
        // Compute the circum circle between the base edge and potential candidate
        Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
        if (circleCenter == null) {
          // Degnerate case of three collinear points
          // Delete the RR edge between baseR and rPotentialCandidate and restart
          neighbors[baseR].remove(potentialCandidate);
          neighbors[potentialCandidate].remove(baseR);
        } else {
          if (nextPotentialCandidate == -1) {
            // The only potential candidate, accept it right away
            rCandidate = potentialCandidate;
          } else {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
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
          }
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
      if (anglePotential - Math.PI < -1E-9 && potentialCandidate != -1) {
        // Compute the circum circle between the base edge and potential candidate
        Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
        if (circleCenter == null) {
          // Degenerate case, the potential candidate is collinear with base edge
          // Delete the LL edge between baseL and potentialCandidate and restart
          neighbors[baseL].remove(potentialCandidate);
          neighbors[potentialCandidate].remove(baseL);
        } else {
          if (nextPotentialCandidate == -1) {
            // The only potential candidate, accept it right away
            lCandidate = potentialCandidate;
          } else {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
            double dx = circleCenter.x - xs[nextPotentialCandidate];
            double dy = circleCenter.y - ys[nextPotentialCandidate];
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - xs[potentialCandidate];
            dy = circleCenter.y - ys[potentialCandidate];
            double d2 = dx * dx + dy * dy;
            if (d1 < d2) {
              // Delete the LL edge between baseL and potentialCandidate and restart
              neighbors[baseL].remove(potentialCandidate);
              neighbors[potentialCandidate].remove(baseL);
              continue;
            } else {
              lCandidate = potentialCandidate;
            }
          } // nextPotentialCandidate == -1
        } // circleCenter == null
      } // anglePotential < Math.PI      
      // Choose the right candidate
      if (lCandidate != -1 && rCandidate != -1) {
        // Two candidates, choose the correct one
        Point circumCircleL = calculateCircumCircleCenter(baseL, baseR, lCandidate);
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
   * There are at most two crossing edges from L to R, the base edge is the one
   * that makes CW angles with sites in the range [0, PI]. 
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
    double cwAngle = base1R != base2R? calculateCWAngle(base1L, base1R, base2R)
        : calculateCWAngle(base1L, base1R, base2L);
    
    return cwAngle < Math.PI ? new int[] {base1L, base1R} :
      new int[] {base2L, base2R};
  }

  /**
   * Merge all triangulations into one
   * @param triangulations
   * @return 
   */
  protected IntermediateTriangulation mergeAllTriangulations(IntermediateTriangulation[] triangulations) {
    long reportTime = 0;
    // Start the merge process
    while (triangulations.length > 1) {
      long currentTime = System.currentTimeMillis();
      if (currentTime - reportTime > 1000) {
        LOG.info("Merging "+triangulations.length+" triangulations");
        reportTime = currentTime;
      }
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

  /**
   * Returns the final answer as a graph.
   * @return
   */
  public SimpleGraph getFinalAnswerAsGraph() {
    SimpleGraph graph = new SimpleGraph();

    graph.sites = this.points.clone();
    int numEdges = 0;
    graph.mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (int s1 = 0; s1 < this.neighbors.length; s1++) {
      numEdges += this.neighbors[s1].size();
      graph.mbr.expand(graph.sites[s1]);
    }
    graph.safeSites = new BitArray(graph.sites.length);
    graph.safeSites.fill(true);
    numEdges /= 2; // We store each undirected edge once
    graph.edgeStarts = new int[numEdges];
    graph.edgeEnds = new int[numEdges];

    for (int s1 = 0; s1 < this.neighbors.length; s1++) {
      for (int s2 : this.neighbors[s1]) {
        if (s1 < s2) {
          numEdges--;
          graph.edgeStarts[numEdges] = s1;
          graph.edgeEnds[numEdges] = s2;
        }
      }
    }
    if (numEdges != 0)
      throw new RuntimeException("Error in edges! Copied "+
    (graph.edgeStarts.length - numEdges)+" instead of "+graph.edgeStarts.length);
  
    return graph;
  }
  
  /**
   * Computes the final answer as a set of Voronoi regions. Each region is
   * represented by a Geometry. The associated site for each Voronoi region is
   * stored as user data inside the geometry. The polygon that represents each
   * region is clipped to the given rectangle (MBR) to ensure the possibility
   * of representing infinite open regions.
   * The answer is split into final and non-final regions based on the given
   * MBR.
   */
  public void getFinalAnswerAsVoronoiRegions(Rectangle mbr,
      List<Geometry> finalRegions, List<Geometry> nonfinalRegions) {
    // Detect final and non-final sites
    BitArray unsafeSites = detectUnsafeSites(mbr);
    // We use the big MBR to clip open sides of Voronoi regions
    double bufferSize = Math.max(mbr.getWidth(), mbr.getHeight()) / 10;
    Rectangle bigMBR = mbr.buffer(bufferSize, bufferSize);
    Rectangle biggerMBR = mbr.buffer(bufferSize, bufferSize * 1.1);
    // A factory that creates polygons
    final GeometryFactory Factory = new GeometryFactory();
    
    for (int iSite = 0; iSite < points.length; iSite++) {
      // Compute the Voronoi region of this site as a polygon
      // Sort all neighbors in CCW order and compute the perependicular bisector
      // of each incident edge. The intersection of perpendicular bisectors form
      // the points of the polygon that represents the Voronoi region.
      // If the angle between two neighbors is larger than PI, this indicates
      // an open region
      final IntArray siteNeighbors = neighbors[iSite];

      // Compute angles between the points and its neighbors
      final double[] angles = new double[siteNeighbors.size()];
      for (int iNeighbor = 0; iNeighbor < siteNeighbors.size(); iNeighbor++) {
        double dx = points[siteNeighbors.get(iNeighbor)].x - points[iSite].x;
        double dy = points[siteNeighbors.get(iNeighbor)].y - points[iSite].y;
        double ccwAngle = Math.atan2(dy, dx);
        angles[iNeighbor] = ccwAngle < 0 ? ccwAngle += Math.PI * 2 : ccwAngle;
      }
      
      // Sort neighbors by CCW order
      IndexedSortable ccwSort = new IndexedSortable() {
        
        @Override
        public void swap(int i, int j) {
          siteNeighbors.swap(i, j);
          double t = angles[i];
          angles[i] = angles[j];
          angles[j] = t;
        }
        
        @Override
        public int compare(int i, int j) {
          double diff = angles[i] - angles[j];
          if (diff < 0) return -1;
          if (diff > 0) return 1;
          return 0;
        }
      };
      new QuickSort().sort(ccwSort, 0, siteNeighbors.size());
      
      // Traverse neighbors in CCW order and compute intersections of
      // perpendicular bisectors
      List<Point> voronoiRegionPoints = new ArrayList<Point>();

      int firstPoint = -1; // -1 indicates a closed polygon with no first point
      for (int iNeighbor1 = 0; iNeighbor1 < siteNeighbors.size(); iNeighbor1++) {
        int iNeighbor2 = (iNeighbor1 + 1) % siteNeighbors.size();
        double ccwAngle = angles[iNeighbor2] - angles[iNeighbor1];
        if (ccwAngle < 0)
          ccwAngle += Math.PI * 2;
        if (ccwAngle > Math.PI) {
          // An open side of the Voronoi region
          // Compute the intersection of each perpendicular bisector to the
          // boundary
          Point p1 = intersectPerpendicularBisector(iSite, siteNeighbors.get(iNeighbor1), biggerMBR);
          Point p2 = intersectPerpendicularBisector(siteNeighbors.get(iNeighbor2), iSite, biggerMBR);
          voronoiRegionPoints.add(p1);
          voronoiRegionPoints.add(p2);
          // Mark p2 as the first point in the open line string
          firstPoint = voronoiRegionPoints.size() - 1;
        } else {
          // A closed side of the Voronoi region. Calculate the next point as
          // the center of the empty circle
          Point emptyCircleCenter = calculateCircumCircleCenter(iSite,
              siteNeighbors.get(iNeighbor1), siteNeighbors.get(iNeighbor2));
          voronoiRegionPoints.add(emptyCircleCenter);
        }
      }
      
      
      // Create the Voronoi polygon
      Geometry geom;
      if (firstPoint == -1) {
        // Indicates a closed polygon with no starting point
        Coordinate[] coords = new Coordinate[voronoiRegionPoints.size() + 1];
        for (int i = 0; i < voronoiRegionPoints.size(); i++) {
          Point voronoiPoint = voronoiRegionPoints.get(i);
          coords[i] = new Coordinate(voronoiPoint.x, voronoiPoint.y);
        }
        coords[coords.length - 1] = coords[0];
        LinearRing ring = Factory.createLinearRing(coords);
        geom = Factory.createPolygon(ring, null);
      } else {
        // Indicates an open line string with 'firstPoint' as the starting point
        // Reorder points according to first point
        List<Point> orderedPoints = new ArrayList<Point>(voronoiRegionPoints.size());
        orderedPoints.addAll(voronoiRegionPoints.subList(firstPoint, voronoiRegionPoints.size()));
        orderedPoints.addAll(voronoiRegionPoints.subList(0, firstPoint));
        voronoiRegionPoints = orderedPoints;
        
        // Cut all the segments to the bigMBR
        for (int i2 = 1; i2 < voronoiRegionPoints.size(); i2++) {
          int i1 = i2 - 1;
          if (!bigMBR.contains(voronoiRegionPoints.get(i1)) &&
              !bigMBR.contains(voronoiRegionPoints.get(i2))) {
            // The whole line segment is outside, remove the whole segment
            voronoiRegionPoints.remove(i1);
          } else if (!bigMBR.contains(voronoiRegionPoints.get(i1))) {
            // Only the first point is outside the bigMBR
            Point cutPoint = bigMBR.intersectLineSegment(
                voronoiRegionPoints.get(i2), voronoiRegionPoints.get(i1));
            voronoiRegionPoints.set(i1, cutPoint);
          } else if (!bigMBR.contains(voronoiRegionPoints.get(i2))) {
            // Only i2 is outside bigMBR. We cut the whole linestring
            Point cutPoint = bigMBR.intersectLineSegment(
                voronoiRegionPoints.get(i1), voronoiRegionPoints.get(i2));
            voronoiRegionPoints.set(i2, cutPoint);
            // Remove all points after i2
            while (voronoiRegionPoints.size() > i2+1)
              voronoiRegionPoints.remove(voronoiRegionPoints.size() - 1);
          }
        }
        
        
        Coordinate[] coords = new Coordinate[voronoiRegionPoints.size()];
        for (int i = 0; i < voronoiRegionPoints.size(); i++) {
          Point voronoiPoint = voronoiRegionPoints.get(i);
          coords[i] = new Coordinate(voronoiPoint.x, voronoiPoint.y);
        }
        geom = Factory.createLineString(coords);
      }
      geom.setUserData(points[iSite]);
      (unsafeSites.get(iSite) ? nonfinalRegions : finalRegions).add(geom);
    }
  }
  
  /**
   * Splits the answer into final and non-final parts. Final parts can be safely
   * written to the output as they are not going to be affected by a future
   * merge step. Non-final parts cannot be written to the output yet as they
   * might be affected (i.e., some edges are pruned) by a future merge step.
   * @param mbr The MBR used to check for final and non-final edges. It is assumed
   *          that no more points can be introduced in this MBR but more points
   *          can appear later outside that MBR.
   * @param finalGraph The set of final edges
   * @param nonfinalGraph The set of non-final edges
   */
  public void splitIntoFinalAndNonFinalGraphs(Rectangle mbr,
      SimpleGraph finalGraph, SimpleGraph nonfinalGraph) {
    BitArray unsafeSites = detectUnsafeSites(mbr);
    
    // A final edge is incident to at least one unsafe site
    IntArray finalEdgeStarts = new IntArray();
    IntArray finalEdgeEnds = new IntArray();
    // A final edge connects two safe sites
    IntArray nonfinalEdgeStarts = new IntArray();
    IntArray nonfinalEdgeEnds = new IntArray();

    for (int i = 0; i < points.length; i++) {
      if (progress != null)
        progress.progress();
      if (unsafeSites.get(i)) {
        // An unsafe site, all of its adjacent edges are also unsafe
        for (int n : neighbors[i]) {
          if (i < n) { // To ensure that an edge is written only once
            nonfinalEdgeStarts.add(i);
            nonfinalEdgeEnds.add(n);
          }
        }
      } else {
        // Found a safe site, all edges that are adjacent to another safe site
        // are final edges
        for (int n : neighbors[i]) {
          if (i < n) { // To ensure that an edge is written only once
            if (!unsafeSites.get(n)) {
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
    
    finalGraph.sites = this.points;
    finalGraph.edgeStarts = finalEdgeStarts.toArray();
    finalGraph.edgeEnds = finalEdgeEnds.toArray();
    finalGraph.safeSites = unsafeSites.invert();
    finalGraph.compact();

    nonfinalGraph.sites = this.points;
    nonfinalGraph.edgeStarts = nonfinalEdgeStarts.toArray();
    nonfinalGraph.edgeEnds = nonfinalEdgeEnds.toArray();
    nonfinalGraph.safeSites = new BitArray(this.points.length); // No safe sites
    nonfinalGraph.compact();
  }

  /**
   * Detects which sites are unsafe based on the definition of Delaunay
   * triangulation. An unsafe site participates to at least one unsafe triangle.
   * An unsafe triangle has an empty circle that goes (partially) outside the
   * given MBR.
   * @param mbr
   * @return
   */
  private BitArray detectUnsafeSites(Rectangle mbr) {
    // Nodes that has its adjacency list sorted by node ID to speed up the merge
    BitArray sortedSites = new BitArray(points.length);
    // An unsafe site is a site that participates to at least one unsafe triangle 
    BitArray unsafeSites = new BitArray(points.length);
    // Sites that need to be checked whether they have unsafe triangles or not
    IntArray sitesToCheck = new IntArray();
    
    // Initially, add all sites on the convex hull to unsafe sites
    for (int convexHullPoint : finalAnswer.convexHull) {
      sitesToCheck.add(convexHullPoint);
      unsafeSites.set(convexHullPoint, true);
    }
    
    while (!sitesToCheck.isEmpty()) {
      if (progress != null)
        progress.progress();
      int siteToCheck = sitesToCheck.pop();
      IntArray neighbors1 = neighbors[siteToCheck];
      // Sort the array to speedup merging neighbors
      if (!sortedSites.get(siteToCheck)) {
        neighbors1.sort();
        sortedSites.set(siteToCheck, true);
      }
      for (int neighborID : neighbors1) {
        IntArray neighbors2 = neighbors[neighborID];
        // Sort neighbor nodes, if needed
        if (!sortedSites.get(neighborID)) {
          neighbors2.sort();
          sortedSites.set(neighborID, true);
        }
        // Find common nodes which form triangles
        int i1 = 0, i2 = 0;
        while (i1 < neighbors1.size() && i2 < neighbors2.size()) {
          if (neighbors1.get(i1) == neighbors2.get(i2)) {
            // Found a triangle. Check whether the triangle is safe or not
            // A safe triangle is a triangle with an empty circle that fits
            // completely inside partition boundaries. This means that this safe
            // triangle cannot be disrupted by any point in other partitions
            boolean safeTriangle = true;
            // Found a triangle between unsafeNode, neighborID and neighbors1[i1]
            Point emptyCircle = calculateCircumCircleCenter(siteToCheck, neighborID, neighbors1.get(i1));
            if (emptyCircle == null || !mbr.contains(emptyCircle)) {
              // The center is outside the MBR, unsafe
              safeTriangle = false;
            } else {
              // If the empty circle is not completely contained in the MBR,
              // the triangle is unsafe as the circle might become non-empty
              // when the merge process happens
              double dx = emptyCircle.x - xs[siteToCheck];
              double dy = emptyCircle.y - ys[siteToCheck];
              double r2 = dx * dx + dy * dy;
              double dist = Math.min(Math.min(emptyCircle.x - mbr.x1, mbr.x2 - emptyCircle.x),
                  Math.min(emptyCircle.y - mbr.y1, mbr.y2 - emptyCircle.y));
              if (dist * dist <= r2)
                safeTriangle = false;
            }
            if (!safeTriangle) {
              // The two additional sites are unsafe and need to be further checked
              if (!unsafeSites.get(neighborID)) {
                sitesToCheck.add(neighborID);
                unsafeSites.set(neighborID, true);
              }
              if (!unsafeSites.get(neighbors1.get(i1))) {
                sitesToCheck.add(neighbors1.get(i1));
                unsafeSites.set(neighbors1.get(i1), true);
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
    return unsafeSites;
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
  
  /**
   * Calculate the intersection between the perpendicular bisector of the line
   * segment (p1, p2) towards the right (CCW) and the given rectangle.
   * It is assumed that the two end points p1 and p2 lie inside the given
   * rectangle.
   * @param p1
   * @param p2
   * @param rect
   * @return
   */
  Point intersectPerpendicularBisector(int p1, int p2, Rectangle rect) {
    double px = (xs[p1] + xs[p2]) / 2;
    double py = (ys[p1] + ys[p2]) / 2;
    double vx = ys[p1] - ys[p2];
    double vy = xs[p2] - xs[p1];
    double a1 = ((vx >= 0 ? rect.x2 : rect.x1) - px) / vx;
    double a2 = ((vy >= 0 ? rect.y2 : rect.y1) - py) / vy;
    double a = Math.min(a1, a2);
    return new Point(px + a * vx, py + a * vy);
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
    if (den < 1E-11 && den > -1E-11) {
      // The three points are collinear, circle exists at infinity
      // Although the calculations will return coordinates at infinity based
      // on the calculations, we prefer to return null to avoid the following
      // computations and allow the sender to easily check for this degenerate
      // case
      return null;
    }
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
    // array of site is sorted by x in the DT algorithm
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
