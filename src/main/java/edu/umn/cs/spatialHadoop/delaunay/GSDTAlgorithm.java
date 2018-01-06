package edu.umn.cs.spatialHadoop.delaunay;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.util.IFastSum;
import edu.umn.cs.spatialHadoop.util.MergeSorter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.*;

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

  /** A bitmask with a bit set for each site that has been previously reported
   * to the output. This is needed when merging intermediate triangulations to
   * avoid reporting the same output twice. If <code>null</code>, it indicates
   * that no sites have been previously reported.
   */
  BitArray reportedSites;

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
  protected Progressable progress;

  /**A temporary array used to compute sums accurately using {@link IFastSum}*/
  private transient double[] values = new double[17];


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
     * A triangulation with a single site.
     * @param s
     */
    public IntermediateTriangulation(int s) {
      site1 = site2 = s;
      convexHull = new int[] {s};
    }

    /**
     * Initialize a triangulation that consists only of straight lines between
     * the two given sites. If these two sites are not consecutive in the sort
     * order, they are treated as a contiguous range of sites and every pair
     * of consecutive sites are connected with a line. This is used to construct
     * a triangulation for a list of sites that form a straight line.
     * @param s1
     * @param s2
     */
    IntermediateTriangulation(int s1, int s2) {
      site1 = s1;
      site2 = s2;
      convexHull = new int[s2 - s1 + 1];
      for (int s = s1; s < s2; s++) {
        neighbors[s].add(s+1);
        neighbors[s+1].add(s);
        convexHull[s-s1] = s;
      }
      convexHull[s2-s1] = s2;
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
      if (crossProduct(s1, s2, s3) != 0) {
        // The three points are not collinear
        neighbors[s1].add(s3); neighbors[s3].add(s1); // edge: s1 -- s3
      }
      convexHull = new int[] {s1, s2, s3};
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
      }
    }

    @Override
    public String toString() {
      return String.format("Triangulation: [%d, %d]", site1, site2);
    }

    /**
     * Perform some sanity checks to see if the current triangulation could
     * be incorrect. This can be used to find as early as possible when the
     * output becomes bad. Notice that it is named 'isIncorrect' rather than
     * 'isCorrect' for sanity. If the function returns <code>true</code> it
     * indicates for sure that there is some error. If it returns <code>false</code>
     * the underlying triangulation might or might not be correct.
     *
     * In particular, this method checks for three types of errors:
     * <ul>
     *   <li>Intersection: No two segments in the triangulation should intersect</li>
     *   <li>Convex hull: All edges of the convex hull should be part of the Delaunay triangulation</li>
     *   <li>Triangulation: Edges should form triangles. There should not be any areas with
     *   polygons of more than three edges. To test this feature, each vertex is considered. The
     *   neighbors of this vertex are sorted in CCW order. Then, we check that each two consecutive
     *   neighbors that form less than 180 degrees are also neighbors.</li>
     * </ul>
     * @return <code>true</code> if a violation of the Delaunay triangulation was
     * found; <code>false</code> if no errors were found.
     */
    public boolean isIncorrect() {
      // Test if there are any overlapping edges
      // First, compute the MBR of all edges
      class Edge extends Rectangle {
        int source, destination;

        public Edge(int s, int d) {
          this.source = s;
          this.destination = d;
          super.set(xs[s], ys[s], xs[d], ys[d]);
        }
      }

      List<Edge> edges = new ArrayList<Edge>();

      for (int s = site1; s <= site2; s++) {
        for (int d : neighbors[s]) {
          // Add each undirected edge only once
          if (s < d)
            edges.add(new Edge(s, d));
        }
      }

      Edge[] arEdges = edges.toArray(new Edge[edges.size()]);
      final BooleanWritable correct = new BooleanWritable(true);

      try {
        SpatialAlgorithms.SelfJoin_rectangles(arEdges, new OutputCollector<Edge, Edge>() {
          static final double Threshold = 1E-5;
          @Override
          public void collect(Edge e1, Edge e2) throws IOException {
            if (e1.source == e2.source || e1.source == e2.destination ||
                e1.destination == e2.source || e1.destination == e2.destination) {
              // Skip the test if the two edges share an end point.
              // If they share an end point they have to be intersected but it
              // is not considered a violation of the triangulation property.
              return;
            }
            // Do a refine step where we compare the actual lines
            double x1 = xs[e1.source];
            double y1 = ys[e1.source];
            double x2 = xs[e1.destination];
            double y2 = ys[e1.destination];
            double x3 = xs[e2.source];
            double y3 = ys[e2.source];
            double x4 = xs[e2.destination];
            double y4 = ys[e2.destination];

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
            // Make sure that the intersection is on the two line segments.
            // The intersection has to be in the x and y ranges of the two line
            // segments. A threshold is used to avoid precision error where the
            // intersection is off by a little bit due to calculation errors.
            if ((ix > minx1 + Threshold && ix < maxx1-Threshold) && (iy > miny1 + Threshold && iy < maxy1-Threshold) &&
                (ix > minx2 + Threshold && ix < maxx2-Threshold) && (iy > miny2 + Threshold && iy < maxy2-Threshold)) {
              System.out.printf("line %f, %f, %f, %f\n", x1, y1, x2, y2);
              System.out.printf("line %f, %f, %f, %f\n", x3, y3, x4, y4);
              System.out.printf("circle %f, %f, 0.5\n", ix, iy);
              correct.set(false);
            }
          }
        }, null);
        if (!correct.get())
          return true; // true means incorrect

        // Test if all the lines of the convex hull are edges
        boolean collinear_ch = true;
        for (int i = 2; collinear_ch && i < convexHull.length; i++) {
          collinear_ch = crossProduct(convexHull[i-2], convexHull[i-1], convexHull[i]) == 0;
        }

        if (!collinear_ch) {
          // Test if all edges of the convex hull are in the triangulation only
          // if the convex hull is not a line
          for (int i = 0; i < convexHull.length; i++) {
            int s = convexHull[i];
            int d = convexHull[(i+1)%convexHull.length];
            if (!neighbors[s].contains(d)) {
              System.out.printf("Edge %d, %d on the convex hull but not found in the DT\n", s, d);
              return true; // true means incorrect
            }
          }
        }

        // Test that this is indeed a triangulation. For each node, sort its
        // neighbors in a CW order and make sure that every pair of nodes in
        // a consecutive CW order with less than 180 degrees are connected
        for (int i = site1; i <= site2; i++) {
          final IntArray ineighbors = neighbors[i];
          if (ineighbors.size() == 1)
            continue;
          final int center = i;
          //final Point center = points[i];
          Comparator<Integer> ccw_comparator = new Comparator<Integer>() {
            @Override
            public int compare(Integer a, Integer b) {
              if (xs[a] - xs[center] >= 0 && xs[b] - xs[center] < 0)
                return 1;
              if (xs[a] - xs[center] < 0 && xs[b] - xs[center] >= 0)
                return -1;
              if (xs[a] - xs[center] == 0 && xs[b] - xs[center] == 0)
                return Double.compare(ys[b] - ys[center], ys[a] - ys[center]);

              // compute the cross product of vectors (center -> a) x (center -> b)
              double det = (xs[a] - xs[center]) * (ys[b] - ys[center]) - (xs[b] - xs[center]) * (ys[a] - ys[center]);
              if (det < 0)
                return -1;
              if (det > 0)
                return 1;
              return 0;
            }
          };
          for (int n1 = ineighbors.size() - 1; n1 >= 0 ; n1--) {
            for (int n2 = 0; n2 < n1; n2++) {
              // Compare neighbors n2 and n2+1
              final int a = ineighbors.get(n2);
              final int b = ineighbors.get(n2+1);
              if (ccw_comparator.compare(a, b) > 0)
                ineighbors.swap(n2, n2+1);
            }
          }

          for (int j1 = 0; j1 < ineighbors.size(); j1++) {
            int j2 = (j1 + 1) % ineighbors.size();
            int n1 = ineighbors.get(j1);
            int n2 = ineighbors.get(j2);
            // Check if the triangle (i, n1, n1+1) can be reported
            double a_x = xs[n1] - xs[i];
            double a_y = ys[n1] - ys[i];
            double b_x = xs[n2] - xs[i];
            double b_y = ys[n2] - ys[i];
            if (a_x * b_y - a_y * b_x < 0) {
              // Triangle is correct. Now make sure that the edge n1-n2 exists
              if (!neighbors[n1].contains(n2)) {
                System.out.printf("An incomplete triangle (%d,%d,%d)\n",
                    i, n1, n2);
                return true; // true means incorrect
              }
            }
          }
        }


      } catch (IOException e) {
        e.printStackTrace();
        return true; // true means incorrect
      }

      return false; // false means correct
    }

    public Rectangle getMBR() {
      Rectangle mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
      for (int i : convexHull) {
        mbr.expand(points[i]);
      }
      return mbr;
    }

    public void draw(PrintStream out) {
      Rectangle mbr = getMBR();
      this.draw(out, mbr);
    }

    public void draw(PrintStream out, Rectangle mbr) {
      double scale = 1000 / Math.max(mbr.getWidth(), mbr.getHeight());
      this.draw(out, mbr, scale);
    }
    public void draw(PrintStream out, Rectangle mbr, double scale) {
      // Draw to rasem
      out.println("=begin");
      Text text = new Text();
      for (int i = site1; i <= site2; i++) {
        text.clear();
        out.printf("%s\n", points[i].toText(text));
      }
      out.println("=end");
      out.println("group {");
      for (int i = site1; i <= site2; i++) {
        out.printf("text(%f, %f) { raw '%d' }\n", (xs[i]-mbr.x1) * scale,
            (ys[i]-mbr.y1) * scale, i);
      }
      out.println("}");
      out.println("group {");
      for (int i = site1; i <= site2; i++) {
        for (int j : neighbors[i]) {
          if (i < j)
            out.printf("line %f, %f, %f, %f\n", (xs[i]-mbr.x1) * scale,
                (ys[i]-mbr.y1) * scale, (xs[j]-mbr.x1) * scale,
              (ys[j]-mbr.y1) * scale);
        }
      }
      out.println("}");
    }
  }

  /**
   * Constructs a triangulation that merges two existing triangulations.
   * @param inPoints
   * @param progress
   */
  public <P extends Point> GSDTAlgorithm(P[] inPoints, Progressable progress) {
    this.progress = progress;
    this.points = new Point[inPoints.length];
    this.xs = new double[points.length];
    this.ys = new double[points.length];
    this.neighbors = new IntArray[points.length];
    for (int i = 0; i < points.length; i++)
      neighbors[i] = new IntArray();
    this.reportedSites = new BitArray(points.length); // Initialized to zeros
    System.arraycopy(inPoints, 0, points, 0, points.length);

    // Compute the answer
    this.finalAnswer = computeTriangulation(0, points.length);
  }

  /**
   * Performs the actual computation for the given subset of the points array.
   */
  protected IntermediateTriangulation computeTriangulation(int start, int end) {
    // Sort all points by x-coordinates to prepare for processing
    // Sort points by their containing cell
    Arrays.sort(points, new Comparator<Point>() {
      @Override
      public int compare(Point p1, Point p2) {
        int dx = Double.compare(p1.x, p2.x);
        if (dx != 0)
          return dx;
        return Double.compare(p1.y, p2.y);
      }
    });

    // Store all coordinates in primitive arrays for efficiency
    for (int i = start; i < end; i++) {
      xs[i] = points[i].x;
      ys[i] = points[i].y;
    }


    int size = end - start;
    IntermediateTriangulation[] triangulations = new IntermediateTriangulation[size / 3 + (size % 3 == 0 ? 0 : 1)];
    // Compute the trivial Delaunay triangles of every three consecutive points
    int i, t = 0;
    for (i = start; i < end - 4; i += 3) {
      // Compute DT for three points
      triangulations[t++] =  new IntermediateTriangulation(i, i+1, i+2);
      if (progress != null && (t & 0xff) == 0)
        progress.progress();
    }
    if (end - i == 4) {
       // Compute DT for every two points
       triangulations[t++] = new IntermediateTriangulation(i, i+1);
       triangulations[t++] = new IntermediateTriangulation(i+2, i+3);
    } else if (end - i == 3) {
      // Compute for three points
      triangulations[t++] = new IntermediateTriangulation(i, i+1, i+2);
    } else if (end - i == 2) {
      // Two points, connect with a line
      triangulations[t++] = new IntermediateTriangulation(i, i+1);
    } else if (end - i == 1) {
      // One point, have it as a single site
      triangulations[t++] = new IntermediateTriangulation(i);
    }

    if (progress != null)
      progress.progress();
    return mergeAllTriangulations(triangulations);
  }

  /**
   * Compute the DT by merging existing triangulations created at different
   * machines. The given triangulations should be sorted correctly such that
   * they can be merged in their current sort order.
   * @param ts
   * @param progress
   */
  public GSDTAlgorithm(Triangulation[] ts, Progressable progress) {
    this.progress = progress;
    // Copy all triangulations
    int totalPointCount = 0;
    for (Triangulation t : ts)
      totalPointCount += t.sites.length;
    
    this.points = new Point[totalPointCount];
    this.reportedSites = new BitArray(totalPointCount);
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
        this.reportedSites.set(i, t.reportedSites.get(i - currentPointsCount));
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
      List<Triangulation> triangulations, Progressable progress) {
    // Arrange triangulations column-by-column
    List<List<Triangulation>> columns = new ArrayList<List<Triangulation>>();
    int numTriangulations = 0;
    for (Triangulation t : triangulations) {
      double x1 = t.mbr.x1, x2 = t.mbr.x2;
      List<Triangulation> selectedColumn = null;
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
        selectedColumn = new ArrayList<Triangulation>();
        columns.add(selectedColumn);
      }
      selectedColumn.add(t);
      numTriangulations++;
    }
    
    LOG.debug("Merging "+numTriangulations+" triangulations in "+columns.size()+" columns" );
    
    List<Triangulation> mergedColumns = new ArrayList<Triangulation>();
    // Merge all triangulations together column-by-column
    for (List<Triangulation> column : columns) {
      // Sort this column by y-axis
      Collections.sort(column, new Comparator<Triangulation>() {
        @Override
        public int compare(Triangulation t1, Triangulation t2) {
          double dy = t1.mbr.y1 - t2.mbr.y1;
          if (dy < 0)
            return -1;
          if (dy > 0)
            return 1;
          return 0;
        }
      });
  
      LOG.debug("Merging "+column.size()+" triangulations vertically");
      GSDTAlgorithm algo =
          new GSDTAlgorithm(column.toArray(new Triangulation[column.size()]), progress);
      mergedColumns.add(algo.getFinalTriangulation());
    }
    
    // Merge the result horizontally
    Collections.sort(mergedColumns, new Comparator<Triangulation>() {
      @Override
      public int compare(Triangulation t1, Triangulation t2) {
        double dx = t1.mbr.x1 - t2.mbr.x1;
        if (dx < 0)
          return -1;
        if (dx > 0)
          return 1;
        return 0;
      }
    });
    LOG.debug("Merging "+mergedColumns.size()+" triangulations horizontally");
    GSDTAlgorithm algo = new GSDTAlgorithm(
        mergedColumns.toArray(new Triangulation[mergedColumns.size()]),
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
    // Trace the base LR edge up to the top
    neighbors[baseL].add(baseR);
    neighbors[baseR].add(baseL);
    boolean finished = false;
    do { // Until the finished flag is raised
      // Search for the potential candidate on the right
      // Cache the cross product of the potential candidate and next one
      double crossProductPotentialCandidate=0, crossProductNextPotentialCandidate = 0;
      int potentialCandidate = -1, nextPotentialCandidate = -1;
      for (int rNeighbor : neighbors[baseR]) {
        // Check if this is an edge that crosses from L to R and skip it if true
        if (rNeighbor <= L.site2)
          continue;
        double crossProduct = crossProduct(baseL, baseR, rNeighbor);
        // This neighbor can be considered, let's add it in the right order
        if (potentialCandidate == -1 || crossProductPotentialCandidate < 0 ||
            (crossProduct > 0 && crossProduct(potentialCandidate, baseR, rNeighbor) < 0)){
          nextPotentialCandidate = potentialCandidate;
          crossProductNextPotentialCandidate = crossProductPotentialCandidate;
          potentialCandidate = rNeighbor;
          crossProductPotentialCandidate = crossProduct;
        } else if (nextPotentialCandidate == -1 || crossProductNextPotentialCandidate < 0 ||
            (crossProduct > 0 && crossProduct(nextPotentialCandidate, baseR, rNeighbor) < 0))  {
          nextPotentialCandidate = rNeighbor;
          crossProductNextPotentialCandidate = crossProduct;
        }
      }
      int rCandidate;
      if (potentialCandidate == -1 || crossProductPotentialCandidate <= 0) {
        // No potential candidate
        rCandidate = -1;
      } else if (nextPotentialCandidate == -1){
        rCandidate = potentialCandidate;
      } else {
        if (inCircle(baseL, baseR, potentialCandidate, nextPotentialCandidate)) {
          // Delete the RR edge between baseR and rPotentialCandidate and restart
          neighbors[baseR].remove(potentialCandidate);
          neighbors[potentialCandidate].remove(baseR);
          continue;
        } else {
          rCandidate = potentialCandidate;
        }
      }
      // Search for the potential candidate on the left
      crossProductPotentialCandidate=0; crossProductNextPotentialCandidate = 0;
      potentialCandidate = -1; nextPotentialCandidate = -1;
      for (int lNeighbor : neighbors[baseL]) {
        if (lNeighbor > L.site2)
          continue;
        // Check this LL edge
        double crossProduct = crossProduct(baseL, baseR, lNeighbor);
        // This neighbor can be considered, let's add it in the right order
        if (potentialCandidate == -1 || crossProductPotentialCandidate < 0 ||
            (crossProduct > 0 && crossProduct(potentialCandidate, baseL, lNeighbor) > 0)){
          nextPotentialCandidate = potentialCandidate;
          crossProductNextPotentialCandidate = crossProductPotentialCandidate;
          potentialCandidate = lNeighbor;
          crossProductPotentialCandidate = crossProduct;
        } else if (nextPotentialCandidate == -1 || crossProductNextPotentialCandidate < 0 ||
            (crossProduct > 0 && crossProduct(nextPotentialCandidate, baseL, lNeighbor) > 0))  {
          nextPotentialCandidate = lNeighbor;
          crossProductNextPotentialCandidate = crossProduct;
        }
      }
      int lCandidate = -1;
      if (potentialCandidate == -1 || crossProductPotentialCandidate <= 0) {
        // No potential candidate
        lCandidate = -1;
      } else if (nextPotentialCandidate == -1){
        lCandidate = potentialCandidate;
      } else {
        if (inCircle(baseL, baseR, potentialCandidate, nextPotentialCandidate)) {
          // Delete the LL edge between baseR and rPotentialCandidate and restart
          neighbors[baseL].remove(potentialCandidate);
          neighbors[potentialCandidate].remove(baseL);
          continue;
        } else {
          lCandidate = potentialCandidate;
        }
      }
      // Choose the right candidate
      if (lCandidate != -1 && rCandidate != -1) {
        // Two candidates, choose the candidate that defines a circle with the
        // base edge that does NOT contain the other candidate

        // Check if the left candidate is valid, if not, choose the *right* one
        if (!inCircle(baseL, baseR, lCandidate, rCandidate)) {
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
   * There are at most two crossing edges from L to R, the correct one is the
   * one that crosses from L to R as we traverse the CH in the CW order.
   * @param mergedConvexHull
   * @param L
   * @param R
   * @return
   */
  private int[] findBaseEdge(int[] mergedConvexHull, IntermediateTriangulation L,
      IntermediateTriangulation R) {
    int baseL, baseR;
    int i = 0;
    while (i < mergedConvexHull.length && mergedConvexHull[i] > L.site2) {
      // The first point is on the right side, continue the search until we
      // find a point on the left side
      i++;
    }
    // Now i points to the first point on the left side, continue the search
    // until we find the first point on the right side.
    while (++i < mergedConvexHull.length && mergedConvexHull[i] <= L.site2);
    baseL = mergedConvexHull[i-1];
    baseR = mergedConvexHull[i % mergedConvexHull.length];

    return new int[] {baseL, baseR};
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
        LOG.debug("Merging "+triangulations.length+" triangulations");
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
   * Returns the final answer as a triangulation.
   * @return
   */
  public Triangulation getFinalTriangulation() {
    Triangulation result = new Triangulation();

    result.sites = this.points.clone();
    result.reportedSites = this.reportedSites;
    result.sitesToReport = this.reportedSites.invert();
    int numEdges = 0;
    result.mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (int s1 = 0; s1 < this.neighbors.length; s1++) {
      //if (this.neighbors[s1].isEmpty())
      //  throw new RuntimeException("A site has no incident edges");
      numEdges += this.neighbors[s1].size();
      result.mbr.expand(result.sites[s1]);
    }
    // We store each undirected edge twice, once for each direction
    result.edgeStarts = new int[numEdges];
    result.edgeEnds = new int[numEdges];

    for (int s1 = 0; s1 < this.neighbors.length; s1++) {
      for (int s2 : this.neighbors[s1]) {
        numEdges--;
        result.edgeStarts[numEdges] = s1;
        result.edgeEnds[numEdges] = s2;
      }
    }
    result.sortEdges();
    if (numEdges != 0)
      throw new RuntimeException("Error in edges! Copied "+
    (result.edgeStarts.length - numEdges)+" instead of "+result.edgeStarts.length);
  
    return result;
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
   * Splits the answer into safe and unsafe parts. Safe parts are final and can
   * be safely written to the output as they are not going to be affected by any
   * future merge steps. Unsafe parts cannot be written to the output yet as they
   * might be affected by a future merge step, e.g., some edges are pruned.
   *
   * Here is how this functions works.
   * 1. It classifies sites into safe sites and unsafe sites using the method
   *   detectUnsafeSites. An unsafe site participates to at least one unsafe
   *   triangle
   * 2. Two graphs are written, one that contains all safe sites along with all
   *   their incident edges and one for unsafe sites along with all their
   *   incident edges.
   *
   * Notice that edges are not completely separated as an edge connecting a safe
   * and an unsafe site is written twice. Keep in mind that an edge that is
   * incident to a safe site is also safe because, by definition, it
   * participates in at least one safe triangle.
   * This also means that the safe graph might contain some unsafe sites and
   * the unsafe graph might contain some safe sites. This cannot be avoided as
   * those edges connecting a safe and unsafe site is written in both graphs and
   * needs its two ends to be present for completeness.
   *
   * @param mbr The MBR used to check for safe and unsafe sites. It is assumed
   *          that no more points can be introduced in this MBR but more points
   *          can appear later outside that MBR.
   * @param safeGraph The graph of all safe sites along with their edges and neighbors.
   * @param unsafeGraph The set of unsafe sites along with their edges and neighbors.
   */
  public void splitIntoSafeAndUnsafeGraphs(Rectangle mbr,
                                           Triangulation safeGraph,
                                           Triangulation unsafeGraph) {
    BitArray unsafeSites = detectUnsafeSites(mbr);

    // A safe edge is incident to at least one safe site
    IntArray safeEdgeStarts = new IntArray();
    IntArray safeEdgeEnds = new IntArray();
    // An unsafe edge is incident to at least one unsafe site
    IntArray unsafeEdgeStarts = new IntArray();
    IntArray unsafeEdgeEnds = new IntArray();

    for (int i = 0; i < points.length; i++) {
      if (progress != null)
        progress.progress();
      for (int n : neighbors[i]) {
        if (unsafeSites.get(n) || unsafeSites.get(i)) {
          unsafeEdgeStarts.add(i);
          unsafeEdgeEnds.add(n);
        }
        // Do NOT add an else statement because an edge might be added to both
        if (!unsafeSites.get(n) || !unsafeSites.get(i)) {
          safeEdgeStarts.add(i);
          safeEdgeEnds.add(n);
        }
      }
    }
    
    safeGraph.sites = this.points;
    safeGraph.edgeStarts = safeEdgeStarts.toArray();
    safeGraph.edgeEnds = safeEdgeEnds.toArray();
    safeGraph.sitesToReport = unsafeSites.or(reportedSites).invert();
    safeGraph.reportedSites = reportedSites;
    safeGraph.sortEdges();

    unsafeGraph.sites = this.points;
    unsafeGraph.edgeStarts = unsafeEdgeStarts.toArray();
    unsafeGraph.edgeEnds = unsafeEdgeEnds.toArray();
    unsafeGraph.sitesToReport = new BitArray(this.points.length); // Report nothing
    unsafeGraph.reportedSites = safeGraph.sitesToReport.or(this.reportedSites);
    unsafeGraph.sortEdges();

    safeGraph.compact();
    unsafeGraph.compact();
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

  /**
   * Compute the cross product of the two vectors (s2->s1) and (s2->s3)
   * @param s1
   * @param s2
   * @param s3
   * @return
   */
  double crossProduct(int s1, int s2, int s3) {
    double dx1 = xs[s1]-xs[s2];
    double dy1 = ys[s1]-ys[s2];
    double dx2 = xs[s3]-xs[s2];
    double dy2 = ys[s3]-ys[s2];
    return dy1 * dx2 - dx1 * dy2;
  }


  /**
   * Calculate the intersection between the perpendicular bisector of the line
   * segment (p1, p2) towards the right (CCW) and the given rectangle.
   * It is assumed that the two pend points p1 and p2 lie inside the given
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

  /**
   * @deprecated use inCircle(int, int, int, int) instead
   * @param s1
   * @param s2
   * @param s3
   * @return
   */
  @Deprecated
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

  /**
   * Returns true if p4 is inside the circumcircle of the three points
   * p1, p2, and p3
   * If p4 is exactly on the circumference of the circle, it returns false.
   * See Guibas and Stolfi (1985) p.107.
   * @return true iff p4 is inside the circle (p1, p2, p3)
   */
  boolean inCircle(int p1, int p2, int p3, int p4) {

    // The calculations might seem too complicated. They were originally written
    // in a very simple way but they resulted in round-off errors in very
    // degenerate cases. After some investigation and tests, I ended up rewriting
    // them in the way below. I had to inline the calculation of the area of
    // a triangle and integrate it in the higher level to be able to reach the
    // expressions below. The original expressions were:
    // triArea(a,b,c) = (b.x - a.x)*(c.y - a.y) - (b.y - a.y)*(c.x - a.x)
    // norm2(a) = a.x^2 + a.y^2
    // inCircle(p1,p2,p3,p4) = triArea(p2,p3,p4)*norm2(p1) - triArea(p1,p3,p4)*norm2(p2)
    //                        +triArea(p1,p2,p4)*norm2(p3) - triArea(p1,p2,p3)*norm2(p4);

    double v1 = (xs[p3] - xs[p2]) * (ys[p4] - ys[p2]);
    double v2 = (ys[p3] - ys[p2]) * (xs[p4] - xs[p2]);
    values[1]  = v1 * xs[p1] * xs[p1];
    values[2]  = -v2 * xs[p1] * xs[p1];
    values[3]  =  v1 * ys[p1] * ys[p1];
    values[4]  = -v2 * ys[p1] * ys[p1];

    v1 = (xs[p3] - xs[p1]) * (ys[p4] - ys[p1]);
    v2 = (ys[p3] - ys[p1]) * (xs[p4] - xs[p1]);
    values[5]  = -v1 * xs[p2] * xs[p2];
    values[6]  =  v2 * xs[p2] * xs[p2];
    values[7]  = -v1 * ys[p2] * ys[p2];
    values[8]  =  v2 * ys[p2] * ys[p2];

    v1 = (xs[p2] - xs[p1]) * (ys[p4] - ys[p1]);
    v2 = (ys[p2] - ys[p1]) * (xs[p4] - xs[p1]);

    values[9]  =  v1 * xs[p3] * xs[p3];
    values[10] = -v2 * xs[p3] * xs[p3];
    values[11] =  v1 * ys[p3] * ys[p3];
    values[12] = -v2 * ys[p3] * ys[p3];

    v1 = (xs[p2] - xs[p1]) * (ys[p3] - ys[p1]);
    v2 = (ys[p2] - ys[p1]) * (xs[p3] - xs[p1]);

    values[13] = -v1 * xs[p4] * xs[p4];
    values[14] =  v2 * xs[p4] * xs[p4];
    values[15] = -v1 * ys[p4] * ys[p4];
    values[16] =  v2 * ys[p4] * ys[p4];

    IFastSum.r_c = 0;
    return IFastSum.iFastSum(values, 16) > 0;
  }


  /**
   * Compute the convex hull of a list of points given by their indexes in the
   * array of points
   * @param points
   * @return
   */
  int[] convexHull(final int[] points) {
    Stack<Integer> lowerChain = new Stack<Integer>();
    Stack<Integer> upperChain = new Stack<Integer>();

    // Sort sites by increasing x-axis. We cannot rely of them being sorted as
    // different algorithms can partition the data points in different ways
    // For example, Dwyer's algorithm partition points by a grid
    IndexedSorter sort = new MergeSorter();
    IndexedSortable xSortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        int dx = Double.compare(xs[points[i]], xs[points[j]]);
        if (dx != 0)
          return dx;
        return Double.compare(ys[points[i]], ys[points[j]]);
      }

      @Override
      public void swap(int i, int j) {
        int temp = points[i];
        points[i] = points[j];
        points[j] = temp;
      }
    };
    sort.sort(xSortable, 0, points.length);

    // pminmin, pminmax, pmaxmin, and pmaxmax are used to handle the case where
    // several points have min x or several points have maxx
    // Check http://geomalgorithms.com/a10-_hull-1.html
    int pminmin = 0;
    int pminmax = pminmin;
    while (pminmax < points.length && xs[points[pminmax]] == xs[points[pminmin]])
      pminmax++;
    if (pminmax == points.length) {
      // Special case, all points form one vertical line, return all of them
      return points;
    }
    pminmax--;
    int pmaxmax = points.length - 1;
    int pmaxmin = pmaxmax;
    while (pmaxmin > 0 && xs[points[pmaxmin]] == xs[points[pmaxmax]])
      pmaxmin--;
    pmaxmin++;

    // Lower chain
    lowerChain.push(points[pminmin]);
    for (int i = pminmax+1; i <= pmaxmin; i++) {
      while(lowerChain.size() > 1) {
        int s1 = lowerChain.get(lowerChain.size() - 2);
        int s2 = lowerChain.get(lowerChain.size() - 1);
        int s3 = points[i];
        // Compute the cross product between (s1 -> s2) and (s1 -> s3)
        double crossProduct = (xs[s2] - xs[s1]) * (ys[s3] - ys[s1]) - (ys[s2] - ys[s1]) * (xs[s3] - xs[s1]);
        // Changing the following condition to "crossProduct <= 0" will remove
        // collinear points along the convex hull edge which might product incorrect
        // results with Delaunay triangulation as it might skip the points in
        // the middle.
        if (crossProduct < 0)
          lowerChain.pop();
        else break;
      }
      lowerChain.push(points[i]);
    }
    
    // Upper chain
    for (int i = pmaxmax; i >= pminmax; i--) {
      while(upperChain.size() > 1) {
        int s1 = upperChain.get(upperChain.size() - 2);
        int s2 = upperChain.get(upperChain.size() - 1);
        int s3 = points[i];
        double crossProduct = (xs[s2] - xs[s1]) * (ys[s3] - ys[s1]) - (ys[s2] - ys[s1]) * (xs[s3] - xs[s1]);
        // Changing the following condition to "crossProduct <= 0" will remove
        // collinear points along the convex hull edge which might product incorrect
        // results with Delaunay triangulation as it might skip the points in
        // the middle.
        if (crossProduct < 0)
          upperChain.pop();
        else break;
      }
      upperChain.push(points[i]);
    }
    
    lowerChain.pop();
    upperChain.pop();

    int[] result = new int[lowerChain.size() + upperChain.size() + (pminmax-pminmin) + (pmaxmax-pmaxmin)];
    if (result.length > points.length) {
      // More points on the convex hull than the actual points.
      // The only case when this happens is when all the points are collinear
      // Simply, return the original set of points which are sorted on (x, y)
      return points;
    }
    int iResult = 0;
    for (int i = pminmax; i > pminmin; i--)
      result[iResult++] = points[i];
    for (int il = 0; il < lowerChain.size(); il++)
      result[iResult++] = lowerChain.get(il);
    for (int i = pmaxmin; i < pmaxmax; i++)
      result[iResult++] = points[i];
    for (int iu = 0; iu < upperChain.size(); iu++)
      result[iResult++] = upperChain.get(iu);
    return result;
  }
}