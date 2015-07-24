package edu.umn.cs.spatialHadoop.delaunay;

import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * The divide and conquer Delaunay Triangulation algorithm as proposed in
 * L. J Guibas and J. Stolfi, "Primitives for the manipulation of general
 * subdivisions and the computation of Voronoi diagrams",
 * ACM Transactions on Graphics, 4(1985), 74-123,
 * and as further illustrated in
 * http://www.geom.uiuc.edu/~samuelp/del_project.html
 * @author Ahmed Eldawy
 *
 */
public class GuibasStolfiDelaunayAlgorithm {
  
  private Point[] points;
  /**Coordinates of all sites*/
  private double[] xs, ys;
  /**All neighboring sites. A neighbor site has a common edge in the 
   * Delaunay triangulation*/
  private IntArray[] neighbors;

  public <P extends Point> GuibasStolfiDelaunayAlgorithm(P[] points) {
    this.points = new Point[points.length];
    System.arraycopy(points, 0, this.points, 0, points.length);
    Arrays.sort(this.points);
    this.xs = new double[this.points.length];
    this.ys = new double[this.points.length];
    this.neighbors = new IntArray[this.points.length];
    for (int i = 0; i < this.points.length; i++) {
      xs[i] = this.points[i].x;
      ys[i] = this.points[i].y;
      neighbors[i] = new IntArray();
    }
  }

  /** Computes the Delaunay triangulation for the points */
  public Triangulation compute() {
    Triangulation[] triangulations = new Triangulation[points.length / 3 + (points.length % 3 == 0 ? 0 : 1)];
    // Compute the trivial Delaunay triangles of every three consecutive points
    int i, t=0;
    for (i = 0; i < points.length - 4; i += 3) {
      // Compute Delaunay triangulation for three points
      triangulations[t++] =  new Triangulation(i, i+1, i+2);
    }
    if (points.length - i == 4) {
      // Compute Delaunay triangulation for every two points
       triangulations[t++] = new Triangulation(i, i+1);
       triangulations[t++] = new Triangulation(i+2, i+3);
    } else if (points.length - i == 3) {
      // Compute for three points
      triangulations[t++] = new Triangulation(i, i+1, i+2);
    } else if (points.length - i == 2) {
      // Two points, connect with a line
      triangulations[t++] = new Triangulation(i, i+1);
    } else {
      throw new RuntimeException("Cannot happen");
    }
    
    // Start the merge process
    while (triangulations.length > 1) {
      // Merge every pair of Deluanay triangulations
      Triangulation[] newTriangulations = new Triangulation[triangulations.length / 2 + (triangulations.length & 1)];
      int t2 = 0;
      int t1;
      for (t1 = 0; t1 < triangulations.length - 1; t1 += 2) {
        Triangulation dt1 = triangulations[t1];
        Triangulation dt2 = triangulations[t1+1];
        newTriangulations[t2++] = new Triangulation(dt1, dt2);
      }
      if (t1 < triangulations.length)
        newTriangulations[t2++] = triangulations[t1];
      triangulations = newTriangulations;
    }
    return triangulations[0];
  }
  
  
  class Triangulation {
    /**
     * The contiguous range of sites stored at this triangulation.
     * The range is inclusive of both site1 and site2
     */
    int site1, site2;
    /**Sites on the convex null of the triangulation*/
    int[] convexHull;
    
    /**
     * Initialize a triangulation with two sites only. The triangulation consists
     * of one line connecting the two sites and no triangles at all.
     * @param s1
     * @param s2
     */
    public Triangulation(int s1, int s2) {
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
    public Triangulation(int s1, int s2, int s3) {
      site1 = s1;
      site2 = s3;
      neighbors[s1].add(s2); neighbors[s1].add(s3);
      neighbors[s2].add(s1); neighbors[s2].add(s3);
      neighbors[s3].add(s1); neighbors[s3].add(s2);
      convexHull = new int[] {s1, s2, s3};
    }
    
    /**
     * Constructs a triangulation that merges two existing triangulations.
     * @param L
     * @param R
     */
    public Triangulation(Triangulation L, Triangulation R) {
      // Compute the convex hull of the result
      int[] bothHulls = new int[L.convexHull.length + R.convexHull.length];
      System.arraycopy(L.convexHull, 0, bothHulls, 0, L.convexHull.length);
      System.arraycopy(R.convexHull, 0, bothHulls, L.convexHull.length, R.convexHull.length);
      this.convexHull = convexHull(bothHulls);
      
      // Find the base LR-edge (lowest edge of the convex hull that crosses from L to R)
      int baseL = -1, baseR = -1;
      for (int i = 0; i < this.convexHull.length; i++) {
        int p1 = this.convexHull[i];
        int p2 = i == this.convexHull.length - 1 ? this.convexHull[0] : this.convexHull[i+1];
        if (inArray(L.convexHull, p1) && inArray(R.convexHull, p2)) {
          if (baseL == -1 || (ys[p1] <= ys[baseL] && ys[p2] <= ys[baseR]) /*||
              (p1.x <= baseL.x && p2.x <= baseR.x)*/) {
            baseL = p1;
            baseR = p2;
          }
        } else if (inArray(L.convexHull, p2) && inArray(R.convexHull, p1)) {
          if (baseL == -1 || (ys[p2] <= ys[baseL] && ys[p1] <= ys[baseR]) /*||
              (p2.x <= baseL.x && p1.x <= baseR.x)*/) {
            baseL = p2;
            baseR = p1;
          }
        }
      }

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
      
      // Merge both L and R
      this.site1 = L.site1;
      this.site2 = R.site2;
    }

    public void draw() {
      int i =0;
      for (int s1 = site1; s1 <= site2; s1++) {
        for (int s2 : neighbors[s1]) {
          if (s1 < s2) {
            System.out.printf("line %f, %f, %f, %f, :id=>'%d,%d'\n", xs[s1], ys[s1], xs[s2], ys[s2], s1, s2);
            i++;
          }
        }
      }
      System.out.println("Total lines "+i);
    }
    
    public boolean test() {
      final double threshold = 1E-6;
      List<Point> starts = new Vector<Point>();
      List<Point> ends = new Vector<Point>();
      for (int s1 = site1; s1 <= site2; s1++) {
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
