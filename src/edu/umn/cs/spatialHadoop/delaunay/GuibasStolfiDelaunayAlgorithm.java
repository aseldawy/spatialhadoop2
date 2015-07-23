package edu.umn.cs.spatialHadoop.delaunay;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;

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
  /**The coordinates of all sites*/
  double[] x, y;
  /**Neighbors of all sites*/
  IntArray[] neighbors;
  /**Input points given by the user*/
  private Point[] points;
  
  /**
   * Stores a triangulation of some points. All points are referenced by indexes
   * in an array and the array is not stored in this class.
   * @author eldawy
   *
   */
  class Triangulation {
    /**Index of first (inclusive) and last (exclusive) site in the array*/
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
      neighbors[s1].append(s1+1);
      neighbors[s1+1].append(s1);
      if (s2 - s1 == 3) {
        neighbors[s1].append(s1+2);
        neighbors[s1+1].append(s1+2);
        neighbors[s1+2].append(s2);
        neighbors[s1+2].append(s1+1);
        convexHull = new int[] {s1, s1+1, s1+2};
      } else {
        convexHull = new int[] {s1, s1+1};
      }
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
          if (baseL == -1 || (y[p1] <= y[baseL] && y[p2] <= y[baseR]) /*||
              (p1.x <= baseL.x && p2.x <= baseR.x)*/) {
            baseL = p1;
            baseR = p2;
          }
        } else if (inArray(L.convexHull, p2) && inArray(R.convexHull, p1)) {
          if (baseL == -1 || (y[p2] <= y[baseL] && y[p1] <= y[baseR]) /*||
              (p2.x <= baseL.x && p1.x <= baseR.x)*/) {
            baseL = p2;
            baseR = p1;
          }
        }
      }
      
      // Trace the base LR edge up to the top
      boolean finished = false;
      do {
        // Add the base edge to the Delaunay triangulation
        neighbors[baseL].append(baseR);
        neighbors[baseR].append(baseL);
        // Search for the potential candidate on the right
        double anglePotential = -1, angleNextPotential = -1;
        int potentialCandidate = -1, nextPotentialCandidate = -1;
        for (int rNeighbor : neighbors[baseR]) {
          if (rNeighbor >= R.site1 && rNeighbor < R.site2) {
            // Check this RR edge
            double cwAngle = calculateCWAngle(baseL, baseR, rNeighbor);
            if (potentialCandidate == -1 || cwAngle < anglePotential) {
              // Found a new potential candidate
              angleNextPotential = anglePotential;
              nextPotentialCandidate = potentialCandidate;
              anglePotential = cwAngle;
              potentialCandidate = rNeighbor;
            } else if (nextPotentialCandidate == -1 | cwAngle < angleNextPotential) {
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
            double dx = circleCenter.x - x[nextPotentialCandidate];
            double dy = circleCenter.y - y[nextPotentialCandidate];
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - x[potentialCandidate];
            dy = circleCenter.y - y[potentialCandidate];
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
          if (lNeighbor >= L.site1 && lNeighbor < L.site2) {
            // Check this LL edge
            double ccwAngle = Math.PI * 2 - calculateCWAngle(baseR, baseL, lNeighbor);
            if (potentialCandidate == -1 || ccwAngle < anglePotential) {
              // Found a new potential candidate
              angleNextPotential = anglePotential;
              nextPotentialCandidate = potentialCandidate;
              anglePotential = ccwAngle;
              potentialCandidate = lNeighbor;
            } else if (nextPotentialCandidate == -1 | ccwAngle < angleNextPotential) {
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
            double dx = circleCenter.x - x[nextPotentialCandidate];
            double dy = circleCenter.y - y[nextPotentialCandidate];
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - x[potentialCandidate];
            dy = circleCenter.y - y[potentialCandidate];
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
          double dx = circumCircleL.x - x[lCandidate];
          double dy = circumCircleL.y - y[lCandidate];
          double lCandidateDistance = dx * dx + dy * dy;
          dx = circumCircleL.x - x[rCandidate];
          dy = circumCircleL.y - y[rCandidate];
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
        } else if (rCandidate != -1) {
          // Right candidate has been chosen
          // Make baseL and rPotentialCandidate the new base line
          baseR = rCandidate;
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
      for (int s1 = site1; s1 < site2; s1++) {
        for (int s2 : neighbors[s1]) {
          System.out.printf("line %f, %f, %f, %f\n", x[s1], y[s1], x[s2], y[s2]);
        }
      }
    }
    

    boolean inArray(int[] array, int objectToFind) {
      for (int objectToCompare : array)
        if (objectToFind == objectToCompare)
          return true;
      return false;
    }
    
    double calculateCWAngle(int s1, int s2, int s3) {
      double angle1 = Math.atan2(y[s1] - y[s2], x[s1] - x[s2]);
      double angle2 = Math.atan2(y[s3] - y[s2], x[s3] - x[s2]);
      return angle1 > angle2 ? (angle1 - angle2) : (Math.PI * 2 + (angle1 - angle2));
    }

    Point calculateCircumCircleCenter(int s1, int s2, int s3) {
      // Calculate the perpendicular bisector of the first two points
      double x1 = (x[s1] + x[s2]) / 2;
      double y1 = (y[s1] + y[s2]) /2;
      double x2 = x1 + y[s2] - y[s1];
      double y2 = y1 + x[s1] - x[s2];
      // Calculate the perpendicular bisector of the second two points 
      double x3 = (x[s3] + x[s2]) / 2;
      double y3 = (y[s3] + y[s2]) / 2;
      double x4 = x3 + y[s2] - y[s3];
      double y4 = y3 + x[s3] - x[s2];
      
      // Calculate the intersection of the two new lines
      // See https://en.wikipedia.org/wiki/Line%E2%80%93line_intersection
      double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
      double ix = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
      double iy = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
      return new Point(ix, iy);
    }
    
    int[] convexHull(final int[] points) {

      // Sort sites by increasing x-axis
      // It is enough to order them by ID because the original array is sorted
      // by X in the Delaunay triangulation algorithm
      Arrays.sort(points);
      
      // Lower chain
      Stack<Integer> lowerChain = new Stack<Integer>();
      for (int i=0; i < points.length; i++) {
        while(lowerChain.size() > 1) {
          int s1 = lowerChain.get(lowerChain.size() - 2);
          int s2 = lowerChain.get(lowerChain.size() - 1);
          int s3 = points[i];
          double crossProduct = (x[s2] - x[s1]) * (y[s3] - y[s1]) - (y[s2] - y[s1]) * (x[s3] - x[s1]);
          if (crossProduct <= 0) lowerChain.pop();
          else break;
        }
        lowerChain.push(points[i]);
      }
      
      // Upper chain
      Stack<Integer> upperChain = new Stack<Integer>();
      for (int i=points.length - 1; i>=0; i--) {
        while(upperChain.size() > 1) {
          int s1 = upperChain.get(upperChain.size() - 2);
          int s2 = upperChain.get(upperChain.size() - 1);
          int s3 = points[i];
          double crossProduct = (x[s2] - x[s1]) * (y[s3] - y[s1]) - (y[s2] - y[s1]) * (x[s3] - x[s1]);
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
  
  
  public <P extends Point> GuibasStolfiDelaunayAlgorithm(P[] points) {
    // Sort all points by X
    this.points = new Point[points.length];
    System.arraycopy(points, 0, this.points, 0, points.length);
    Arrays.sort(this.points, new Comparator<Point>() {
      @Override
      public int compare(Point s1, Point s2) {
        if (s1.x < s2.x)
          return -1;
        if (s1.x > s2.x)
          return 1;
        if (s1.y < s2.y)
          return -1;
        if (s1.y > s2.y)
          return 1;
        return 0;
      }
    });
    x = new double[this.points.length];
    y = new double[this.points.length];
    neighbors = new IntArray[this.points.length];
    for (int i = 0; i < this.points.length; i++) {
      x[i] = points[i].x;
      y[i] = points[i].y;
      neighbors[i] = new IntArray();
    }
  }
  
  public Triangulation compute() {
    Triangulation[] triangulations = new Triangulation[points.length / 3 + (points.length % 3 == 0 ? 0 : 1)];
    
    // Compute the trivial Delaunay triangles of every three consecutive points
    int i, t=0;
    for (i = 0; i < points.length - 4; i += 3) {
      // Compute Delaunay triangulation for three points
      triangulations[t++] =  new Triangulation(i, i+3);
    }
    if (points.length - i == 4) {
      // Compute Delaunay triangulation for every two points
       triangulations[t++] = new Triangulation(i, i+2);
       triangulations[t++] = new Triangulation(i+2, i+4);
    } else if (points.length - i == 3) {
      // Compute for three points
      triangulations[t++] = new Triangulation(i, i+3);
    } else if (points.length - i == 2) {
      // Two points, connect with a line
      triangulations[t++] = new Triangulation(i, i+2);
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
  
}
