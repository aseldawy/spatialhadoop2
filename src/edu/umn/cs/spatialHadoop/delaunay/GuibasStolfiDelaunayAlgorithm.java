package edu.umn.cs.spatialHadoop.delaunay;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import edu.umn.cs.spatialHadoop.core.Point;

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
  
  private Site[] sites;
  private Point[] points;
  private double[] xs, ys;

  public <P extends Point> GuibasStolfiDelaunayAlgorithm(P[] points) {
    this.points = new Point[points.length];
    System.arraycopy(points, 0, this.points, 0, points.length);
    Arrays.sort(this.points);
    sites = new Site[this.points.length];
    this.xs = new double[this.points.length];
    this.ys = new double[this.points.length];
    for (int i = 0; i < this.points.length; i++) {
      xs[i] = this.points[i].x;
      ys[i] = this.points[i].y;
      sites[i] = new Site(i);
    }
  }

  /** Computes the Delaunay triangulation for the points */
  public Triangulation compute() {
    Triangulation[] triangulations = new Triangulation[sites.length / 3 + (sites.length % 3 == 0 ? 0 : 1)];
    // Compute the trivial Delaunay triangles of every three consecutive points
    int i, t=0;
    for (i = 0; i < sites.length - 4; i += 3) {
      // Compute Delaunay triangulation for three points
      triangulations[t++] =  new Triangulation(sites[i], sites[i+1], sites[i+2]);
    }
    if (points.length - i == 4) {
      // Compute Delaunay triangulation for every two points
       triangulations[t++] = new Triangulation(sites[i], sites[i+1]);
       triangulations[t++] = new Triangulation(sites[i+2], sites[i+3]);
    } else if (points.length - i == 3) {
      // Compute for three points
      triangulations[t++] = new Triangulation(sites[i], sites[i+1], sites[i+2]);
    } else if (points.length - i == 2) {
      // Two points, connect with a line
      triangulations[t++] = new Triangulation(sites[i], sites[i+1]);
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
    /**All sites (points) in the triangulation*/
    Site[] allSites;
    /**Sites on the convex null of the triangulation*/
    Site[] convexHull;
    
    /**
     * Initialize a triangulation with two sites only. The triangulation consists
     * of one line connecting the two sites and no triangles at all.
     * @param s1
     * @param s2
     */
    public Triangulation(Site s1, Site s2) {
      allSites = new Site[] {s1, s2};
      s1.neighbors.add(s2.id);
      s2.neighbors.add(s1.id);
      convexHull = allSites;
    }
    
    /**
     * Initialize a triangulation with three sites. The trianglation consists
     * of a single triangles connecting the three points.
     * @param s1
     * @param s2
     * @param s3
     */
    public Triangulation(Site s1, Site s2, Site s3) {
      allSites = new Site[] {s1, s2, s3};
      s1.neighbors.add(s2.id); s1.neighbors.add(s3.id);
      s2.neighbors.add(s1.id); s2.neighbors.add(s3.id);
      s3.neighbors.add(s1.id); s3.neighbors.add(s2.id);
      convexHull = allSites;
    }
    
    /**
     * Constructs a triangulation that merges two existing triangulations.
     * @param L
     * @param R
     */
    public Triangulation(Triangulation L, Triangulation R) {
      // Compute the convex hull of the result
      Site[] bothHulls = new Site[L.convexHull.length + R.convexHull.length];
      System.arraycopy(L.convexHull, 0, bothHulls, 0, L.convexHull.length);
      System.arraycopy(R.convexHull, 0, bothHulls, L.convexHull.length, R.convexHull.length);
      this.convexHull = convexHull(bothHulls);
      
      // Find the base LR-edge (lowest edge of the convex hull that crosses from L to R)
      Site baseL = null, baseR = null;
      for (int i = 0; i < this.convexHull.length; i++) {
        Site p1 = this.convexHull[i];
        Site p2 = i == this.convexHull.length - 1 ? this.convexHull[0] : this.convexHull[i+1];
        if (inArray(L.convexHull, p1) && inArray(R.convexHull, p2)) {
          if (baseL == null || (ys[p1.id] <= ys[baseL.id] && ys[p2.id] <= ys[baseR.id]) /*||
              (p1.x <= baseL.x && p2.x <= baseR.x)*/) {
            baseL = p1;
            baseR = p2;
          }
        } else if (inArray(L.convexHull, p2) && inArray(R.convexHull, p1)) {
          if (baseL == null || (ys[p2.id] <= ys[baseL.id] && ys[p1.id] <= ys[baseR.id]) /*||
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
        baseL.neighbors.add(baseR.id);
        baseR.neighbors.add(baseL.id);
        // Search for the potential candidate on the right
        double anglePotential = -1, angleNextPotential = -1;
        Site potentialCandidate = null, nextPotentialCandidate = null;
        for (int rNeighbor : baseR.neighbors) {
          if (inArray(R.allSites, sites[rNeighbor])) {
            // Check this RR edge
            double cwAngle = calculateCWAngle(baseL, baseR, sites[rNeighbor]);
            if (potentialCandidate == null || cwAngle < anglePotential) {
              // Found a new potential candidate
              angleNextPotential = anglePotential;
              nextPotentialCandidate = potentialCandidate;
              anglePotential = cwAngle;
              potentialCandidate = sites[rNeighbor];
            } else if (nextPotentialCandidate == null | cwAngle < angleNextPotential) {
              angleNextPotential = cwAngle;
              nextPotentialCandidate = sites[rNeighbor];
            }
          }
        }
        Site rCandidate = null;
        if (anglePotential < Math.PI) {
          if (nextPotentialCandidate != null) {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
            Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
            double dx = circleCenter.x - xs[nextPotentialCandidate.id];
            double dy = circleCenter.y - ys[nextPotentialCandidate.id];
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - xs[potentialCandidate.id];
            dy = circleCenter.y - ys[potentialCandidate.id];
            double d2 = dx * dx + dy * dy;
            if (d1 < d2) {
              // Delete the RR edge between baseR and rPotentialCandidate and restart
              baseR.neighbors.remove(potentialCandidate.id);
              potentialCandidate.neighbors.remove(baseR.id);
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
        potentialCandidate = null; nextPotentialCandidate = null;
        for (int lNeighbor : baseL.neighbors) {
          if (inArray(L.allSites, sites[lNeighbor])) {
            // Check this LL edge
            double ccwAngle = Math.PI * 2 - calculateCWAngle(baseR, baseL, sites[lNeighbor]);
            if (potentialCandidate == null || ccwAngle < anglePotential) {
              // Found a new potential candidate
              angleNextPotential = anglePotential;
              nextPotentialCandidate = potentialCandidate;
              anglePotential = ccwAngle;
              potentialCandidate = sites[lNeighbor];
            } else if (nextPotentialCandidate == null | ccwAngle < angleNextPotential) {
              angleNextPotential = ccwAngle;
              nextPotentialCandidate = sites[lNeighbor];
            }
          }
        }
        Site lCandidate = null;
        if (anglePotential < Math.PI) {
          if (nextPotentialCandidate != null) {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
            Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
            double dx = circleCenter.x - xs[nextPotentialCandidate.id];
            double dy = circleCenter.y - ys[nextPotentialCandidate.id];
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - xs[potentialCandidate.id];
            dy = circleCenter.y - ys[potentialCandidate.id];
            double d2 = dx * dx + dy * dy;
            if (d1 < d2) {
              // Delete the LL edge between baseR and rPotentialCandidate and restart
              baseL.neighbors.remove(potentialCandidate.id);
              potentialCandidate.neighbors.remove(baseL.id);
              continue;
            } else {
              lCandidate = potentialCandidate;
            }
          } else {
            lCandidate = potentialCandidate;
          }
        }
        
        // Choose the right candidate
        if (lCandidate != null && rCandidate != null) {
          // Two candidates, choose the correct one
          Point circumCircleL = calculateCircumCircleCenter(lCandidate, baseL, baseR);
          double dx = circumCircleL.x - xs[lCandidate.id];
          double dy = circumCircleL.y - ys[lCandidate.id];
          double lCandidateDistance = dx * dx + dy * dy;
          dx = circumCircleL.x - xs[rCandidate.id];
          dy = circumCircleL.y - ys[rCandidate.id];
          double rCandidateDistance = dx * dx + dy * dy;
          if (lCandidateDistance < rCandidateDistance) {
            // rCandidate is outside the circumcircle, lCandidate is correct
            rCandidate = null;
          } else {
            // rCandidate is inside the circumcircle, lCandidate is incorrect
            lCandidate = null;
          }
        }
        
        if (lCandidate != null) {
          // Left candidate has been chosen
          // Make lPotentialCandidate and baseR the new base line
          baseL = lCandidate;
        } else if (rCandidate != null) {
          // Right candidate has been chosen
          // Make baseL and rPotentialCandidate the new base line
          baseR = rCandidate;
        } else {
          // No candidates, merge finished
          finished = true;
        }
      } while (!finished);
      
      // Merge both L and R
      this.allSites = new Site[L.allSites.length + R.allSites.length];
      System.arraycopy(L.allSites, 0, this.allSites, 0, L.allSites.length);
      System.arraycopy(R.allSites, 0, this.allSites, L.allSites.length, R.allSites.length);
    }

    public void draw() {
      int i =0;
      for (Site s1 : allSites) {
        for (int s2 : s1.neighbors) {
          System.out.printf("line %f, %f, %f, %f\n", xs[s1.id], ys[s1.id], xs[s2], ys[s2]);
          i++;
        }
      }
      System.out.println("Total lines "+i);
    }
    
    public boolean test() {
      final double threshold = 1E-5;
      List<Point> starts = new Vector<Point>();
      List<Point> ends = new Vector<Point>();
      for (Site s1 : allSites) {
        for (int s2 : s1.neighbors) {
          starts.add(new Point(xs[s1.id], ys[s1.id]));
          ends.add(new Point(xs[s2], ys[s2]));
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
          double ix = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
          double iy = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
          if ((ix - x1 > threshold && ix - x2 < -threshold) && (iy - y1 > threshold && iy - y2 < -threshold) &&
              (ix - x3 > threshold && ix - x4 < -threshold) && (iy - y3 > threshold && iy - y4 < -threshold)) {
            System.out.printf("line %f, %f, %f, %f\n", x1, y1, x2, y2);
            System.out.printf("line %f, %f, %f, %f\n", x3, y3, x4, y4);
            System.out.printf("point %f, %f\n", ix, iy);
            throw new RuntimeException("error");
          }
        }
      }
      return true;
    }
  }
  
  boolean inArray(Object[] array, Object objectToFind) {
    for (Object objectToCompare : array)
      if (objectToFind == objectToCompare)
        return true;
    return false;
  }
  
  double calculateCWAngle(Site s1, Site s2, Site s3) {
    double angle1 = Math.atan2(ys[s1.id] - ys[s2.id], xs[s1.id] - xs[s2.id]);
    double angle2 = Math.atan2(ys[s3.id] - ys[s2.id], xs[s3.id] - xs[s2.id]);
    return angle1 > angle2 ? (angle1 - angle2) : (Math.PI * 2 + (angle1 - angle2));
  }

  Point calculateCircumCircleCenter(Site s1, Site s2, Site s3) {
    // Calculate the perpendicular bisector of the first two points
    double x1 = (xs[s1.id] + xs[s2.id]) / 2;
    double y1 = (ys[s1.id] + ys[s2.id]) /2;
    double x2 = x1 + ys[s2.id] - ys[s1.id];
    double y2 = y1 + xs[s1.id] - xs[s2.id];
    // Calculate the perpendicular bisector of the second two points 
    double x3 = (xs[s3.id] + xs[s2.id]) / 2;
    double y3 = (ys[s3.id] + ys[s2.id]) / 2;
    double x4 = x3 + ys[s2.id] - ys[s3.id];
    double y4 = y3 + xs[s3.id] - xs[s2.id];
    
    // Calculate the intersection of the two new lines
    // See https://en.wikipedia.org/wiki/Line%E2%80%93line_intersection
    double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
    double ix = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
    double iy = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
    return new Point(ix, iy);
  }
  
  Site[] convexHull(Site[] points) {
    Stack<Site> lowerChain = new Stack<Site>();
    Stack<Site> upperChain = new Stack<Site>();

    // Sort sites by increasing x-axis
    Arrays.sort(points, new Comparator<Site>() {
      @Override
      public int compare(Site s1, Site s2) {
        if (xs[s1.id] < xs[s2.id])
          return -1;
        if (xs[s1.id] > xs[s2.id])
          return +1;
        if (ys[s1.id] < ys[s2.id])
          return -1;
        if (ys[s1.id] > ys[s2.id])
          return +1;
        return 0;
      }
    });
    
    // Lower chain
    for (int i=0; i<points.length; i++) {
      while(lowerChain.size() > 1) {
        Site s1 = lowerChain.get(lowerChain.size() - 2);
        Site s2 = lowerChain.get(lowerChain.size() - 1);
        Site s3 = points[i];
        double crossProduct = (xs[s2.id] - xs[s1.id]) * (ys[s3.id] - ys[s1.id]) - (ys[s2.id] - ys[s1.id]) * (xs[s3.id] - xs[s1.id]);
        if (crossProduct <= 0) lowerChain.pop();
        else break;
      }
      lowerChain.push(points[i]);
    }
    
    // Upper chain
    for (int i=points.length - 1; i>=0; i--) {
      while(upperChain.size() > 1) {
        Site s1 = upperChain.get(upperChain.size() - 2);
        Site s2 = upperChain.get(upperChain.size() - 1);
        Site s3 = points[i];
        double crossProduct = (xs[s2.id] - xs[s1.id]) * (ys[s3.id] - ys[s1.id]) - (ys[s2.id] - ys[s1.id]) * (xs[s3.id] - xs[s1.id]);
        if (crossProduct <= 0) upperChain.pop();
        else break;
      }
      upperChain.push(points[i]);
    }
    
    lowerChain.pop();
    upperChain.pop();
    lowerChain.addAll(upperChain);
    return lowerChain.toArray(new Site[lowerChain.size()]);    
  }
}
