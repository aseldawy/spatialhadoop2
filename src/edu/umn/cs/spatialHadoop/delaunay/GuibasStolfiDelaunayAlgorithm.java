package edu.umn.cs.spatialHadoop.delaunay;

import java.util.Arrays;
import java.util.Comparator;

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

  public <P extends Point> GuibasStolfiDelaunayAlgorithm(P[] points) {
    this.points = new Point[points.length];
    System.arraycopy(points, 0, this.points, 0, points.length);
    sites = new Site[this.points.length];
    for (int i = 0; i < this.points.length; i++)
      sites[i] = new Site(this.points[i].x, this.points[i].y);
    // Sort all points by X
    Arrays.sort(sites, new Comparator<Site>() {
      @Override
      public int compare(Site s1, Site s2) {
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
      s1.neighbors.add(s2);
      s2.neighbors.add(s1);
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
      s1.neighbors.add(s2); s1.neighbors.add(s3);
      s2.neighbors.add(s1); s2.neighbors.add(s3);
      s3.neighbors.add(s1); s3.neighbors.add(s2);
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
      this.convexHull = Utils.convexHull(bothHulls);
      
      // Find the base LR-edge (lowest edge of the convex hull that crosses from L to R)
      Site baseL = null, baseR = null;
      for (int i = 0; i < this.convexHull.length; i++) {
        Site p1 = this.convexHull[i];
        Site p2 = i == this.convexHull.length - 1 ? this.convexHull[0] : this.convexHull[i+1];
        if (Utils.inArray(L.convexHull, p1) && Utils.inArray(R.convexHull, p2)) {
          if (baseL == null || (p1.y <= baseL.y && p2.y <= baseR.y) /*||
              (p1.x <= baseL.x && p2.x <= baseR.x)*/) {
            baseL = p1;
            baseR = p2;
          }
        } else if (Utils.inArray(L.convexHull, p2) && Utils.inArray(R.convexHull, p1)) {
          if (baseL == null || (p2.y <= baseL.y && p1.y <= baseR.y) /*||
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
        baseL.neighbors.add(baseR);
        baseR.neighbors.add(baseL);
        // Search for the potential candidate on the right
        double anglePotential = -1, angleNextPotential = -1;
        Site potentialCandidate = null, nextPotentialCandidate = null;
        for (Site rNeighbor : baseR.neighbors) {
          if (Utils.inArray(R.allSites, rNeighbor)) {
            // Check this RR edge
            double cwAngle = Utils.calculateCWAngle(baseL, baseR, rNeighbor);
            if (potentialCandidate == null || cwAngle < anglePotential) {
              // Found a new potential candidate
              angleNextPotential = anglePotential;
              nextPotentialCandidate = potentialCandidate;
              anglePotential = cwAngle;
              potentialCandidate = rNeighbor;
            } else if (nextPotentialCandidate == null | cwAngle < angleNextPotential) {
              angleNextPotential = cwAngle;
              nextPotentialCandidate = rNeighbor;
            }
          }
        }
        Site rCandidate = null;
        if (anglePotential < Math.PI) {
          if (nextPotentialCandidate != null) {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
            Site circleCenter = Utils.calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
            double dx = circleCenter.x - nextPotentialCandidate.x;
            double dy = circleCenter.y - nextPotentialCandidate.y;
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - potentialCandidate.x;
            dy = circleCenter.y - potentialCandidate.y;
            double d2 = dx * dx + dy * dy;
            if (d1 < d2) {
              // Delete the RR edge between baseR and rPotentialCandidate and restart
              baseR.neighbors.remove(potentialCandidate);
              potentialCandidate.neighbors.remove(baseR);
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
        for (Site lNeighbor : baseL.neighbors) {
          if (Utils.inArray(L.allSites, lNeighbor)) {
            // Check this LL edge
            double ccwAngle = Math.PI * 2 - Utils.calculateCWAngle(baseR, baseL, lNeighbor);
            if (potentialCandidate == null || ccwAngle < anglePotential) {
              // Found a new potential candidate
              angleNextPotential = anglePotential;
              nextPotentialCandidate = potentialCandidate;
              anglePotential = ccwAngle;
              potentialCandidate = lNeighbor;
            } else if (nextPotentialCandidate == null | ccwAngle < angleNextPotential) {
              angleNextPotential = ccwAngle;
              nextPotentialCandidate = lNeighbor;
            }
          }
        }
        Site lCandidate = null;
        if (anglePotential < Math.PI) {
          if (nextPotentialCandidate != null) {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
            Site circleCenter = Utils.calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
            double dx = circleCenter.x - nextPotentialCandidate.x;
            double dy = circleCenter.y - nextPotentialCandidate.y;
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - potentialCandidate.x;
            dy = circleCenter.y - potentialCandidate.y;
            double d2 = dx * dx + dy * dy;
            if (d1 < d2) {
              // Delete the LL edge between baseR and rPotentialCandidate and restart
              baseL.neighbors.remove(potentialCandidate);
              potentialCandidate.neighbors.remove(baseL);
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
          Site circumCircleL = Utils.calculateCircumCircleCenter(lCandidate, baseL, baseR);
          double dx = circumCircleL.x - lCandidate.x;
          double dy = circumCircleL.y - lCandidate.y;
          double lCandidateDistance = dx * dx + dy * dy;
          dx = circumCircleL.x - rCandidate.x;
          dy = circumCircleL.y - rCandidate.y;
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
      for (Site s1 : allSites) {
        for (Site s2 : s1.neighbors) {
          System.out.printf("line %f, %f, %f, %f\n", s1.x, s1.y, s2.x, s2.y);
        }
      }
    }
    
  }
}
