package edu.umn.cs.spatialHadoop.delaunay;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;

/**
 * Stores a triangulation of some points. All points are referenced by indexes
 * in an array and the array is not stored in this class.
 * @author eldawy
 *
 */
public class Triangulation {
  /**All sites (points) in the triangulation*/
  Site[] allSites;
  /**Sites on the convex null of the triangulation*/
  Site[] convexHull;
  
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
        if (baseL == null || (p1.y <= baseL.y && p2.y <= baseR.y) /*||
            (p1.x <= baseL.x && p2.x <= baseR.x)*/) {
          baseL = p1;
          baseR = p2;
        }
      } else if (inArray(L.convexHull, p2) && inArray(R.convexHull, p1)) {
        if (baseL == null || (p2.y <= baseL.y && p1.y <= baseR.y) /*||
            (p2.x <= baseL.x && p1.x <= baseR.x)*/) {
          baseL = p2;
          baseR = p1;
        }
      }
    }
    
    // Trace the base LR edge up to the root
    boolean finished = false;
    do {
      // Add the current base edge to the Delaunay triangulation
      baseL.neighbors.add(baseR);
      baseR.neighbors.add(baseL);
      
      // Search for the potential candidate on the right
      double anglePotential = -1, angleNextPotential = -1;
      Site potentialCandidate = null, nextPotentialCandidate = null;
      for (Site rNeighbor : baseR.neighbors) {
        if (inArray(R.allSites, rNeighbor)) {
          // Check this RR edge
          double cwAngle = calculateCWAngle(baseL, baseR, rNeighbor);
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
          Site circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
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
        if (inArray(L.allSites, lNeighbor)) {
          // Check this LL edge
          double ccwAngle = Math.PI * 2 - calculateCWAngle(baseR, baseL, lNeighbor);
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
          Site circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
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
        Site circumCircleL = calculateCircumCircleCenter(lCandidate, baseL, baseR);
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
  
  public Triangulation(Site s1, Site s2) {
    allSites = new Site[] {s1, s2};
    s1.neighbors.add(s2);
    s2.neighbors.add(s1);
    convexHull = allSites;
  }
  
  public Triangulation(Site s1, Site s2, Site s3) {
    allSites = new Site[] {s1, s2, s3};
    s1.neighbors.add(s2); s1.neighbors.add(s3);
    s2.neighbors.add(s1); s2.neighbors.add(s3);
    s3.neighbors.add(s1); s3.neighbors.add(s2);
    convexHull = allSites;
  }
  
  public void draw() {
    for (Site s1 : allSites) {
      for (Site s2 : s1.neighbors) {
        System.out.printf("line %f, %f, %f, %f\n", s1.x, s1.y, s2.x, s2.y);
      }
    }
  }
  
  private static boolean inArray(Object[] array, Object objectToFind) {
    for (Object objectToCompare : array)
      if (objectToFind == objectToCompare)
        return true;
    return false;
  }
  
  private static double calculateCWAngle(Site s1, Site s2, Site s3) {
    double angle1 = Math.atan2(s1.y - s2.y, s1.x - s2.x);
    double angle2 = Math.atan2(s3.y - s2.y, s3.x - s2.x);
    return angle1 > angle2 ? (angle1 - angle2) : (Math.PI * 2 + (angle1 - angle2));
  }

  private static Site calculateCircumCircleCenter(Site s1, Site s2, Site s3) {
    // Calculate the perpendicular bisector of the first two points
    double x1 = (s1.x + s2.x) / 2;
    double y1 = (s1.y + s2.y) /2;
    double x2 = x1 + s2.y - s1.y;
    double y2 = y1 + s1.x - s2.x;
    // Calculate the perpendicular bisector of the second two points 
    double x3 = (s3.x + s2.x) / 2;
    double y3 = (s3.y + s2.y) / 2;
    double x4 = x3 + s2.y - s3.y;
    double y4 = y3 + s3.x - s2.x;
    
    // Calculate the intersection of the two new lines
    // See https://en.wikipedia.org/wiki/Line%E2%80%93line_intersection
    double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
    double ix = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
    double iy = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
    return new Site(ix, iy);
  }
  
  public static Site[] convexHull(Site[] points) {
    Stack<Site> lowerChain = new Stack<Site>();
    Stack<Site> upperChain = new Stack<Site>();

    // Sort sites by increasing x-axis
    Arrays.sort(points, new Comparator<Site>() {
      @Override
      public int compare(Site s1, Site s2) {
        if (s1.x < s2.x)
          return -1;
        if (s1.x > s2.x)
          return +1;
        if (s1.y < s2.y)
          return -1;
        if (s1.y > s2.y)
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
        double crossProduct = (s2.x - s1.x) * (s3.y - s1.y) - (s2.y - s1.y) * (s3.x - s1.x);
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
        double crossProduct = (s2.x - s1.x) * (s3.y - s1.y) - (s2.y - s1.y) * (s3.x - s1.x);
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