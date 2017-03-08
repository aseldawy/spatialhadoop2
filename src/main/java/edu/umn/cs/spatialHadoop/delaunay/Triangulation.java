/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.delaunay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

/**
 * A class to store a Triangulation for a set of points as a graph. Sites
 * are stored as vertices and triangle edges are stored as graph edges.
 * 
 * @author Ahmed Eldawy
 *
 */
public class Triangulation implements Writable {
  /**A list of all vertices in this graph.*/
  Point[] sites;
  /**A set of all edges, each connecting two points in the graph*/
  int[] edgeStarts, edgeEnds;
  /**Minimum bounding rectangles for all points*/
  Rectangle mbr;
  /**A bitmask for all sites that can be reported*/
  BitArray sitesToReport;
  /**A bitmask for all sites that have been previously reported*/
  BitArray reportedSites;

  public Triangulation() {
    sitesToReport = new BitArray();
    reportedSites = new BitArray();
    mbr = new Rectangle();
  }
  
  /**
   * Return number of sites (vertices)
   * @return
   */
  public int getNumSites() {
    return sites.length;
  }
  
  /**
   * Remove all unnecessary nodes.
   */
  void compact() {
    // Skip compaction if we have one site only as it should not be removed
    // even though it does not have any neighbors.
    if (this.sites.length == 1)
      return;
    // Detect which nodes are connected and which are disconnected
    BitArray connectedNodes = new BitArray(sites.length);
    int newSiteCount = 0;
    for (int i = 0; i < edgeStarts.length; i++) {
      if (!connectedNodes.get(edgeStarts[i])) {
        newSiteCount++;
        connectedNodes.set(edgeStarts[i], true);
      }
    }
    
    // Create a mapping from each old node ID to a new node ID.
    // Old node ID is a position in the current (soon to be old) sites array.
    // New node ID is a position in the new (soon to be created) sites array.
    int maxID = 0;
    Point[] newSites = new Point[newSiteCount];
    int[] newNodeIDs = new int[sites.length];
    BitArray newSitesToReport = new BitArray(newSiteCount);
    BitArray newReportedSites = new BitArray(newSiteCount);
    this.mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (int oldNodeID = 0; oldNodeID < sites.length; oldNodeID++) {
      if (connectedNodes.get(oldNodeID)) {
        newSites[maxID] = sites[oldNodeID];
        this.mbr.expand(newSites[maxID]);
        newSitesToReport.set(maxID, sitesToReport.get(oldNodeID));
        newReportedSites.set(maxID, reportedSites.get(oldNodeID));
        newNodeIDs[oldNodeID] = maxID++;
      }
    }
    if (maxID != newSiteCount)
      throw new RuntimeException(String.format("Error in compaction. "
          + "Copied only %d sites instead of %d", maxID, newSiteCount));
    // Update all edges accordingly
    // Notice that the number of edges is not changed because we only delete
    // nodes that do not have any edges
    for (int i = 0; i < edgeStarts.length; i++) {
      this.edgeStarts[i] = newNodeIDs[edgeStarts[i]];
      this.edgeEnds[i] = newNodeIDs[edgeEnds[i]];
    }

    this.sites = newSites;
    this.reportedSites = newReportedSites;
    this.sitesToReport = newSitesToReport;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    this.mbr.write(out);
    out.writeInt(sites.length);
    out.writeUTF(sites[0].getClass().getName());
    for (Point site : sites)
      site.write(out);
    IntArray.writeIntArray(edgeStarts, out);
    IntArray.writeIntArray(edgeEnds, out);
    sitesToReport.write(out);
    reportedSites.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      this.mbr.readFields(in);
      int numSites = in.readInt();
      Class<? extends Point> siteClass =
          Class.forName(in.readUTF()).asSubclass(Point.class);
      sites = new Point[numSites];
      for (int i = 0; i < numSites; i++) {
        sites[i] = siteClass.newInstance();
        sites[i].readFields(in);
      }
      edgeStarts = IntArray.readIntArray(edgeStarts, in);
      edgeEnds = IntArray.readIntArray(edgeEnds, in);
      sitesToReport.readFields(in);
      reportedSites.readFields(in);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot find site class", e);
    } catch (InstantiationException e) {
      throw new RuntimeException("Cannot instantiate objects of site class", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Non-accessbile constructor of site class", e);
    }
  }

  /**
   * Make an exact replica of the triangulation. This is needed for the reducer
   * to work as it needs to merge all triangulations.
   * @return
   */
  @Override
  protected Triangulation clone() {
    Triangulation replica = new Triangulation();
    replica.sites = this.sites.clone(); // Deep clone is not necessary
    replica.edgeStarts = this.edgeStarts.clone();
    replica.edgeEnds = this.edgeEnds.clone();
    replica.mbr = this.mbr.clone();
    replica.sitesToReport = this.sitesToReport.clone();
    replica.reportedSites = this.reportedSites.clone();
    return replica;
  }

  public void draw(PrintStream out) {
    double scale = 1000 / Math.max(mbr.getWidth(), mbr.getHeight());
    this.draw(out, mbr, scale);
  }

    /**
     * Draw as SVG Rasem commands.
     */
  public void draw(PrintStream out, Rectangle mbr, double scale) {
    out.println("group {");
    Text text = new Text();
    for (Point s : sites) {
      text.clear();
      out.printf("circle %f, %f, 0.5 # %s\n", (s.x - mbr.x1) * scale,
          (s.y - mbr.y1) * scale, s.toText(text).toString());
    }
    out.println("}");
    out.println("group {");
    for (int i = 0; i < edgeStarts.length; i++) {
      if (edgeStarts[i] < edgeEnds[i])
        out.printf("line %f, %f, %f, %f\n",
            (sites[edgeStarts[i]].x - mbr.x1) * scale,
            (sites[edgeStarts[i]].y - mbr.y1) * scale,
            (sites[edgeEnds[i]].x - mbr.x1) * scale,
            (sites[edgeEnds[i]].y - mbr.y1) * scale);
    }
    out.println("}");
  }

  /**
   * Sort edges lexicographically by edgeStart and edgeEnd to be able to use
   * binary search when finding all neighbors of a specific node.
   */
  public void sortEdges() {
    QuickSort sorter = new QuickSort();
    IndexedSortable sortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        if (edgeStarts[i] != edgeStarts[j])
          return edgeStarts[i] - edgeStarts[j];
        return edgeEnds[i]  - edgeEnds[j];
      }

      @Override
      public void swap(int i, int j) {
        int t = edgeStarts[i];
        edgeStarts[i] = edgeStarts[j];
        edgeStarts[j] = t;

        t = edgeEnds[i];
        edgeEnds[i] = edgeEnds[j];
        edgeEnds[j] = t;
      }
    };

    if (edgeStarts.length > 0)
      sorter.sort(sortable, 0, edgeStarts.length);
  }

  /**
   * Make this the final triangulation by marking all sites that have never
   * been reported as final. This method should be called when there is only one
   * triangulation that will not be merged with any other triangulations to
   * ensure that all sites will be reported correct.
   */
  public void  makeFinal() {
    sitesToReport = reportedSites.invert();
  }

  class TriangleIterable implements Iterable<Point[]>, Iterator<Point[]> {
    /**
     * Stores a pointer to a state and a specific neighbor in that state.
     */
    class Pointer {
      /**The index of the site*/
      int siteIndex;
      /**The index of the neighbor in the sorted array of neighbors*/
      int neighborIndex;
      /**
       * The triangle pointed with the current pointer. The triangle has the
       * following three corners
       * 1. site[siteIndex]
       * 2. site[neighbors[neighborIndex]]
       * 3. site[neighbors[neighborIndex + 1 (mod neighbors.length)]]
       */
      Point[] triangle = new Point[3];

      public void copyFrom(Pointer other) {
        this.siteIndex = other.siteIndex;
        this.neighborIndex = other.neighborIndex;
        this.triangle[0] = other.triangle[0];
        this.triangle[1] = other.triangle[1];
        this.triangle[2] = other.triangle[2];
      }
    }

    /**
     * Store two separate states, currentState where the iterator points
     * and nextState where the iterator will point after calling next().
     * We have to store these two separate pointers to ensure that we can
     * execute hasNext() operation efficiently
     */
    protected Pointer currentState, nextState;

    /**
     * A sorted array of neighbors in a CCW order for next site.
     */
    protected IntArray neighbors;

    public TriangleIterable() {
      this.currentState = new Pointer();
      this.nextState = new Pointer();
      neighbors = new IntArray();
      // Initialize at the first site
      nextState.siteIndex = -1;
      moveToNextSite(nextState);
    }

    /**
     * Finds the first pointer where a triangle can be reported. A triangle
     * can be reported at a specific pointer if the following conditions hold.
     * 1. The site pointed by the pointer is safe.
     * 2. The two consecutive neighbors pointed by the neighbor index form
     *    an angle that is less than PI, i.e., they actually form a triangle
     * 3. The site pointed by the pointer has the smallest index in the three
     *    corners. This is to ensure that each triangle is reported exactly once
     * @param state
     */
    private void moveToNextSite(Pointer state) {
      while (++state.siteIndex < sites.length) {
        // Skip if the site is not safe
        if (!sitesToReport.get(state.siteIndex))
          continue;
        // Found a safe site, load its neighbors
        neighbors.clear();
        int edge = Arrays.binarySearch(edgeStarts, state.siteIndex);

        if (edge < 0)
          continue;
        //  throw new RuntimeException(String.format(
        //      "No neighbors found for safe site (%f, %f)",
        //      sites[state.siteIndex].x, sites[state.siteIndex].y));

        for (int iEdge = edge; iEdge < edgeStarts.length && edgeStarts[iEdge] == state.siteIndex; iEdge++)
          neighbors.add(edgeEnds[iEdge]);
        for (int iEdge = edge-1; iEdge >= 0 && edgeStarts[iEdge] == state.siteIndex; iEdge--)
          neighbors.add(edgeEnds[iEdge]);

        // Sort neighbors in a CCW order to find triangles.
        // http://stackoverflow.com/questions/6989100/sort-points-in-clockwise-order
        final Point center = sites[state.siteIndex];
        Comparator<Point> ccw_comparator = new Comparator<Point>() {
          @Override
          public int compare(Point a, Point b) {
            if (a.x - center.x >= 0 && b.x - center.x < 0)
              return 1;
            if (a.x - center.x < 0 && b.x - center.x >= 0)
              return -1;
            if (a.x - center.x == 0 && b.x - center.x == 0)
              return Double.compare(b.y - center.y, a.y - center.y);

            // compute the cross product of vectors (center -> a) x (center -> b)
            double det = (a.x - center.x) * (b.y - center.y) - (b.x - center.x) * (a.y - center.y);
            if (det < 0)
              return -1;
            if (det > 0)
              return 1;
            return 0;
          }
        };
        // Use bubble sort since we do not expect too many neighbors
        for (int i = neighbors.size() - 1; i >= 0 ; i--) {
          for (int j = 0; j < i; j++) {
            // Compare neighbors j and j+1
            final Point a = sites[neighbors.get(j)];
            final Point b = sites[neighbors.get(j+1)];
            if (ccw_comparator.compare(a, b) > 0)
              neighbors.swap(j, j+1);
          }
        }

        // Search for the first triangle that can be reported
        state.neighborIndex = -1;
        moveToNextNeighbor(state);
        // If the neighbor found is valid, break the loop
        if (state.neighborIndex < neighbors.size())
          break;
      }
    }

    /**
     * Moves the given pointer to the next neighbor where a triangle can be
     * reported.
     * @param state
     */
    private void moveToNextNeighbor(Pointer state) {
      boolean canReportTriangle = false;
      while (!canReportTriangle && ++state.neighborIndex < neighbors.size()) {
        canReportTriangle = canReportTriangle(
            state.siteIndex,
            neighbors.get(state.neighborIndex),
            neighbors.get((state.neighborIndex + 1) % neighbors.size()));
      }
      if (canReportTriangle) {
        // Store the triangle to report
        state.triangle[0] = sites[state.siteIndex];
        state.triangle[1] = sites[neighbors.get(state.neighborIndex)];
        state.triangle[2] = sites[neighbors.get((state.neighborIndex + 1) % neighbors.size())];
      }
    }


    @Override
    public Iterator<Point[]> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return nextState.siteIndex < sites.length;
    }

    @Override
    public Point[] next() {
      // Copy nextState into currentState
      currentState.copyFrom(nextState);

      // Advance nextState to point to the next triangle
      moveToNextNeighbor(nextState);
      if (nextState.neighborIndex >= neighbors.size())
        moveToNextSite(nextState);

      return currentState.triangle;
    }

    /**
     * Returns true if an only if the given triangle should be reported.
     * A triangle is reported iff the following conditions hold:
     * 1- The head of the triangle (p0) is a safe site
     * 2- The head of the triangle (p0) has the smallest index among the safe
     *    sites of the three corners.
     * 3- The angle at the triangle head is less than PI. This is actually to
     *    make sure it is a valid triangle
     * @param p0 Index of the first corner (head) of the triangle
     * @param p1 Index of the second corner
     * @param p2 Index of the third corner
     * @return
     */
    private boolean canReportTriangle(int p0, int p1, int p2) {
      // To report the triangle, the current head (p0) should be the one with
      // the least index. This is to ensure reporting a triangle exactly once
      if (p1 < p0 || p2 < p0)
        return false;
      // Compute the cross product between the vectors
      // a = p0 -> p1
      // b = p0 -> p2
      // If the cross product is positive, it indicates that the angle at p0
      // is less than PI, hence the three points represent a valid triangle
      // Otherwise, if the cross produce is negative, it indicates an angle
      // larger than PI (or less than zero) which indicates an invalid triangle
      double a_x = sites[p2].x - sites[p0].x;
      double a_y = sites[p2].y - sites[p0].y;
      double b_x = sites[p1].x - sites[p0].x;
      double b_y = sites[p1].y - sites[p0].y;
      if (a_x * b_y - a_y * b_x <= 0)
        return false;
      return true;
    }


    @Override
    public void remove() {
      throw new RuntimeException("Not implemented");
    }
  }

  Iterable<Point[]> iterateTriangles() {
   return new TriangleIterable();
  }
}
