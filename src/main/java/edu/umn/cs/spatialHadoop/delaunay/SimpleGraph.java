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
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * A class to store the output of Dealaunay Triangulation as a graph. Sites
 * are stored as vertices and Delaunay edges are stored as graph edges.
 * 
 * @author Ahmed Eldawy
 *
 */
public class SimpleGraph implements Writable {
  /**A list of all vertices in this graph.*/
  Point[] sites;
  /**A set of all edges, each connecting two points in the graph*/
  int[] edgeStarts, edgeEnds;
  /**Minimum bounding rectangles for all points*/
  Rectangle mbr;
  /**A safe site is a site that does not participate in any unsafe triangles*/
  BitArray safeSites;

  public SimpleGraph() {
    safeSites = new BitArray();
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
    // Detect which nodes are connected and which are disconnected
    BitArray connectedNodes = new BitArray(sites.length);
    int newSiteCount = 0;
    for (int i = 0; i < edgeStarts.length; i++) {
      if (!connectedNodes.get(edgeStarts[i])) {
        newSiteCount++;
        connectedNodes.set(edgeStarts[i], true);
      }
      if (!connectedNodes.get(edgeEnds[i])) {
        newSiteCount++;
        connectedNodes.set(edgeEnds[i], true);
      }
    }
    
    // Create a mapping from each old node ID to a new node ID.
    // Old node ID is a position in the current (soon to be old) sites array.
    // New node ID is a position in the new (soon to be created) sites array.
    int maxID = 0;
    Point[] newSites = new Point[newSiteCount];
    int[] newNodeIDs = new int[sites.length];
    BitArray newSafeSites = new BitArray(newSiteCount);
    this.mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (int oldNodeID = 0; oldNodeID < sites.length; oldNodeID++) {
      if (connectedNodes.get(oldNodeID)) {
        newSites[maxID] = sites[oldNodeID];
        this.mbr.expand(newSites[maxID]);
        newSafeSites.set(maxID, safeSites.get(oldNodeID));
        newNodeIDs[oldNodeID] = maxID++;
      }
    }
    if (maxID != newSiteCount)
      throw new RuntimeException(String.format("Error in compaction. "
          + "Copied only %d sites instead of %d", maxID, newSiteCount));
    this.sites = newSites;
    
    // Update all edges accordingly
    for (int i = 0; i < edgeStarts.length; i++) {
      edgeStarts[i] = newNodeIDs[edgeStarts[i]];
      edgeEnds[i] = newNodeIDs[edgeEnds[i]];
    }
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
    safeSites.write(out);
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
      safeSites.readFields(in);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot find site class", e);
    } catch (InstantiationException e) {
      throw new RuntimeException("Cannot instantiate objects of site class", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Non-accessbile constructor of site class", e);
    }
  }
  
  /**
   * Draw as SVG Rasem commands.
   */
  public void draw() {
    System.out.println("group {");
    for (Point s : sites) {
      System.out.printf("circle %f, %f, 0.5\n", s.x, s.y);
    }
    System.out.println("}");
    System.out.println("group {");
    for (int i = 0; i < edgeStarts.length; i++) {
      System.out.printf("line %f, %f, %f, %f\n", sites[edgeStarts[i]].x,
          sites[edgeStarts[i]].y, sites[edgeEnds[i]].x, sites[edgeEnds[i]].y);
    }
    System.out.println("}");
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
        if (!safeSites.get(state.siteIndex))
          continue;
        // Found a safe site, load its neighbors
        neighbors.clear();
        for (int iEdge = 0; iEdge < edgeStarts.length; iEdge++) {
          if (edgeStarts[iEdge] == state.siteIndex)
            neighbors.add(edgeEnds[iEdge]);
          if (edgeEnds[iEdge] == state.siteIndex)
            neighbors.add(edgeStarts[iEdge]);
        }

        if (neighbors.size() < 2)
          throw new RuntimeException("A safe site must have at least two neighbors");

        // Sort neighbors in a CCW order to find triangles.
        // Use bubble sort since we do not expect too many neighbors
        final Point center = sites[state.siteIndex];
        for (int i = neighbors.size() - 1; i >= 0 ; i--) {
          for (int j = 0; j < i; j++) {
            // Compare neighbors j and j+1
            final Point a = sites[neighbors.get(j)];
            final Point b = sites[neighbors.get(j+1)];
            // Equation taken from http://stackoverflow.com/questions/6989100/sort-points-in-clockwise-order
            double det = (a.x - center.x) * (b.y - center.y) -
                (b.x - center.x) * (a.y - center.y);
            if (det < 0) {
              // Swap neighbors at i and j
              neighbors.swap(i, j);
            }
          }
        }

        // Search for the first triangle that can be reported
        state.neighborIndex = -1;
        moveToNextNeighbor(state);
        // If the neighbor found is valid, break the loop
        if (state.neighborIndex < neighbors.size())
          break;
      }
      if (state.siteIndex < neighbors.size()) {
        // Store the triangle to report
        state.triangle[0] = sites[state.siteIndex];
        state.triangle[1] = sites[neighbors.get(state.neighborIndex)];
        state.triangle[2] = sites[neighbors.get((state.neighborIndex + 1) % neighbors.size())];
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
     * 2- The head of the triangle (p0) has the smallest index among the three
     *    corners.
     * 3- The angle at the triangle head is less than PI. This is actually to
     *    make sure it is a valid triangle
     * @param p0
     * @param p1
     * @param p2
     * @return
     */
    private boolean canReportTriangle(int p0, int p1, int p2) {
      if (p1 < p0 || p2 < p0)
        return false;
      // Compute the cross product between the vectors
      // a = p1 -> p2
      // b = p1 -> p0
      double a_x = sites[p2].x - sites[p1].x;
      double a_y = sites[p2].y - sites[p1].y;
      double b_x = sites[p0].x - sites[p1].x;
      double b_y = sites[p0].y - sites[p1].y;
      if (a_x * b_y - a_y * b_x < 0)
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
