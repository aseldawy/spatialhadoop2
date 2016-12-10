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
    /**The index of the current site.
     * A value equal to sites.length indicates that the iterator has finished.
     */
    protected int currentSiteIndex;

    /**
     * Index of the next site to be considered. Needed to implement hasNext
     * without having to search for final sites each time it is called.
     */
    protected int nextSiteIndex;

    /**Index of the current neighbor (triangle) being considered*/
    protected int currentNeighborIndex;

    /**Index of the next neighbor to be considered. The value is equal to
     * neighbors.length if the last neighbor for the current site is reached.*/
    protected int nextNeighborIndex;

    /**Neighbors of the current site*/
    protected IntArray neighbors;

    /**Always points to the current triangle*/
    protected Point[] currentTriangle;

    public TriangleIterable() {
      this.currentSiteIndex = -1;
      this.currentTriangle = new Point[3];
      neighbors = new IntArray();
      // Initialize at the first site
      nextSiteIndex = 0;
      while (nextSiteIndex < sites.length && !safeSites.get(nextSiteIndex))
        nextSiteIndex++;
    }

    /**
     * Skips the iterator to the next site and initialize the list of neighbors
     * for the new site.
     */
    private void skipToNextSite() {
      if (nextSiteIndex >= sites.length)
        return;

      // Move pointer to next safe site
      currentSiteIndex = nextSiteIndex;

      // Advance nextSiteIndex to the next safe site to be able to answer hasNext

      do {
        nextSiteIndex++;
      } while (nextSiteIndex < sites.length && !safeSites.get(nextSiteIndex));

      // Load all its neighbors of the current site
      neighbors.clear();
      for (int iEdge = 0; iEdge < edgeStarts.length; iEdge++) {
        if (edgeStarts[iEdge] == currentSiteIndex)
          neighbors.add(edgeEnds[iEdge]);
        if (edgeEnds[iEdge] == currentSiteIndex)
          neighbors.add(edgeStarts[iEdge]);
      }

      if (neighbors.size() < 2)
        throw new RuntimeException("A final site must have at least one triangles");

      // Sort neighbors in a clock-wise order to find triangles
      // Use bubble sort since we do not expect too many neighbors
      final Point center = currentTriangle[1] = sites[currentSiteIndex];
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

      // Set nextNeighborIndex to the first neighbor with a valid triangle
      currentNeighborIndex = -1;
      nextNeighborIndex = -1;
      boolean nextNeighborIsValid = false;
      while (!nextNeighborIsValid && ++nextNeighborIndex < neighbors.size()) {
        currentTriangle[2] = sites[neighbors.get(nextNeighborIndex)];
        currentTriangle[0] = sites[neighbors.get((nextNeighborIndex + 1) % neighbors.size())];
        nextNeighborIsValid = corssProduct(currentTriangle) > 0;
      }

      if (!nextNeighborIsValid)
        throw new RuntimeException("Error! Found a valid site with no valid triangles");
    }

    @Override
    public Iterator<Point[]> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return nextNeighborIndex < neighbors.size() || nextSiteIndex < sites.length;
    }

    @Override
    public Point[] next() {
      if (nextNeighborIndex >= neighbors.size())
        skipToNextSite();

      // Skip to next neighbor
      currentNeighborIndex = nextNeighborIndex;

      // Set nextNeighborIndex to the next valid neighbor
      boolean nextNeighborIsValid = false;
      while (!nextNeighborIsValid && ++nextNeighborIndex < neighbors.size()) {
        currentTriangle[2] = sites[neighbors.get(nextNeighborIndex)];
        currentTriangle[0] = sites[neighbors.get((nextNeighborIndex + 1) % neighbors.size())];
        nextNeighborIsValid = corssProduct(currentTriangle) > 0;
      }

      // Set the corners of the current triangle
      currentTriangle[2] = sites[neighbors.get(currentNeighborIndex)];
      currentTriangle[0] = sites[neighbors.get((currentNeighborIndex + 1) % neighbors.size())];

      return currentTriangle;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not implemented");
    }
  }

  /**
   * Calculate the cross product of the two vectors (a x b)
   * a = p1 -> p2
   * b = p1 -> p0
   * @param p
   * @return
   */
  private static double corssProduct(Point[] p) {
    double a_x = p[2].x - p[1].x;
    double a_y = p[2].y - p[1].y;
    double b_x = p[0].x - p[1].x;
    double b_y = p[0].y - p[1].y;
    return a_x * b_y - a_y * b_x;
  }


  Iterable<Point[]> iterateTriangles() {
   return new TriangleIterable();
  }
}
