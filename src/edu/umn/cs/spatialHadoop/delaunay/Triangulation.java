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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * A class to store the output of a triangulation method. It stores a list of
 * input records and a set of edges that represent triangle edges. Each
 * undirected edge is stored only once.
 * @author Ahmed Eldawy
 *
 */
public class Triangulation implements Writable, TextSerializable {
  /**A list of all points in this triangulation.*/
  Point[] sites;
  /**A set of all edges, each connecting two points in the triangulation*/
  int[] edgeStarts, edgeEnds;
  /**Minimum bounding rectangles for all points*/
  Rectangle mbr;
  
  public Triangulation() {}
  
  Triangulation(GSDelaunayAlgorithm algo) {
    this.sites = algo.points.clone();
    int numEdges = 0;
    this.mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (int s1 = 0; s1 < algo.neighbors.length; s1++) {
      numEdges += algo.neighbors[s1].size();
      this.mbr.expand(this.sites[s1]);
    }
    numEdges /= 2; // We store each undirected edge once
    edgeStarts = new int[numEdges];
    edgeEnds = new int[numEdges];

    for (int s1 = 0; s1 < algo.neighbors.length; s1++) {
      for (int s2 : algo.neighbors[s1]) {
        if (s1 < s2) {
          numEdges--;
          edgeStarts[numEdges] = s1;
          edgeEnds[numEdges] = s2;
        }
      }
    }
    if (numEdges != 0)
      throw new RuntimeException("Error in edges! Copied "+
    (edgeStarts.length - numEdges)+" instead of "+edgeStarts.length);
  }
  
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
    this.mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (int oldNodeID = 0; oldNodeID < sites.length; oldNodeID++) {
      if (connectedNodes.get(oldNodeID)) {
        newSites[maxID] = sites[oldNodeID];
        this.mbr.expand(newSites[maxID]);
        newNodeIDs[oldNodeID] = maxID++;
      }
    }
    if (maxID != newSiteCount)
      throw new RuntimeException(String.format("Error in compaction. Copied only %d sites instead of %d", maxID, newSiteCount));
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
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      this.mbr = new Rectangle();
      this.mbr.readFields(in);
      int numSites = in.readInt();
      Class<? extends Point> siteClass = Class.forName(in.readUTF()).asSubclass(Point.class);
      sites = new Point[numSites];
      for (int i = 0; i < numSites; i++) {
        sites[i] = siteClass.newInstance();
        sites[i].readFields(in);
      }
      edgeStarts = IntArray.readIntArray(edgeStarts, in);
      edgeEnds = IntArray.readIntArray(edgeEnds, in);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot find site class", e);
    } catch (InstantiationException e) {
      throw new RuntimeException("Cannot instantiate objects of site class", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot access the constructor of site class", e);
    }
  }
  
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

  static final byte[] SEPARATOR = new byte[] {'\t'};
  /**New line marker to separate records*/
  protected static byte[] NEW_LINE;
  
  static {
    try {
      NEW_LINE = System.getProperty("line.separator", "\n").getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      throw new RuntimeException("Cannot retrieve system line separator", e);
    }
  }  
  
  @Override
  public Text toText(Text text) {
    for (int i = 0; i < edgeStarts.length; i++) {
      // Add a line separator except before first line
      if (i > 0)
        text.append(NEW_LINE, 0, NEW_LINE.length);
      Point startNode = sites[edgeStarts[i]];
      Point endNode = sites[edgeEnds[i]];
      startNode.toText(text);
      text.append(SEPARATOR, 0, SEPARATOR.length);
      endNode.toText(text);
    }
    return text;
  }

  @Override
  public void fromText(Text text) {
    throw new RuntimeException("Not yet implemented");
  }
  
}
