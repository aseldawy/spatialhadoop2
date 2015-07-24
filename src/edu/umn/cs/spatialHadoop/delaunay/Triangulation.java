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

import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * A class to store the output of a triangulation method. It stores a list of
 * input records and a set of edges that represent triangle edges. Each
 * undirected edge is stored only once.
 * @author Ahmed Eldawy
 *
 */
public class Triangulation implements Writable {
  Point[] sites;
  int[] edgeStarts, edgeEnds;
  
  public Triangulation() {}
  
  Triangulation(GuibasStolfiDelaunayAlgorithm algo) {
    this.sites = algo.points.clone();
    int numEdges = 0;
    for (int s1 = 0; s1 < algo.neighbors.length; s1++) {
      numEdges += algo.neighbors[s1].size();
    }
    numEdges /= 2; // We store each undirected edge once
    edgeStarts = new int[numEdges];
    edgeEnds = new int[numEdges];

    for (int s1 = 0; s1 < algo.neighbors.length; s1++) {
      for (int s2 : algo.neighbors[s1]) {
        if (s1 < s2) {
          numEdges--;
          edgeStarts[numEdges] = s1;
          edgeEnds[numEdges] = s1;
        }
      }
    }
    if (numEdges != 0)
      throw new RuntimeException("Error in edges");
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
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
  
}
