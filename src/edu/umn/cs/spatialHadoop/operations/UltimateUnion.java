package edu.umn.cs.spatialHadoop.operations;

import java.util.Collection;

import com.vividsolutions.jts.geom.Geometry;

public class UltimateUnion {

  /**
   * Computes the union between the given shape with all overlapping shapes
   * and return only the segments in the result that overlap with the shape.
   * 
   * @param shape
   * @param overlappingShapes
   * @return
   */
  public static Geometry partialUnion(Geometry shape, Collection<Geometry> overlappingShapes) {
    return null;
  }
}
