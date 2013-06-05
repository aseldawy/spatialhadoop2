package edu.umn.cs.spatialHadoop.core;

import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.io.TextSerializable;

/**
 * A general 2D shape.
 * @author aseldawy
 *
 */
public interface Shape extends Writable, Cloneable, TextSerializable {
  /**
   * Returns minimum bounding rectangle for this shape.
   * @return
   */
  public Rectangle getMBR();
  
  /**
   * Gets the distance of this shape to the given point.
   * @param x
   * @param y
   * @return
   */
  public double distanceTo(double x, double y);
  
  /**
   * Returns true if this shape is intersected with the given shape
   * @param s
   * @return
   */
  public boolean isIntersected(final Shape s);
  
  /**
   * Returns a clone of this shape
   * @return
   * @throws CloneNotSupportedException
   */
  public Shape clone();
}
