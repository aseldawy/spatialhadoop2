package edu.umn.cs.spatialHadoop.core;

/**
 * Used to collect results of unary operators
 * @author eldawy
 *
 */
public interface ResultCollector<R> {
  public void collect(R r);
}
