package edu.umn.cs.spatialHadoop.core;

/**
 * Used to collect the output of binary operators.
 * @author eldawy
 *
 */
public interface ResultCollector2<R, S> {
  public void collect(R r, S s);
}
