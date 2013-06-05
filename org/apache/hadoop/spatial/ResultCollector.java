package org.apache.hadoop.spatial;

/**
 * Used to collect results of unary operators
 * @author eldawy
 *
 */
public interface ResultCollector<R> {
  public void collect(R r);
}
