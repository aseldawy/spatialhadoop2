package org.apache.hadoop.mapred.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**A class that stores a pair of objects where both are writable*/
public class PairWritable<T extends Writable> implements Writable {
  public T first;
  public T second;

  public PairWritable() {}
  
  public PairWritable(T first, T second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }

  @Override
  public String toString() {
    return "<"+first.toString()+", "+second.toString()+">";
  }
}