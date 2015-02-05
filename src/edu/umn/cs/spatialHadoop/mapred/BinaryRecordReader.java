/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;



/**
 * Reads every possible pair of values from two underlying record readers.
 * In other words, it makes a Cartesian product from the records stored in
 * the two splits given to this reader.
 * @author eldawy
 *
 * @param <K>
 * @param <V>
 */
public abstract class BinaryRecordReader<K extends Writable, V extends Writable>
    implements RecordReader<PairWritable<K>, PairWritable<V>> {
  
  /**A flag that is set before the first record is read*/
  protected boolean firstTime = true;
  
  
  /**The internal readers that actually do the parsing*/
  protected RecordReader<K, V>[] internalReaders;
  
  /**The two splits parsed by this record reader*/
  protected CombineFileSplit split;
  
  /**Configuration of the current job*/
  protected Configuration conf;
  
  /**Progress of file 1 before reading the current record. Used for progress.*/
  protected float progress1Before;
  
  /**
   * Creates a record reader for one of the two splits parsed by this reader.
   * @param split
   * @return
   */
  protected abstract RecordReader<K, V> createRecordReader(Configuration conf,
      CombineFileSplit split, int index) throws IOException;
  
  @SuppressWarnings("unchecked")
  public BinaryRecordReader(Configuration conf, CombineFileSplit split) throws IOException {
    this.conf = conf;
    this.split = split;
    internalReaders = new RecordReader[(int) split.getNumPaths()];
    // Initialize all record readers
    for (int i = 0; i < split.getNumPaths(); i++) {
      this.internalReaders[i] = createRecordReader(this.conf, this.split, i);
    }
  }
  
  @Override
  public boolean next(PairWritable<K> key, PairWritable<V> value) throws IOException {
    if (firstTime) {
      progress1Before = internalReaders[0].getProgress();
      if (!internalReaders[0].next(key.first, value.first)) {
        return false;
      }
      firstTime = false;
    }
    if (internalReaders[1].next(key.second, value.second)) {
      return true;
    }
    // Reached the end of the second split. Reset the second split and advance
    // to the next item in the first split
    progress1Before = internalReaders[0].getProgress();
    if (!internalReaders[0].next(key.first, value.first)) {
      // Already finished all records in first split
      return false;
    }
    
    // Restart second reader
    internalReaders[1].close();
    internalReaders[1] = createRecordReader(conf, split, 1);
    return internalReaders[1].next(key.second, value.second);
  }

  @Override
  public PairWritable<K> createKey() {
    PairWritable<K> key = new PairWritable<K>();
    key.first = internalReaders[0].createKey();
    key.second = internalReaders[1].createKey();
    return key;
  }

  @Override
  public PairWritable<V> createValue() {
    PairWritable<V> value = new PairWritable<V>();
    value.first = internalReaders[0].createValue();
    value.second = internalReaders[1].createValue();
    return value;
  }

  @Override
  public long getPos() throws IOException {
    return internalReaders[0].getPos() + internalReaders[1].getPos();
  }

  @Override
  public void close() throws IOException {
    internalReaders[0].close();
    internalReaders[1].close();
  }

  @Override
  public float getProgress() throws IOException {
    float progress1 = internalReaders[0].getProgress();
    float progress2 = internalReaders[1].getProgress();
    // Scale 0-9 for most significant digit
    float progress = progress1Before * (1.0f - progress2) + progress1 * progress2;
    return progress;
  }
}
