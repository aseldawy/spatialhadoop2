/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;

/**
 * A record reader for objects of class {@link Shape}
 * @author Ahmed Eldawy
 *
 */
public class ShapeRecordReader<S extends Shape>
    extends SpatialRecordReader<Rectangle, S> {
  
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(ShapeRecordReader.class);

  /**Object used for deserialization*/
  private S stockShape;

  public ShapeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    stockShape = (S) SpatialSite.createStockShape(job);
  }

  public ShapeRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    stockShape = (S) SpatialSite.createStockShape(conf);
  }
  
  public ShapeRecordReader(InputStream in, long offset, long endOffset)
      throws IOException {
    super(in, offset, endOffset);
  }

  @Override
  public boolean next(Rectangle key, S shape) throws IOException {
    boolean read_line = nextShape(shape);
    key.set(cellMbr);
    return read_line;
  }

  @Override
  public Rectangle createKey() {
    return new Rectangle();
  }

  @Override
  public S createValue() {
    return stockShape;
  }
}
