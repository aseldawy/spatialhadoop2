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
package edu.umn.cs.spatialHadoop.core;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Progressable;

public interface ShapeRecordWriter<S extends Shape> {
  /**
   * Writes the given shape to the file to all cells it overlaps with
   * @param dummyId
   * @param shape
   * @throws IOException
   */
  public void write(NullWritable dummy, S shape) throws IOException;

  /**
   * Writes the given shape to the specified cell
   * @param cellId
   * @param shape
   */
  public void write(int cellId, S shape) throws IOException;
  
  /**
   * Writes the given shape only to the given cell even if it overlaps
   * with other cells. This is used when the output is prepared to write
   * only one cell. The caller ensures that another call will write the object
   * to the other cell(s) later.
   * @param cellInfo
   * @param shape
   * @throws IOException
   */
  public void write(CellInfo cellInfo, S shape) throws IOException;

  /**
   * Sets a stock object used to serialize/deserialize objects when written to
   * disk.
   * @param shape
   */
  public void setStockObject(S shape);
  
  /**
   * Closes this writer
   * @param reporter
   * @throws IOException
   */
  public void close(Progressable progressable) throws IOException;
}
