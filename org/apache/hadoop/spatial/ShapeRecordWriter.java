package org.apache.hadoop.spatial;

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
