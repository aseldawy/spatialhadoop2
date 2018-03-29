package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A class that reads and processes files stored using a local index.
 */
public interface LocalIndex<S extends Shape> extends Closeable {

  /**The name of the configuration line that stores the local index class name*/
  String LocalIndexClass = "LocalIndex.LocalIndexClass";

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @interface LocalIndexMetadata {
    /**The extension used with the locally indexed files*/
    String extension();
  }

  /**
   * Initializes the local indexer with a configuration
   * @param conf
   */
  void setup(Configuration conf);

  /**
   * Build a local index for a file that is not yet indexed.
   * @param nonIndexedFile - path to the file that contains input records.
   *   It is formatted as a text file with one record per line.
   *   The input file is in the local file system (not in HDFS).
   * @param outputIndexedFile - path to the file that will contain the indexed
   *   file. The output file might be in HDFS.
   * @param shape - The shape that is stored in the input file.
   * @throws IOException
   * @throws InterruptedException
   */
  void buildLocalIndex(File nonIndexedFile, Path outputIndexedFile, S shape)
      throws IOException, InterruptedException;

  /**
   * Get the starting offset of the data part
   * @return
   */
  long getDataStart();

  /**
   * Get the offset of the first byte right after the data. The data size should
   * be {@link #getDataEnd()} - {@link #getDataStart()}
   * @return
   */
  long getDataEnd();

  /**
   * Points the local index to an input stream
   * @param in
   * @param start
   * @param end
   */
  void read(FSDataInputStream in, long start, long end, S shape) throws IOException;

  /**
   * Searches for all records that overlap the given query range.
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @return
   */
  Iterable<? extends S> search(double x1, double y1, double x2, double y2);

  /**
   * Scans all records in the local index as if there is no index.
   * @return
   */
  Iterable<? extends S> scanAll();
}
