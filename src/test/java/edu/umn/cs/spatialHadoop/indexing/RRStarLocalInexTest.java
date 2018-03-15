package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Point;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Unit test for the RTreeGuttman class
 */
public class RRStarLocalInexTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public RRStarLocalInexTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(RRStarLocalInexTest.class);
  }

  public void testIndexWriteRead() {
    try {
      RRStarLocalIndex<Point> lindex = new RRStarLocalIndex<Point>();
      File heapFile = new File("src/test/resources/test.points");
      Path indexFile = new Path("tempout");
      Configuration conf = new Configuration();
      lindex.setup(conf);
      lindex.buildLocalIndex(heapFile, indexFile, new Point());

      lindex = new RRStarLocalIndex<Point>();
      lindex.setup(conf);
      FileSystem fs = indexFile.getFileSystem(conf);
      FSDataInputStream in = fs.open(indexFile);
      long len = fs.getFileStatus(indexFile).getLen();
      lindex.read(in, 0, len, new Point());
      Iterable<? extends Point> results = lindex.search(0, 0, 5, 5);
      int count = 0;
      for (Point p : results)
        count++;
      assertEquals(2, count);

      results = lindex.scanAll();
      count = 0;
      for (Point p : results)
        count++;
      assertEquals(11, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error writing or reading the index");
    }
  }

}
