package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.MemoryInputStream;
import edu.umn.cs.spatialHadoop.util.IntArray;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Unit test for the STRPartitioner class
 */
public class STRPartitionerTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public STRPartitionerTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(STRPartitionerTest.class);
  }

  public void testSmallFiles() {
    STRPartitioner str = new STRPartitioner();
    str.construct(null, new Point[] {new Point(1, 1), new Point(2, 2)}, 10);
    assertEquals(1, str.getPartitionCount());
    int i = str.overlapPartition(new Rectangle(0, 0, 3, 3));
    assertEquals(0, i);
  }

}
