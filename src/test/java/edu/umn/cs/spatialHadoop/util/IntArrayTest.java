package edu.umn.cs.spatialHadoop.util;

import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Unit test for the utility class {@link Head}.
 */
public class IntArrayTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public IntArrayTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(IntArrayTest.class);
  }

  public void testRandomInsert() {
    IntArray array = new IntArray();
    array.add(5);
    array.insert(0, 3);
    assertEquals(2, array.size());
    assertEquals(3, array.get(0));
    assertEquals(5, array.get(1));
  }

}
