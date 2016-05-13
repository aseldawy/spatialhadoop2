package edu.umn.cs.spatialHadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for the utility class {@link Head}.
 */
public class HeadTest extends TestCase {
  
  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public HeadTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(HeadTest.class);
  }

  public void testPlainTextFile() {
    try {
      Path inFile = new Path("src/test/resources/test.rect");
      FileSystem fs = inFile.getFileSystem(new Configuration());
      String[] headLines = Head.head(fs, inFile, 2);
      
      assertEquals(2, headLines.length);
      assertEquals("913,16,924,51", headLines[0]);
      assertEquals("953,104,1000.0,116", headLines[1]);
    } catch (Exception e) {
      throw new RuntimeException("Error running test", e);
    }
  }
  
  public void testRTreeFile() {
    try {
      Path inFile = new Path("src/test/resources/test.rtree");
      FileSystem fs = inFile.getFileSystem(new Configuration());
      String[] headLines = Head.head(fs, inFile, 2);
      
      assertEquals(2, headLines.length);
      assertEquals("200,728,210,767", headLines[0]);
      assertEquals("277,145,324,246", headLines[1]);
    } catch (Exception e) {
      throw new RuntimeException("Error running test", e);
    }
  }
}
