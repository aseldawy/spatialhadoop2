package edu.umn.cs.spatialHadoop.io;

import edu.umn.cs.spatialHadoop.OperationsParams;
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

  /**A scratch area used to do all the tests which gets wiped at the end*/
  protected Path scratchPath = new Path("testindex");

  @Override
  protected void tearDown() throws Exception {
    OperationsParams params = new OperationsParams();
    FileSystem fs = scratchPath.getFileSystem(params);
    fs.delete(scratchPath, true);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    OperationsParams params = new OperationsParams();
    FileSystem fs = scratchPath.getFileSystem(params);
    if (fs.exists(scratchPath))
      fs.delete(scratchPath, true);
    if (!fs.exists(scratchPath))
      fs.mkdirs(scratchPath);
  }


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
      e.printStackTrace();
      fail("Error in test");
    }
  }

  public void testDirectory() {
    try {
      Path inFile = new Path("src/test/resources/test.rect");
      Path outFile = new Path(scratchPath, "testfile");
      FileSystem fs = inFile.getFileSystem(new Configuration());
      fs.copyFromLocalFile(inFile, outFile);
      String[] headLines = Head.head(fs, scratchPath, 2);

      assertEquals(2, headLines.length);
      assertEquals("913,16,924,51", headLines[0]);
      assertEquals("953,104,1000.0,116", headLines[1]);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }
  }

}
