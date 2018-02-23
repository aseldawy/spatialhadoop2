package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Unit test for the index construction operation
 */
public class IndexerTest extends TestCase {

  Path outPath = new Path("src/test/resources/temp_out");

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public IndexerTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(IndexerTest.class);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    // Clean up temporary file
    FileSystem outFS = outPath.getFileSystem(new Configuration());
    outFS.deleteOnExit(outPath);
  }

  public void testEmptyGeometries() {
    // Should not crash with an input file that contains empty geomtries
    try {
      Path inPath = new Path("src/test/resources/polys.osm");

      OperationsParams params = new OperationsParams();
      params.setBoolean("local", false);
      params.setClass("shape", OSMPolygon.class, Shape.class);
      params.setFloat("ratio", 1.0f);
      params.set("sindex", "rtreeag");
      Indexer.index(inPath, outPath, params);
    } catch (FileNotFoundException e) {
      fail("Error opening test file");
    } catch (IOException e) {
      e.printStackTrace();
      fail("Error working with the test file");
    } catch (InterruptedException e) {
      fail("Error running the job");
    } catch (ClassNotFoundException e) {
      fail("Could not create the shape");
    }
  }
}
