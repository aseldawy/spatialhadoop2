package edu.umn.cs.spatialHadoop.operations;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.TestHelper;
import edu.umn.cs.spatialHadoop.core.*;
import edu.umn.cs.spatialHadoop.indexing.LocalIndex;
import edu.umn.cs.spatialHadoop.indexing.RRStarLocalIndex;
import edu.umn.cs.spatialHadoop.indexing.RRStarTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Random;

/**
 * Unit test for {@link LocalSampler} class.
 */
public class LocalSamplerTest extends TestCase {
  
  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public LocalSamplerTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(LocalSamplerTest.class);
  }
  
  public void testSampleRTree() {
    // Build an R-tree on a random data and store in a file
    Path randomFile = new Path("temp.points");
    String rrstarExtension = RRStarLocalIndex.class.getAnnotation(LocalIndex.LocalIndexMetadata.class).extension();
    Path rtreeFile = new Path("temp."+rrstarExtension);
    try {
      final Rectangle mbr = new Rectangle(0, 0, 1000, 1000);
      // Generate a random file of points
      OperationsParams params = new OperationsParams();
      TestHelper.generateFile(randomFile.getName(), Point.class, mbr, 1024*1024, params);

      // Locally index the file
      RRStarLocalIndex localIndexer = new RRStarLocalIndex();
      localIndexer.setup(params);
      localIndexer.buildLocalIndex(new File(randomFile.getName()), rtreeFile, new Point());

      // Sample from the locally indexed file
      LocalSampler.sampleLocal(new Path[] {rtreeFile}, 10, new ResultCollector<Text>() {
        @Override
        public void collect(Text r) {
          Point pt = new Point();
          pt.fromText(r);
          assertTrue("Incorrect point", mbr.contains(pt));
        }
      }, params);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test!");
    }
    /*Path inFile = new Path("src/test/resources/test.rtree");
    
    try {
      LocalSampler.sampleLocal(new Path[] {inFile}, 10, new ResultCollector<Text>() {
        @Override
        public void collect(Text r) {
          System.out.println(r);
          assertEquals(4, r.toString().split(",").length);
        }
      }, new Configuration());
    } catch (Exception e) {
      throw new RuntimeException("Error running test", e);
    }*/
  }

}
