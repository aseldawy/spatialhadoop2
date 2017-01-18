package edu.umn.cs.spatialHadoop.operations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.ResultCollector;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
