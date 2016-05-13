package edu.umn.cs.spatialHadoop;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.osm.OSMEdge;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for {@link OperationsParams} class.
 */
public class OperationsParamsTest extends TestCase {
  
  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public OperationsParamsTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(OperationsParamsTest.class);
  }

  public void testDetectPoint() {
    String detectedShape = OperationsParams.detectShape(new String[] {
       "1,5", "0.5,7.8", "-13.54,33E+15" 
    });
    assertEquals("point", detectedShape);
  }

  public void testDetectRectangle() {
    String detectedShape = OperationsParams.detectShape(new String[] {
        "1,5,-10,100", "0.5,7.8,15,300", "-13.54,33E+15,7,8" 
    });
    assertEquals("rectangle", detectedShape);
  }
  
  public void testDetectOSMEdge() {
    String detectedShape = OperationsParams.detectShape(new String[] {
        "2100,338642,-2.3634904,51.3845649,14718707,-2.3634254,51.3843983,210,{\"highway\"=\"primary\"}{\"ref\"=\"A4\"}{\"name\"=\"Gay Street\"}",
        "4200,200516,-1.8230973,52.5541131,1030999477,-1.8230368,52.5540756,420,{\"highway\"=\"residential\"}{\"is_in\"=\"Sutton Coldfield\"}",
        "4206,200521,-1.8226204,52.5537103,200522,-1.8223988,52.5534041,420,{\"highway\"=\"residential\"}"
    });
    assertEquals(OSMEdge.class.getName(), detectedShape);
  }

}
