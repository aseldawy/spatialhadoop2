package edu.umn.cs.spatialHadoop;

import edu.umn.cs.spatialHadoop.osm.OSMEdge;
import edu.umn.cs.spatialHadoop.osm.OSMPoint;
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
        "4200,200516,-1.8230973,52.5541131,1030999477,-1.8230368,52.5540756,420,{\"highway\"=\"residential\"}{\"is_in\"=\"Sutton Coldfield\"}",
        "4206,200521,-1.8226204,52.5537103,200522,-1.8223988,52.5534041,420,{\"highway\"=\"residential\"}"
    });
    assertEquals(OSMEdge.class.getName(), detectedShape);
  }

  public void testDetectOSMPolygon() {
    String detectedShape = OperationsParams.detectShape(new String[] {
        "22987\tPOLYGON ((9.1825602 48.7758568, 9.1823907 48.7757439, 9.1825602 48.7758568))\t[type#multipolygon,building#yes,name#Altes Waisenhaus]",
        "167066\tPOLYGON ((18.066006 59.3206665, 18.0663116 59.3205506, 18.0661665 59.3205876, 18.066006 59.3206665))\t[type#multipolygon,building#yes]",
        "423037\tPOLYGON ((-40.3031302 -20.2814281, -40.3032133 -20.2813125, -40.3031302 -20.2814281))\t[type#multipolygon,building#yes]"
    });
    assertEquals("osm", detectedShape);
  }

  public void testDetectOSMPoint() {
    String detectedShape = OperationsParams.detectShape(new String[] {
        "270\t16.6764183\t68.4073792\t[]",
        "19\t-0.20698\t51.9458753\t[ref#SG4 90,collection_times#Mo-Fr 16:30; Sa 09:45,box_type#lamp_box,amenity#post_box]",
        "151\t10.7757166\t59.9515306\t[]"
    });
    assertEquals(OSMPoint.class.getName(), detectedShape);
  }
  
}
