package edu.umn.cs.spatialHadoop;

import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for {@link TigerShape} class.
 */
public class TigerShapeTest extends TestCase {
  
  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public TigerShapeTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(TigerShapeTest.class);
  }

  public void testParseTabSeparatedWKT() {
    Text text = new Text("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))\t234");
    TigerShape shape = new TigerShape();
    shape.fromText(text);
    assertNotNull(shape.geom);
    assertTrue(Math.abs(shape.geom.getArea() - 1.0) < 1E-5);
  }

}
