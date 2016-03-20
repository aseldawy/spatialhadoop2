package edu.umn.cs.spatialHadoop.io;

import org.apache.hadoop.io.Text;

import com.vividsolutions.jts.geom.Geometry;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for the utility class {@link TextSerializerHelper}.
 */
public class TextSerializerHelperTest extends TestCase {
  
  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public TextSerializerHelperTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(TextSerializerHelperTest.class);
  }

  public void testParseSimpleWKT() {
    Text text = new Text("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry geom = TextSerializerHelper.consumeGeometryJTS(text, '\0');
    assertNotNull(geom);
    assertTrue(Math.abs(geom.getArea() - 1.0) < 1E-5);
  }

  public void testParseQuotedWKT() {
    Text text = new Text("\"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))\"");
    Geometry geom = TextSerializerHelper.consumeGeometryJTS(text, '\0');
    assertNotNull(geom);
    assertTrue(Math.abs(geom.getArea() - 1.0) < 1E-5);
    assertEquals(0, text.getLength());
  }
  
  public void testConsumeWKT() {
    Text text = new Text("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)),1234");
    Geometry geom = TextSerializerHelper.consumeGeometryJTS(text, ',');
    assertNotNull(geom);
    assertTrue(Math.abs(geom.getArea() - 1.0) < 1E-5);
    int dummyInt = TextSerializerHelper.consumeInt(text, '\0');
    assertEquals(1234, dummyInt);
  }

  public void testConsumeQuotedWKT() {
    Text text = new Text("\'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))\',1234");
    Geometry geom = TextSerializerHelper.consumeGeometryJTS(text, ',');
    assertNotNull(geom);
    assertTrue(Math.abs(geom.getArea() - 1.0) < 1E-5);
    int dummyInt = TextSerializerHelper.consumeInt(text, '\0');
    assertEquals(1234, dummyInt);
  }
}
