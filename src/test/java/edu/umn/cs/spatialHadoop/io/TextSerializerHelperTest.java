package edu.umn.cs.spatialHadoop.io;

import org.apache.hadoop.io.Text;

import com.vividsolutions.jts.geom.Geometry;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.HashMap;
import java.util.Map;

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

  public void testConsumeMap() {
    String textMap = "[k1#v1,k2#v2,k3#v3]";
    Text text = new Text(textMap);
    Map<String, String> map = new HashMap<String, String>();
    TextSerializerHelper.consumeMap(text, map);
    assertEquals(3, map.size());
    assertEquals("v1", map.get("k1"));
    assertEquals("v2", map.get("k2"));
    assertEquals("v3", map.get("k3"));
  }

  public void testSerializeMap() {
    String textMap = "[k1#v1,k2#v2,k3#v3]";
    Text text = new Text();
    Map<String, String> map = new HashMap<String, String>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    map.put("k3", "v3");
    TextSerializerHelper.serializeMap(text, map);
    assertEquals(textMap.length(), text.getLength());
    assertTrue("k1#v1 not found", text.toString().contains("k1#v1"));
    assertTrue("k2#v2 not found", text.toString().contains("k2#v2"));
    assertTrue("k3#v3 not found", text.toString().contains("k3#v3"));
  }
}
