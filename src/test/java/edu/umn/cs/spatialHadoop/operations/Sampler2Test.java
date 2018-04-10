package edu.umn.cs.spatialHadoop.operations;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Sampler2Test extends TestCase {

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

  public void testTakeSample() {
    Path input = new Path("src/test/resources/test.rect");
    OperationsParams params = new OperationsParams();
    params.setClass("shape", Rectangle.class, Shape.class);
    params.setClass("outshape", Point.class, Shape.class);
    params.setFloat("ratio", 1.0f); // Read all records
    try {
      String[] lines = Sampler2.takeSample(new Path[]{input}, params);
      assertEquals(14, lines.length);
      // Make sure that they are points
      assertEquals(2, lines[0].split(",").length);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }
  }

  public void testSampleConvert() {
    Path input = new Path("src/test/resources/test.rect");
    Path output = new Path(scratchPath, "sampled");
    OperationsParams params = new OperationsParams();
    params.setClass("shape", Rectangle.class, Shape.class);
    params.setClass("outshape", Point.class, Shape.class);
    params.setFloat("ratio", 1.0f); // Read all records
    try {
      Sampler2.sampleMapReduce(new Path[]{input}, output, params);
      String[] lines = Head.head(output.getFileSystem(params), output, 20);
      assertNotNull(lines[13]);
      assertNull(lines[14]);
      // Make sure that they are points
      assertEquals(2, lines[0].split(",").length);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }
  }
}