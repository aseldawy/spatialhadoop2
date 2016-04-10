package edu.umn.cs.spatialHadoop.nasa;

import java.nio.ByteBuffer;

import edu.umn.cs.spatialHadoop.TigerShape;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.ShortArray;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for {@link TigerShape} class.
 */
public class HDFRecordReaderTest extends TestCase {
  
  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public HDFRecordReaderTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(HDFRecordReaderTest.class);
  }
  
  public void testFindTrueRuns() {
    short[] values = {
        0, 20, 0, 0,
        30, 0, 0, 60,
        0, 0, 0, 0,
        0, 80, 0, 0
    };
    // Convert the data above to formats compatible with the calee
    ByteBuffer valuesBytes = ByteBuffer.allocate(values.length * 2);
    for (int i = 0; i < values.length; i++)
      valuesBytes.putShort(i*2, values[i]);
    
    short fillValue = 0;
    ShortArray[] trueRuns = HDFRecordReader.findTrueRuns(valuesBytes, fillValue);
    assertEquals(2, trueRuns[0].size());
    assertEquals(1, trueRuns[0].get(0));
    assertEquals(1, trueRuns[0].get(1));

    assertEquals(4, trueRuns[1].size());
    assertEquals(0, trueRuns[1].get(0));
    assertEquals(0, trueRuns[1].get(1));
    assertEquals(3, trueRuns[1].get(2));
    assertEquals(3, trueRuns[1].get(3));
    
    assertEquals(0, trueRuns[2].size());
  }

  public void testRecoverXY() {
    short[] values = {
       0,20, 0,  0,
      30, 0, 0, 60,
       0, 0, 0, 70,
       0,80, 0,  0,
    };
    byte[] waterMask = {
      0, 1, 1, 0,
      1, 0, 0, 1,
      1, 1, 0, 1,
      0, 1, 1, 0,
    };
    // Convert the data above to formats compatible with the calee
    ByteBuffer valuesBytes = ByteBuffer.allocate(values.length * 2);
    BitArray waterMaskBits = new BitArray(waterMask.length);
    for (int i = 0; i < values.length; i++) {
      valuesBytes.putShort(2*i, values[i]);
      waterMaskBits.set(i, waterMask[i] == 1);
    }
    
    short fillValue = 0;
    HDFRecordReader.recoverXYShorts(valuesBytes, fillValue, waterMaskBits);
    
    assertEquals(70, valuesBytes.getShort(2*10));
    assertEquals(50, valuesBytes.getShort(2*6));
    
    assertEquals(25, valuesBytes.getShort(2*0));
    assertEquals(47, valuesBytes.getShort(2*3));
    assertEquals(40, valuesBytes.getShort(2*5));
    assertEquals(63, valuesBytes.getShort(2*12));
    assertEquals(73, valuesBytes.getShort(2*15));
  }

  /**
   * Tests that values which cannot be recovered should be specified as fillValue.
   */
  public void testRecoverXYEmpty() {
    short[] values = {
       0,20, 0, 0,
      30, 0, 0, 0,
       0, 0, 0, 0,
       0, 0, 0, 0,
    };
    byte[] waterMask = {
      1, 1, 1, 1,
      1, 1, 1, 1,
      1, 1, 1, 1,
      1, 1, 1, 0,
    };
    // Convert the data above to formats compatible with the calee
    ByteBuffer valuesBytes = ByteBuffer.allocate(values.length * 2);
    BitArray waterMaskBits = new BitArray(waterMask.length);
    for (int i = 0; i < values.length; i++) {
      valuesBytes.putShort(2*i, values[i]);
      waterMaskBits.set(i, waterMask[i] == 1);
    }
    
    short fillValue = 0;
    HDFRecordReader.recoverXYShorts(valuesBytes, fillValue, waterMaskBits);
    
    assertEquals(fillValue, valuesBytes.getShort(2*15));
  }
  
  public void testConvertWaterMaskToBits() {
    byte[] waterMask = {
        0, 0, 0, 0,
        0, 0, 0, 1,
        0, 1, 1, 1,
        0, 1, 1, 0,
      };
    
    BitArray wmBits = HDFRecordReader.convertWaterMaskToBits(ByteBuffer.wrap(waterMask), 2);
    assertEquals(4L, wmBits.size());
    assertFalse(wmBits.get(0));
    assertFalse(wmBits.get(1));
    assertFalse(wmBits.get(2));
    assertTrue(wmBits.get(3));
  }

}
