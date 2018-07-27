package edu.umn.cs.spatialHadoop.util;

import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.*;

/**
 * Unit test for the utility class {@link Head}.
 */
public class BitArrayTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public BitArrayTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(BitArrayTest.class);
  }

  public void testRandomInsert() {
    BitArray bitArray = new BitArray(20);
    bitArray.set(5, true);
    assertTrue(bitArray.get(5));
    bitArray.resize(10000);
    bitArray.set(10000, true);
    assertTrue(bitArray.get(5));
    assertTrue(bitArray.get(10000));
  }

  public void testCountOnes() {
    BitArray bitArray = new BitArray(100);
    for (int i = 0; i < 100; i++) {
      bitArray.set(i, true);
      assertEquals(i+1, bitArray.countOnes());
    }
  }

  public void testReadWrite() {
    BitArray bitArray = new BitArray(20);
    bitArray.set(5, true);
    bitArray.set(10, true);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      bitArray.write(dos);
      dos.close();
    } catch (IOException e) {
      fail("Error in write");
    }
    byte[] buffer = baos.toByteArray();

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    bitArray = new BitArray();
    try {
      bitArray.readFields(in);
      in.close();
    } catch (IOException e) {
      fail("Error in read");
    }
    assertTrue(bitArray.get(5));
    assertTrue(bitArray.get(10));
    assertEquals(20, bitArray.size);
  }
}
