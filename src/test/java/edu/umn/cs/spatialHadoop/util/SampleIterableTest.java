package edu.umn.cs.spatialHadoop.util;

import edu.umn.cs.spatialHadoop.OperationsParams;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.Iterator;


public class SampleIterableTest extends TestCase {

  public SampleIterableTest(String testName) {
    super(testName);
  }

  public void testFullSampleTextFile() {
    Path fileToSample = new Path("src/test/resources/test.points");
    OperationsParams params = new OperationsParams();
    try {
      FileSystem fs = fileToSample.getFileSystem(params);
      FileSplit fsplit = new FileSplit(fileToSample, 0, fs.getFileStatus(fileToSample).getLen(), null);
      SampleIterable siter = new SampleIterable(fs, fsplit, 1.0f, 0);
      int count = 0;
      for (Text t : siter)
        ++count;
      assertEquals(11, count);
      assertEquals(1.0f, siter.getProgress());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }
  }

  public void testUnderlyingIterator() {
    Path fileToSample = new Path("src/test/resources/test.points");
    OperationsParams params = new OperationsParams();
    try {
      FileSystem fs = fileToSample.getFileSystem(params);
      FileSplit fsplit = new FileSplit(fileToSample, 0, fs.getFileStatus(fileToSample).getLen(), null);
      SampleIterable siter = new SampleIterable(fs, fsplit, 1.0f, 0);
      Iterator<Text> i = siter.iterator();
      i.hasNext();
      i.hasNext();
      i.hasNext();
      int count = 0;
      while (i.hasNext()) {
        i.next();
        ++count;
      }
      assertEquals(11, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }
  }
}