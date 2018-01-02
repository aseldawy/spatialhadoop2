package edu.umn.cs.spatialHadoop.util;

import edu.umn.cs.spatialHadoop.operations.Head;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Unit test for the utility class {@link Head}.
 */
public class FSUtilTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public FSUtilTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(FSUtilTest.class);
  }

  public void testMergeAndFlattenPaths() {
    try {
      LocalFileSystem fs = FileSystem.getLocal(new Configuration());
      Path tempDirectory = new Path(Math.random()+".tmp");
      try {
        Path p1 = new Path(tempDirectory, "path1");
        Path p2 = new Path(tempDirectory, "path2");
        fs.mkdirs(p1);
        fs.mkdirs(p2);
        fs.createNewFile(new Path(p1, "file1"));
        fs.createNewFile(new Path(p1, "file2"));
        fs.createNewFile(new Path(p1, "file3"));
        fs.createNewFile(new Path(p2, "file4"));

        FSUtil.mergeAndFlattenPaths(fs, p1, p2);

        FileStatus[] mergedFiles = fs.listStatus(tempDirectory);
        assertEquals(4, mergedFiles.length);
      } finally {
        fs.delete(tempDirectory, true);
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail("Could not create the local file system");
    }
  }

  public void testMergeAndFlattenPathsShouldOverwriteSynonymFiles() {
    try {
      LocalFileSystem fs = FileSystem.getLocal(new Configuration());
      Path tempDirectory = new Path(Math.random()+".tmp");
      try {
        Path p1 = new Path(tempDirectory, "path1");
        Path p2 = new Path(tempDirectory, "path2");
        fs.mkdirs(p1);
        fs.mkdirs(p2);
        fs.createNewFile(new Path(p1, "file1"));
        fs.createNewFile(new Path(p1, "file2"));
        fs.createNewFile(new Path(p1, "file3"));
        fs.createNewFile(new Path(p2, "file1"));

        FSUtil.mergeAndFlattenPaths(fs, p1, p2);

        FileStatus[] mergedFiles = fs.listStatus(tempDirectory);
        assertEquals(3, mergedFiles.length);
      } finally {
        fs.delete(tempDirectory, true);
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail("Could not create the local file system");
    }
  }

}
