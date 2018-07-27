package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.BaseTest;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.indexing.RRStarLocalIndex;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.*;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class SampleInputFormatTest extends BaseTest {
  public SampleInputFormatTest(String testName) {
    super(testName);
  }

  public void testSampleConcatenatedRTreeFile() {
    try {
      OperationsParams params = new OperationsParams();
      Path concatFile = new Path(scratchPath, "concat.rrstar");
      FileSystem fs = concatFile.getFileSystem(params);
      FSDataOutputStream out = fs.create(concatFile, true);
      RRStarLocalIndex localIndexer = new RRStarLocalIndex();
      localIndexer.setup(params);

      File[] inputFiles = new File[] {
          new File("src/test/resources/test.points"),
          new File("src/test/resources/test111.points"),
      };

      for (File file : inputFiles) {
        Path tempRtreeFile = new Path(scratchPath, "temp.rrstar");
        localIndexer.buildLocalIndex(file, tempRtreeFile, new Point());
        InputStream in = new FileInputStream(tempRtreeFile.toString());
        IOUtils.copyBytes(in, out, params, false);
        in.close();
      }
      out.close();

      // Sample all records in the file
      params.setFloat("ratio", 1.0f);
      Path fileToSample = concatFile;
      SampleInputFormat sinputFormat = new SampleInputFormat();
      Job job = Job.getInstance(params);
      SampleInputFormat.addInputPath(job, fileToSample);
      List<InputSplit> splits = sinputFormat.getSplits(job);
      int count = 0;
      for (InputSplit split : splits) {
        RecordReader<NullWritable, Text> srr =
            sinputFormat.createRecordReader(split, new TaskAttemptContextImpl(params, new TaskAttemptID()));
        while (srr.nextKeyValue()) {
          ++count;
        }
        srr.close();
      }
      assertEquals(11+111, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }

  }

  public void testSampleCompressedFile() {
    Path inputFile = new Path(scratchPath, "sample.gz");
    try {
      OperationsParams params = new OperationsParams();
      FileSystem fs = inputFile.getFileSystem(params);
      PrintStream out = new PrintStream(new GZIPOutputStream(fs.create(inputFile, true)));
      out.println("line1");
      out.println("line2");
      out.println("line3");
      out.close();

      // Sample all lines in the file
      params.setFloat("ratio", 1.0f);
      SampleInputFormat sinputFormat = new SampleInputFormat();
      Job job = Job.getInstance(params);
      SampleInputFormat.addInputPath(job, inputFile);
      List<InputSplit> splits = sinputFormat.getSplits(job);
      int count = 0;
      for (InputSplit split : splits) {
        RecordReader<NullWritable, Text> srr =
            sinputFormat.createRecordReader(split, new TaskAttemptContextImpl(params, new TaskAttemptID()));
        while (srr.nextKeyValue()) {
          assertEquals(5, srr.getCurrentValue().getLength());
          assertTrue("Incorrect line",  srr.getCurrentValue().toString().startsWith("line"));
          ++count;
        }
        srr.close();
      }
      assertEquals(3, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error in test");
    }
  }

  public void testSampleBlockCompressedFile() {
    Path inputFile = new Path(scratchPath, "sample.bz2");
    try {
      OperationsParams params = new OperationsParams();
      FileSystem fs = inputFile.getFileSystem(params);

      PrintStream out = new PrintStream(new CBZip2OutputStream(fs.create(inputFile, true), 1));
      // Write many records to ensure multiple blocks are created
      int numRecords = 10000;
      for (int i = 0; i < numRecords; i++) {
        out.printf("line-%06d\n", i);
      }
      out.close();

      // Sample all lines in the file
      params.setFloat("ratio", 1.0f);
      SampleInputFormat sinputFormat = new SampleInputFormat();
      params.setLong(FileInputFormat.SPLIT_MAXSIZE, fs.getFileStatus(inputFile).getLen() / 2);
      Job job = Job.getInstance(params);
      SampleInputFormat.addInputPath(job, inputFile);
      List<InputSplit> splits = sinputFormat.getSplits(job);
      int count = 0;
      for (InputSplit split : splits) {
        RecordReader<NullWritable, Text> srr =
            sinputFormat.createRecordReader(split, new TaskAttemptContextImpl(params, new TaskAttemptID()));
        while (srr.nextKeyValue()) {
          assertEquals("Incorrect line length "+"Incorrect line "+srr.getCurrentValue(), 11, srr.getCurrentValue().getLength());
          assertTrue("Incorrect line "+srr.getCurrentValue(),  srr.getCurrentValue().toString().startsWith("line-"));
          ++count;
        }
        srr.close();
      }
      assertEquals(numRecords, count);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Error in test", e);
    }
  }
}