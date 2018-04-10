package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.LocalIndex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;

/**
 * An input format that reads a sample
 * @author Ahmed Eldawy
 *
 */
public class SampleInputFormat2 extends FileInputFormat<NullWritable, Text> {
  private static final Log LOG = LogFactory.getLog(SampleInputFormat2.class);

  @Override
  public RecordReader<NullWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext task) throws IOException, InterruptedException {
    FileSplit fsplit = (FileSplit) split;
    String fname = fsplit.getPath().getName();
    // Retrieve the extension to see if it is locally index
    int lastDot = fname.lastIndexOf('.');
    if (lastDot != -1) {
      try {
        String extension = fname.substring(lastDot + 1);
        Class<? extends LocalIndex> lindexClass = SpatialSite.getLocalIndex(extension);
        if (lindexClass != null) {
          LocalIndex lindex = lindexClass.newInstance();
          SampleRecordReaderLocalIndexFile srr = new SampleRecordReaderLocalIndexFile(lindex);
          srr.initialize(split, task);
          return srr;
        }
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    // Either a file with no extension or failed to find a local index
    SampleRecordReaderTextFile srr = new SampleRecordReaderTextFile();
    srr.initialize(split, task);
    return srr;
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    // TODO combine splits that reside on the same machine
    return splits;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path path) {
    boolean superIsSplittable = super.isSplitable(context, path);
    if (!superIsSplittable)
      return false;
    // If it has a local index, do not split it
    String fname = path.getName();
    int lastDot = fname.lastIndexOf('.');
    if (lastDot == -1)
      return true; // No local index
    String extension = fname.substring(lastDot + 1);
    Class<? extends LocalIndex> lindexClass = SpatialSite.getLocalIndex(extension);
    return lindexClass == null; // Splittable only if no associated local index
  }
}