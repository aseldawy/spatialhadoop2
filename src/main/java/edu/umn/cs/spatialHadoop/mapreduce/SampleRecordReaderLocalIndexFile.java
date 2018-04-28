package edu.umn.cs.spatialHadoop.mapreduce;

import edu.umn.cs.spatialHadoop.indexing.LocalIndex;
import edu.umn.cs.spatialHadoop.util.SampleIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * A record reader to read sample of a locally-index file
 * @author Ahmed Eldawy
 *
 */
public class SampleRecordReaderLocalIndexFile extends RecordReader<NullWritable, Text> {

  /**Split to read from*/
  protected FileSplit fsplit;

  /**Sampling ratio*/
  protected float ratio;

  /**Configuration used to obtain FileSystem of file splits*/
  protected Configuration conf;

  /**An iterable that iterates over the sample records*/
  private SampleIterable sampleIterable;

  /**The seed of the random number generator*/
  private long seed;

  /**Current value*/
  protected Text value;

  /**An instance of the local index*/
  private LocalIndex lindex;

  /**The start and end of the local index currently being read*/
  protected long lindexStart, lindexEnd;

  /**The input stream to the underlying file*/
  private FSDataInputStream in;

  public SampleRecordReaderLocalIndexFile(LocalIndex lindex) {
    this.lindex = lindex;
  }
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext task)
      throws IOException {
    this.fsplit = (FileSplit) split;

    this.ratio = task.getConfiguration().getFloat("ratio", 0.01f);
    this.conf = task.getConfiguration();
    this.seed = conf.getLong("seed", System.currentTimeMillis());
    this.lindex.setup(conf);

    FileSystem fs = fsplit.getPath().getFileSystem(conf);
    this.in = fs.open(fsplit.getPath());

    this.lindexEnd = fsplit.getStart() + fsplit.getLength();
    moveToNextLocalIndex();
  }

  private void moveToNextLocalIndex() throws IOException {
    if (lindexEnd <= fsplit.getStart()) {
      sampleIterable = null;
      return;
    }
    in.seek(lindexEnd - 4);
    lindexStart = lindexEnd - in.readInt() - 4;
    in.seek(lindexStart);
    lindex.read(in, lindexStart, lindexEnd, null);
    long dataStart = lindex.getDataStart();
    long dataEnd = lindex.getDataEnd();
    in.seek(dataStart);
    this.sampleIterable = new SampleIterable(in, dataStart, dataEnd, ratio, seed);
  }

  @Override
  public boolean nextKeyValue() {
    if (sampleIterable.hasNext()) {
      value = sampleIterable.next();
      return true;
    }
    // Reached the end of this local index. Move to the  next local index
    try {
      do {
        lindexEnd = lindexStart;
        moveToNextLocalIndex();
      } while (this.sampleIterable != null && !this.sampleIterable.hasNext());
      // No more local indexes with data in them
      return sampleIterable != null && this.sampleIterable.hasNext();
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return sampleIterable == null ? 1.0f : sampleIterable.getProgress();
  }

  @Override
  public void close() throws IOException {
    if (sampleIterable != null)
      sampleIterable.close();
  }
  
}