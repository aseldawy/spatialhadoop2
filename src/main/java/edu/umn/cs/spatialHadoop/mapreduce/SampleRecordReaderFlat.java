package edu.umn.cs.spatialHadoop.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.umn.cs.spatialHadoop.operations.LocalSampler;

/**
 * A record reader to read sample of a flat (uncompressed) file
 * @author Ahmed Eldawy
 *
 */
public class SampleRecordReaderFlat extends RecordReader<NullWritable, Text> {
  private static final Log LOG = LogFactory.getLog(SampleRecordReaderFlat.class);
  
  /**Splits to read from*/
  protected FileSplit[] splits;
  
  /**The index of the split currently being sampled in the splits array*/
  protected int currentSplit;
  
  /**InputStream to the currently open file*/
  protected FSDataInputStream in;
  
  /**Index in the list of sample offsets that corresponds to nextSampleLine*/
  protected int iSampleOffset;
  
  /**Offsets to sample*/
  protected long[] sampleOffsets;
  
  /**Sampling ratio*/
  protected float ratio;
  
  /**The sample that corresponds to current key value pair*/
  protected Text currentSampleLine;
  
  /**The line to be returned next. null if the input files have finished*/
  protected Text nextSampleLine;

  /**Configuration used to obtain FileSystem of file splits*/
  protected Configuration conf;

  /**Random number generator*/
  private Random rand;
  
  /**Total size of all splits that have been already sampled*/
  private long sizeOfClosedSplits;
  
  /**Total size of all splits*/
  private long sizeOfAllSplits;
  
  public SampleRecordReaderFlat(InputSplit split, TaskAttemptContext task)
      throws IOException, InterruptedException {
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext task)
      throws IOException, InterruptedException {
    if (split instanceof FileSplit)
      this.splits = new FileSplit[] {(FileSplit) split};
    for (FileSplit s : splits)
      sizeOfAllSplits += s.getLength();
    this.ratio = task.getConfiguration().getFloat("ratio", 0.1f);
    this.conf = task.getConfiguration();
    long seed = conf.getLong("seed", System.currentTimeMillis());
    this.rand = new Random(seed + task.getTaskAttemptID().getId());
    this.readFirstSample();
  }

  /**
   * Read the first sample out of the input files
   * @throws IOException 
   * @throws InterruptedException 
   */
  private void readFirstSample() throws IOException, InterruptedException {
    currentSplit = 0;
    nextSplit();
    nextSampleLine = new Text();
    currentSampleLine = new Text();
    nextKeyValue();
  }

  private void nextSplit() throws IOException {
    FileSystem fs = splits[currentSplit].getPath().getFileSystem(conf);
    in = fs.open(splits[currentSplit].getPath());
    in.skip(splits[currentSplit].getStart());
    nextSampleLine = new Text();
    // Skip first line as it may be chopped
    if (splits[currentSplit].getStart() > 0)
      LocalSampler.readUntilEOL(in, nextSampleLine);
    long totalSize = 0;
    long maxSize = splits[currentSplit].getLength() - (in.getPos() - splits[currentSplit].getStart());
    // Read the first n lines to estimate average line size
    for (int i = 0; i < 10 && totalSize < maxSize; i++) {
      nextSampleLine.clear();
      totalSize += LocalSampler.readUntilEOL(in, nextSampleLine);
    }
    int averageLineSize = (int) (totalSize / 10);

    int sampleSize = (int) Math.max(1, splits[currentSplit].getLength() * ratio / averageLineSize);
    LOG.info("Sampling "+sampleSize+" from split "+splits[currentSplit]);
    sampleOffsets = new long[sampleSize];
    for (int i = 0; i < sampleOffsets.length; i++)
      sampleOffsets[i] = Math.abs(rand.nextLong()) % splits[currentSplit].getLength() + splits[currentSplit].getStart();
    Arrays.sort(sampleOffsets);
    iSampleOffset = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (nextSampleLine == null)
      return false;
    // Swap last and next sample line
    Text temp = currentSampleLine;
    currentSampleLine = nextSampleLine;
    nextSampleLine = temp;
    
    // Read next sample
    
    if (++iSampleOffset >= sampleOffsets.length) {
      // Reached the end of the current split. Go to next split
      // Close current split
      in.close();
      // Set to null to avoid closing it again
      in = null;
      sizeOfClosedSplits += splits[currentSplit].getLength();
      if (++currentSplit >= splits.length) {
        nextSampleLine = null;
      } else {
        nextSplit();
      }
    }
    if (iSampleOffset < sampleOffsets.length) {
      // More lines to sample in the current split
      in.seek(sampleOffsets[iSampleOffset]);
      LocalSampler.readUntilEOL(in, nextSampleLine);
      nextSampleLine.clear();
      LocalSampler.readUntilEOL(in, nextSampleLine);
    }
    
    return true;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException,
      InterruptedException {
    return NullWritable.get();
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return currentSampleLine;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    float progress = (float)sizeOfClosedSplits / sizeOfAllSplits;
    if (currentSplit < splits.length) {
      progress += (float)splits[currentSplit].getLength() * iSampleOffset / sampleOffsets.length / sizeOfAllSplits;
    }
    return progress;
  }

  @Override
  public void close() throws IOException {
    if (in != null)
      in.close();
  }
  
}