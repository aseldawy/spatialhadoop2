package edu.umn.cs.spatialHadoop.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * A record reader to read sample of a compressed file
 * @author Ahmed Eldawy
 *
 */
public class SampleRecordReaderGeneral extends RecordReader<NullWritable, Text> {

  /**Splits to read from*/
  protected FileSplit[] splits;
  
  /**The index of the split currently being sampled in the splits array*/
  protected int currentSplit;
  
  /**Internal reader that reads each split line-by-line*/
  protected LineRecordReader internalReader;
  
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

  /**Task being run. Used to initialize a record reader for each split*/
  private TaskAttemptContext task;

  public SampleRecordReaderGeneral(InputSplit split, TaskAttemptContext task)
      throws IOException, InterruptedException {
    internalReader = new LineRecordReader();
  }
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext task)
      throws IOException, InterruptedException {
    this.task = task;
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
  
  private void readFirstSample() throws IOException, InterruptedException {
    currentSplit = 0;
    nextSplit();
    nextSampleLine = new Text();
    currentSampleLine = new Text();
    nextKeyValue();
  }
  
  private void nextSplit() throws IOException {
    internalReader.initialize(splits[currentSplit], task);
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
    while (currentSplit < splits.length) {
      while (internalReader.nextKeyValue()) {
        if (rand.nextFloat() < ratio)
          nextSampleLine.set(internalReader.getCurrentValue());
      }
      // Split finished. Move to next split
      // Close current split and set to null to avoid recolosing it
      internalReader.close();
      internalReader = null;
      sizeOfClosedSplits += splits[currentSplit].getLength();
      if (++currentSplit >= splits.length) {
        // No more splits. The current value is the last one
        nextSampleLine = null;
      } else {
        nextSplit();
      }
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
    if (currentSplit < splits.length)
      progress += internalReader.getProgress() * splits[currentSplit].getLength() / sizeOfAllSplits;
    return progress;
  }

  @Override
  public void close() throws IOException {
    if (internalReader != null)
      internalReader.close();
  }
  
}