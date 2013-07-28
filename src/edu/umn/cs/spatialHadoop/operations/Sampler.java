/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.CommandLineArguments;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.RTree;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * Reads a random sample of a file.
 * @author Ahmed Eldawy
 *
 */
public class Sampler {
  private static final Log LOG = LogFactory.getLog(Sampler.class);

  /**Name of the configuration line for sample ratio*/
  private static final String SAMPLE_RATIO = "sampler.SampleRatio";
  
  private static final String InClass = "sampler.InClass";
  private static final String OutClass = "sampler.OutClass";
  
  /**
   * Keeps track of the (uncompressed) size of the last processed file or
   * directory.
   */
  public static long sizeOfLastProcessedFile;
  
  /**Random seed to use by all mappers to ensure unique result per seed*/
  private static final String RANDOM_SEED = "sampler.RandomSeed";
  
  public static class Map extends MapReduceBase implements
  Mapper<CellInfo, Text, NullWritable, Text> {

    /**Ratio of lines to sample*/
    private double sampleRatio;
    
    /**A dummy key for all keys*/
    private final NullWritable dummyKey = NullWritable.get();
    
    /**Random number generator to use*/
    private Random random;

    private Shape inShape;
    
    enum Conversion {None, ShapeToPoint, ShapeToRect};
    Conversion conversion;
    
    @Override
    public void configure(JobConf job) {
      sampleRatio = job.getFloat(SAMPLE_RATIO, 0.01f);
      random = new Random(job.getLong(RANDOM_SEED, System.currentTimeMillis()));
      try {
        Class<? extends TextSerializable> inClass =
            job.getClass(InClass, null).asSubclass(TextSerializable.class);
        Class<? extends TextSerializable> outClass =
            job.getClass(OutClass, null).asSubclass(TextSerializable.class);

        if (inClass == outClass) {
          conversion = Conversion.None;
        } else {
          TextSerializable inObj = inClass.newInstance();
          TextSerializable outObj = outClass.newInstance();

          if (inObj instanceof Shape && outObj instanceof Point) {
            inShape = (Shape) inObj;
            conversion = Conversion.ShapeToPoint;
          } else if (inObj instanceof Shape && outObj instanceof Rectangle) {
            inShape = (Shape) inObj;
            conversion = Conversion.ShapeToRect;
          } else if (outObj instanceof Text) {
            conversion = Conversion.None;
          } else {
            throw new RuntimeException("Don't know how to convert from: "+
                inClass+" to "+outClass);
          }
        }
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    
    public void map(CellInfo cell, Text line,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
            throws IOException {
      if (random.nextFloat() < sampleRatio) {
        switch (conversion) {
        case None:
          output.collect(dummyKey, line);
          break;
        case ShapeToPoint:
          inShape.fromText(line);
          Rectangle mbr = inShape.getMBR();
          if (mbr != null) {
            Point center = mbr.getCenterPoint();
            line.clear();
            center.toText(line);
            output.collect(dummyKey, line);
          }
          break;
        case ShapeToRect:
          inShape.fromText(line);
          mbr = inShape.getMBR();
          if (mbr != null) {
            line.clear();
            mbr.toText(line);
            output.collect(dummyKey, line);
          }
          break;
        }
      }
    }
  }
  
  public static <T extends TextSerializable, O extends TextSerializable> int sampleWithRatio(
      FileSystem fs, Path[] files, double ratio, long threshold, long seed,
      final ResultCollector<O> output, T inObj, O outObj) throws IOException {
    FileStatus inFStatus = fs.getFileStatus(files[0]);
    if (inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 1) {
      // Either a directory of file or a large file
      return sampleMapReduceWithRatio(fs, files, ratio, threshold, seed, output, inObj, outObj);
    } else {
      // A single small file, process it without MapReduce
      return sampleLocalWithRatio(fs, files, ratio, threshold, seed, output, inObj, outObj);
    }
  }  

  public static <T extends TextSerializable, O extends TextSerializable> int sampleLocalWithRatio(
      FileSystem fs, Path[] files, double ratio, long threshold, long seed,
      final ResultCollector<O> output, T inObj, O outObj) throws IOException {
    long total_size = 0;
    for (Path file : files) {
      total_size += fs.getFileStatus(file).getLen();
    }
    sizeOfLastProcessedFile = total_size;
    return sampleLocalWithSize(fs, files, (long) (total_size * ratio), seed,
        output, inObj, outObj);
  }

  /**
   * Sample a ratio of the file through a MapReduce job
   * @param fs
   * @param files
   * @param ratio
   * @param threshold - Maximum number of elements to be sampled
   * @param output
   * @param inObj
   * @return
   * @throws IOException
   */
  public static <T extends TextSerializable, O extends TextSerializable> int sampleMapReduceWithRatio(
      FileSystem fs, Path[] files, double ratio, long threshold, long seed,
      final ResultCollector<O> output, T inObj, O outObj) throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(files[0].toUri().getPath()+
          ".sample_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
    job.setJobName("Sample");
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setClass(InClass, inObj.getClass(), TextSerializable.class);
    job.setClass(OutClass, outObj.getClass(), TextSerializable.class);
    
    job.setMapperClass(Map.class);
    job.setLong(RANDOM_SEED, seed);
    job.setFloat(SAMPLE_RATIO, (float) ratio);

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(0);
    
    job.setInputFormat(ShapeLineInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeLineInputFormat.setInputPaths(job, files);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    RunningJob run_job = JobClient.runJob(job);
    
    Counters counters = run_job.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();

    Counter inputBytesCounter = counters.findCounter(Task.Counter.MAP_INPUT_BYTES);
    Sampler.sizeOfLastProcessedFile = inputBytesCounter.getValue();

    // Ratio of records to return from output based on the threshold
    // Note that any number greater than or equal to one will cause all
    // elements to be returned
    final double selectRatio = (double)threshold / resultCount;
    
    // Read job result
    int result_size = 0;
    if (output != null) {
      Text line = new Text();
      FileStatus[] results = outFs.listStatus(outputPath);
      
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          try {
            while (lineReader.readLine(line) > 0) {
              if (Math.random() < selectRatio) {
                if (output != null) {
                  outObj.fromText(line);
                  output.collect(outObj);
                }
                result_size++;
              }
            }
          } catch (RuntimeException e) {
            e.printStackTrace();
          }
          lineReader.close();
        }
      }
    }
    
    outFs.delete(outputPath, true);
    
    return result_size;
  }

  /**
   * Records as many records as wanted until the total size of the text
   * serialization of sampled records exceed the given limit
   * @param fs
   * @param files
   * @param total_size
   * @param output
   * @param inObj
   * @return
   * @throws IOException
   */
  public static <T extends TextSerializable, O extends TextSerializable> int
    sampleLocalWithSize(
      FileSystem fs, Path[] files, long total_size, long seed,
      final ResultCollector<O> output, final T inObj, final O outObj)
      throws IOException {
    int average_record_size = 1024; // A wild guess for record size
    final LongWritable current_sample_size = new LongWritable();
    int sample_count = 0;

    final ResultCollector<T> converter = createConverter(output, inObj, outObj);

    final ResultCollector<Text2> counter = new ResultCollector<Text2>() {
      @Override
      public void collect(Text2 r) {
        current_sample_size.set(current_sample_size.get() + r.getLength());
        inObj.fromText(r);
        converter.collect(inObj);
      }
    };

    while (current_sample_size.get() < total_size) {
      int count = (int) ((total_size - current_sample_size.get()) / average_record_size);
      if (count < 10)
        count = 10;

      sample_count += sampleLocalByCount(fs, files, count, seed, counter, new Text2(), new Text2());
      // Change the seed to get different sample next time.
      // Still we need to ensure that repeating the program will generate
      // the same value
      seed += sample_count;
      // Update average_records_size
      average_record_size = (int) (current_sample_size.get() / sample_count);
    }
    return sample_count;
  }

  /**
   * Creates a proxy ResultCollector that takes as input objects of type T
   * and converts them to objects of type O.
   * It returns an object with a collect method that takes as input an object
   * of type T (i.e., inObj). This object converts the given object to the
   * type O (i.e., outObj) and sends the result to the method in output#collect.
   * @param <O>
   * @param <T>
   * @param output
   * @param inObj
   * @param outObj
   * @return
   */
  private static <O extends TextSerializable, T extends TextSerializable> ResultCollector<T> createConverter(
      final ResultCollector<O> output, T inObj, final O outObj) {
    if (output == null)
      return null;
    if (inObj.getClass() == outObj.getClass()) {
      return new ResultCollector<T>() {
        @Override
        public void collect(T r) {
          output.collect((O) r);
        }
      };
    } else if (inObj instanceof Shape && outObj instanceof Point) {
      final Point out_pt = (Point) outObj;
      return new ResultCollector<T>() {
        @Override
        public void collect(T r) {
          Point pt = ((Shape)r).getMBR().getCenterPoint();
          out_pt.x = pt.x;
          out_pt.y = pt.y;
          output.collect(outObj);
        }
      };
    } else if (inObj instanceof Shape && outObj instanceof Rectangle) {
      final Rectangle out_rect = (Rectangle) outObj;
      return new ResultCollector<T>() {
        @Override
        public void collect(T r) {
          out_rect.set((Shape)r);
          output.collect(outObj);
        }
      };
    } else if (outObj instanceof Text) {
      final Text text = (Text) outObj;
      return new ResultCollector<T>() {
        @Override
        public void collect(T r) {
          text.clear();
          r.toText(text);
          output.collect(outObj);
        }
      };
    } else if (inObj instanceof Text) {
      final Text text = (Text) inObj;
      return new ResultCollector<T>() {
        @Override
        public void collect(T r) {
          outObj.fromText(text);
          output.collect(outObj);
        }
      };
    } else {
      throw new RuntimeException("Cannot convert from " + inObj.getClass()
          + " to " + outObj.getClass());
    }
  }

  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName(); 
      return !name.startsWith("_") && !name.startsWith("."); 
    }
  }; 

  
  public static <T extends TextSerializable, O extends TextSerializable>
  int sampleLocal(FileSystem fs, Path file, int count, long seed,
      ResultCollector<O> output, T inObj, O outObj) throws IOException {
    return sampleLocalByCount(fs, new Path[] {file}, count, seed, output, inObj, outObj);
  }
  
  /**
   * Reads a sample of the given file and returns the number of items read.
   * 
   * @param fs
   * @param file
   * @param count
   * @return
   * @throws IOException
   */
  public static <T extends TextSerializable, O extends TextSerializable>
  int sampleLocalByCount(
      FileSystem fs, Path[] files, int count, long seed,
      ResultCollector<O> output, T inObj, O outObj) throws IOException {
    ArrayList<Path> data_files = new ArrayList<Path>();
    for (Path file : files) {
      if (fs.getFileStatus(file).isDir()) {
        // Directory, process all data files in this directory (visible files)
        FileStatus[] fileStatus = fs.listStatus(file, hiddenFileFilter);
        for (FileStatus f : fileStatus) {
          data_files.add(f.getPath());
        }
      } else {
        // File, process this file
        data_files.add(file);
      }
    }
    
    files = data_files.toArray(new Path[data_files.size()]);
    
    ResultCollector<T> converter = createConverter(output, inObj, outObj);
    long[] files_start_offset = new long[files.length+1]; // Prefix sum of files sizes
    long total_length = 0;
    for (int i_file = 0; i_file < files.length; i_file++) {
      files_start_offset[i_file] = total_length;
      total_length += fs.getFileStatus(files[i_file]).getLen();
    }
    files_start_offset[files.length] = total_length;

    // Generate offsets to read from and make sure they are ordered to minimize
    // seeks between different HDFS blocks
    Random random = new Random(seed);
    long[] offsets = new long[count];
    for (int i = 0; i < offsets.length; i++) {
      if (total_length == 0)
    	offsets[i] = 0;
      else
        offsets[i] = Math.abs(random.nextLong()) % total_length;
    }
    Arrays.sort(offsets);

    int record_i = 0; // Number of records read so far
    int records_returned = 0;

    int file_i = 0; // Index of the current file being sampled
    while (record_i < count) {
      // Skip to the file that contains the next sample
      while (offsets[record_i] > files_start_offset[file_i+1])
        file_i++;

      // Open a stream to the current file and use it to read all samples
      // in this file
      FSDataInputStream current_file_in = fs.open(files[file_i]);
      long current_file_size = files_start_offset[file_i+1] - files_start_offset[file_i];

      // The start and end offsets of data within this block
      // offsets are calculated relative to file start
      long data_start_offset = 0;
      if (current_file_in.readLong() == SpatialSite.RTreeFileMarker) {
        // This file is an RTree file. Update the start offset to point
        // to the first byte after the header
        data_start_offset = 8 + RTree.getHeaderSize(current_file_in);
      } 
      // Get the end offset of data by searching for the beginning of the
      // last line
      long data_end_offset = current_file_size;
      // Skip the last line too to ensure to ensure that the mapped position
      // will be before some line in the block
      current_file_in.seek(data_end_offset);
      data_end_offset = Tail.tail(current_file_in, 1, null, null);
      long file_data_size = data_end_offset - data_start_offset;

      // Keep sampling as long as records offsets are within this file
      while (record_i < count &&
          (offsets[record_i] - files_start_offset[file_i]) < current_file_size) {
        offsets[record_i] -= files_start_offset[file_i];
        // Map file position to element index in this tree assuming fixed
        // size records
        long element_offset_in_file = offsets[record_i] * file_data_size
            / current_file_size + data_start_offset;
        current_file_in.seek(element_offset_in_file);
        LineReader reader = new LineReader(current_file_in, 4096);
        // Read the first line after that offset
        Text line = new Text();
        reader.readLine(line); // Skip the rest of the current line
        reader.readLine(line); // Read next line

        // Report this element to output
        if (converter != null) {
          inObj.fromText(line);
          converter.collect(inObj);
        }
        record_i++;
        records_returned++;
      }
      current_file_in.close();
    }
    return records_returned;
  }

  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Sampler.class);
    Path[] inputFiles = cla.getPaths();
    FileSystem fs = inputFiles[0].getFileSystem(conf);
    
    if (!fs.exists(inputFiles[0])) {
      throw new RuntimeException("Input file does not exist");
    }
    
    int count = cla.getCount();
    long size = cla.getSize();
    double ratio = cla.getSelectionRatio();
    long seed = cla.getSeed();
    TextSerializable stockObject = cla.getShape(true);
    if (stockObject == null)
      stockObject = new Text2();

    TextSerializable outputShape = cla.getOutputShape();
    if (outputShape == null)
      outputShape = new Text2();
    
    ResultCollector<TextSerializable> output =
    new ResultCollector<TextSerializable>() {
      @Override
      public void collect(TextSerializable value) {
        System.out.println(value.toText(new Text()));
      }
    };
    
    if (size != 0) {
      sampleLocalWithSize(fs, inputFiles, size, seed, output, stockObject, outputShape);
    } else if (ratio != -1.0) {
      long threshold = count == 1? Long.MAX_VALUE : count;
      sampleMapReduceWithRatio(fs, inputFiles, ratio, threshold, seed,
          output, stockObject, outputShape);
    } else {
      // The only way to sample by count is using the local sampler
      sampleLocalByCount(fs, inputFiles, count, seed, output, stockObject, outputShape);
    }
  }
}
