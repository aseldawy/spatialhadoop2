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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;

/**
 * Reads a random sample of a file.
 * @author Ahmed Eldawy
 *
 */
public class Sampler {
  private static final Log LOG = LogFactory.getLog(Sampler.class);

  /**
   * Keeps track of the (uncompressed) size of the last processed file or
   * directory.
   */
  public static long sizeOfLastProcessedFile;
  
  public static class Map extends MapReduceBase implements
  Mapper<Rectangle, Text, IntWritable, Text> {

    /**Ratio of lines to sample*/
    private double sampleRatio;
    
    /**Random number generator to use*/
    private Random random;

    /**The key assigned to all output records to reduce shuffle overhead*/
    private IntWritable key = new IntWritable((int) (Math.random() * Integer.MAX_VALUE));
    
    /**Shape instance used to parse input lines*/
    private Shape inShape;
    
    enum Conversion {None, ShapeToPoint, ShapeToRect};
    Conversion conversion;
    
    @Override
    public void configure(JobConf job) {
      sampleRatio = job.getFloat("ratio", 0.01f);
      random = new Random(job.getLong("seed", System.currentTimeMillis()));
      
      try {
        Class<? extends TextSerializable> inClass =
            job.getClass("shape", null).asSubclass(TextSerializable.class);
        Class<? extends TextSerializable> outClass =
            job.getClass("outshape", null).asSubclass(TextSerializable.class);

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
    
    public void map(Rectangle cell, Text line,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
      if (random.nextFloat() < sampleRatio) {
        switch (conversion) {
        case None:
          output.collect(key, line);
          break;
        case ShapeToPoint:
          inShape.fromText(line);
          Rectangle mbr = inShape.getMBR();
          if (mbr != null) {
            Point center = mbr.getCenterPoint();
            line.clear();
            center.toText(line);
            output.collect(key, line);
          }
          break;
        case ShapeToRect:
          inShape.fromText(line);
          mbr = inShape.getMBR();
          if (mbr != null) {
            line.clear();
            mbr.toText(line);
            output.collect(key, line);
          }
          break;
        }
      }
    }
  }
  
  /**
   * The job of the reduce function is to change the key from NullWritableRnd
   * to NullWritable. TextOutputFormat only understands that NullWritable
   * is not to be written to output not NullWritableRnd.
   * @author Ahmed Eldawy
   *
   */
  public static class Reduce extends MapReduceBase implements
  Reducer<IntWritable, Text, NullWritable, Text> {
    @Override
    public void reduce(IntWritable dummy, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
            throws IOException {
      while (values.hasNext()) {
        Text x = values.next();
        output.collect(NullWritable.get(), x);
      }
    }
  }
  
  @Deprecated
  public static int sampleWithRatio(
      FileSystem fs, Path[] files, float ratio, long threshold, long seed,
      final ResultCollector<TextSerializable> output, TextSerializable inObj,
      TextSerializable outObj) throws IOException {
    OperationsParams params = new OperationsParams();
    params.setFloat("ratio", ratio);
    params.setLong("size", threshold);
    params.setLong("seed", seed);
    params.setClass("shape", inObj.getClass(), TextSerializable.class);
    params.setClass("outshape", outObj.getClass(), TextSerializable.class);
    return sampleWithRatio(files, output, params);
  }
  
  public static <T extends TextSerializable> int sampleWithRatio(
      Path[] files, final ResultCollector<T> output, OperationsParams params) throws IOException {
    FileSystem fs = files[0].getFileSystem(params);
    FileStatus inFStatus = fs.getFileStatus(files[0]);
    if (inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 1) {
      // Either a directory of file or a large file
      return sampleMapReduceWithRatio(files, output, params);
    } else {
      // A single small file, process it without MapReduce
      return sampleLocalWithRatio(files, output, params);
    }
  }  

  @Deprecated
  public static int sampleLocalWithRatio(
      FileSystem fs, Path[] files, float ratio, long threshold, long seed,
      final ResultCollector<TextSerializable> output, TextSerializable inObj,
      TextSerializable outObj) throws IOException {
    OperationsParams params = new OperationsParams();
    params.setFloat("ratio", ratio);
    params.setLong("size", threshold);
    params.setLong("seed", seed);
    params.setClass("shape", inObj.getClass(), TextSerializable.class);
    params.setClass("outshape", outObj.getClass(), TextSerializable.class);
    return sampleLocalWithRatio(files, output, params);
  }
  
  public static <T extends TextSerializable> int sampleLocalWithRatio(
      Path[] files, final ResultCollector<T> output, OperationsParams params) throws IOException {
    long total_size = 0;
    for (Path file : files) {
      FileSystem fs = file.getFileSystem(params);
      total_size += fs.getFileStatus(file).getLen();
    }
    sizeOfLastProcessedFile = total_size;
    float ratio = params.getFloat("ratio", 0.0f);
    params.setLong("size", (long) (total_size * ratio));
    return sampleLocalWithSize(files, output, params);
  }

  /**
   * Sample a ratio of the file through a MapReduce job
   * @param fs
   * @param files
   * @param ratio
   * @param maxSampleSize - Maximum size of sample in bytes
   * @param seed - Random seed to use in randomization for repeatable tests
   * @param output - Collects the generated output
   * @param inObj
   * @param outObj
   * @return
   * @throws IOException
   */
  @Deprecated
  public static int sampleMapReduceWithRatio(
      FileSystem fs, Path[] files, float ratio, long maxSampleSize, long seed,
      final ResultCollector<TextSerializable> output, TextSerializable inObj,
      TextSerializable outObj) throws IOException {
    OperationsParams params = new OperationsParams();
    params.setFloat("ratio", ratio);
    params.setLong("size", maxSampleSize);
    params.setLong("seed", seed);
    params.setClass("shape", inObj.getClass(), TextSerializable.class);
    params.setClass("outshape", outObj.getClass(), TextSerializable.class);
    return sampleMapReduceWithRatio(files, output, params);
  }
  
  public static <T extends TextSerializable> int sampleMapReduceWithRatio(
      Path[] files, final ResultCollector<T> output,
      OperationsParams params) throws IOException {
    JobConf job = new JobConf(params, Sampler.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(files[0].toUri().getPath()+
          ".sample_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
    job.setJobName("Sample");
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    // Number of reduces can be set to zero. However, setting it to a reasonable
    // number ensures that number of output files is limited to that number
    job.setNumReduceTasks(
        Math.max(1, (int)Math.round(clusterStatus.getMaxReduceTasks() * 0.9)));
    
    job.setInputFormat(ShapeLineInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeLineInputFormat.setInputPaths(job, files);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    RunningJob run_job = JobClient.runJob(job);
    
    Counters counters = run_job.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();
    
    Counter outputSizeConter = counters.findCounter(Task.Counter.MAP_OUTPUT_BYTES);
    final long resultSize = outputSizeConter.getValue();
    
    LOG.info("resultSize: "+resultSize);
    LOG.info("resultCount: "+resultCount);

    Counter inputBytesCounter = counters.findCounter(Task.Counter.MAP_INPUT_BYTES);
    Sampler.sizeOfLastProcessedFile = inputBytesCounter.getValue();

    // Ratio of records to return from output based on the threshold
    // Note that any number greater than or equal to one will cause all
    // elements to be returned
    long sampleSize = job.getLong("size", 0);
    final double selectRatio = sampleSize <= 0? 2.0 : (double)sampleSize / resultSize;
    long seed = job.getLong("seed", System.currentTimeMillis());
    T outObj;
    try {
      Class<? extends TextSerializable> outClass = job.getClass("outshape", Text.class).asSubclass(TextSerializable.class);
      outObj = (T) outClass.newInstance();
    } catch (InstantiationException e1) {
      outObj = (T) new Text2();
    } catch (IllegalAccessException e1) {
      outObj = (T) new Text2();
    }


    // Read job result
    int result_size = 0;
    if (output != null) {
      if (selectRatio > 0.1) {
        // Returning a (big) subset of the records extracted by the MR job
        Random rand = new Random(seed);
        if (selectRatio >= 1.0)
          LOG.info("Returning all "+resultCount+" records");
        else
          LOG.info("Returning "+selectRatio+" of "+resultCount+" records");
        Text line = new Text();
        FileStatus[] results = outFs.listStatus(outputPath);

        for (FileStatus fileStatus : results) {
          if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
            InputStream in = outFs.open(fileStatus.getPath());
            // See if we need a decompression coded
            CompressionCodec codec = new CompressionCodecFactory(job).getCodec(fileStatus.getPath());
            Decompressor decompressor = null;
            if (codec != null) {
              decompressor = CodecPool.getDecompressor(codec);
              in = codec.createInputStream(in, decompressor);
            }

            LineReader lineReader = new LineReader(in);
            try {
              while (lineReader.readLine(line) > 0) {
                if (rand.nextDouble() < selectRatio) {
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
            if (decompressor != null)
              CodecPool.returnDecompressor(decompressor);
          }
        }
      } else {
        LOG.info("MapReduce return "+selectRatio+" of "+resultCount+" records");
        // Keep a copy of sizeOfLastProcessedFile because we don't want it changed
        long tempSize = sizeOfLastProcessedFile;
        // Return a (small) ratio of the result using a MapReduce job
        // In this case, the files are very big and we need just a small ratio
        // of them. It is better to do it in parallel
        result_size = sampleMapReduceWithRatio(new Path[] { outputPath},
            output, params);
        sizeOfLastProcessedFile = tempSize;
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
  @Deprecated
  public static int sampleLocalWithSize(FileSystem fs, Path[] files,
      long total_size, long seed,
      final ResultCollector<TextSerializable> output,
      final TextSerializable inObj, final TextSerializable outObj)
      throws IOException {
    OperationsParams params = new OperationsParams();
    params.setLong("size", total_size);
    params.setLong("seed", seed);
    params.setClass("shape", inObj.getClass(), TextSerializable.class);
    params.setClass("outshape", outObj.getClass(), TextSerializable.class);
    return sampleLocalWithSize(files, output, params);
  }

  public static <T extends TextSerializable> int sampleLocalWithSize(Path[] files,
      final ResultCollector<T> output, OperationsParams params)
      throws IOException {

    int average_record_size = 1024; // A wild guess for record size
    final LongWritable current_sample_size = new LongWritable();
    int sample_count = 0;

    TextSerializable inObj1, outObj1;
    try {
      Class<? extends TextSerializable> inClass = params.getClass("shape", TextSerializable.class).asSubclass(TextSerializable.class);
      inObj1 = inClass.newInstance();
    } catch (InstantiationException e) {
      inObj1 = new Text2();
    } catch (IllegalAccessException e) {
      inObj1 = new Text2();
    }

    try {
      Class<? extends TextSerializable> outClass = params.getClass("outshape", TextSerializable.class).asSubclass(TextSerializable.class);
      outObj1 = outClass.newInstance();
    } catch (InstantiationException e) {
      outObj1 = new Text2();
    } catch (IllegalAccessException e) {
      outObj1 = new Text2();
    }
    // Make the objects final to be able to use in the anonymous inner class
    final TextSerializable inObj = inObj1;
    final T outObj = (T) outObj1;
    final ResultCollector<TextSerializable> converter = createConverter(output, inObj, outObj);

    final ResultCollector<Text2> counter = new ResultCollector<Text2>() {
      @Override
      public void collect(Text2 r) {
        current_sample_size.set(current_sample_size.get() + r.getLength());
        inObj.fromText(r);
        converter.collect(inObj);
      }
    };
    
    long total_size = params.getLong("size", 0);
    long seed = params.getLong("seed", System.currentTimeMillis());

    while (current_sample_size.get() < total_size) {
      int count = (int) ((total_size - current_sample_size.get()) / average_record_size);
      if (count < 10)
        count = 10;

      OperationsParams params2 = new OperationsParams(params);
      params2.setClass("shape", Text2.class, TextSerializable.class);
      params2.setClass("outshape", Text2.class, TextSerializable.class);
      params2.setInt("count", count);
      params2.setLong("seed", seed);
      sample_count += sampleLocalByCount(files, counter, params);
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

  
  @Deprecated
  public static int sampleLocalByCount(FileSystem fs, Path file, int count, long seed,
      ResultCollector<TextSerializable> output, TextSerializable inObj,
      TextSerializable outObj) throws IOException {
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
  @Deprecated
  public static int sampleLocalByCount(FileSystem fs, Path[] files, int count,
      long seed, ResultCollector<TextSerializable> output,
      TextSerializable inObj, TextSerializable outObj) throws IOException {
    OperationsParams params = new OperationsParams();
    params.setInt("count", count);
    params.setLong("seed", seed);
    params.setClass("shape", inObj.getClass(), TextSerializable.class);
    params.setClass("outshape", outObj.getClass(), TextSerializable.class);
    return sampleLocalByCount(files, output, params);
  }
  
  public static <T extends TextSerializable> int sampleLocalByCount(Path[] files,
      ResultCollector<T> output, OperationsParams params) throws IOException {

    ArrayList<Path> data_files = new ArrayList<Path>();
    for (Path file : files) {
      FileSystem fs = file.getFileSystem(params);
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
    
    TextSerializable inObj1, outObj1;
    try {
      Class<? extends TextSerializable> inClass = params.getClass("shape", TextSerializable.class).asSubclass(TextSerializable.class);
      inObj1 = inClass.newInstance();
    } catch (InstantiationException e) {
      inObj1 = new Text2();
    } catch (IllegalAccessException e) {
      inObj1 = new Text2();
    }

    try {
      Class<? extends TextSerializable> outClass = params.getClass("outshape", TextSerializable.class).asSubclass(TextSerializable.class);
      outObj1 = outClass.newInstance();
    } catch (InstantiationException e) {
      outObj1 = new Text2();
    } catch (IllegalAccessException e) {
      outObj1 = new Text2();
    }
    // Make the objects final to be able to use in the anonymous inner class
    final TextSerializable inObj = inObj1;
    final T outObj = (T) outObj1;
    
    ResultCollector<TextSerializable> converter = createConverter(output, inObj, outObj);
    long[] files_start_offset = new long[files.length+1]; // Prefix sum of files sizes
    long total_length = 0;
    for (int i_file = 0; i_file < files.length; i_file++) {
      FileSystem fs = files[i_file].getFileSystem(params);
      files_start_offset[i_file] = total_length;
      total_length += fs.getFileStatus(files[i_file]).getLen();
    }
    files_start_offset[files.length] = total_length;

    // Generate offsets to read from and make sure they are ordered to minimize
    // seeks between different HDFS blocks
    Random random = new Random(params.getLong("seed", System.currentTimeMillis()));
    long[] offsets = new long[params.getInt("count", 0)];
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
    while (record_i < offsets.length) {
      // Skip to the file that contains the next sample
      while (offsets[record_i] > files_start_offset[file_i+1])
        file_i++;

      long current_file_size = files_start_offset[file_i+1] - files_start_offset[file_i];
      FileSystem fs = files[file_i].getFileSystem(params);
      ShapeLineRecordReader reader = new ShapeLineRecordReader(fs.getConf(),
          new FileSplit(files[file_i], 0, current_file_size, new String[] {}));
      Rectangle key = reader.createKey();
      Text line = reader.createValue();
      long pos = files_start_offset[file_i];
      
      while (record_i < offsets.length &&
          offsets[record_i] <= files_start_offset[file_i+1] &&
          reader.next(key, line)) {
        pos += line.getLength();
        if (pos > offsets[record_i]) {
          // Passed the offset of record_i
          // Report this element to output
          if (converter != null) {
            inObj.fromText(line);
            converter.collect(inObj);
          }
          record_i++;
          records_returned++;
        }
      }
      reader.close();
      
      // Skip any remaining records that were supposed to be read from this file
      // This case might happen if a generated random position was in the middle
      // of the last line.
      while (record_i < offsets.length &&
          offsets[record_i] <= files_start_offset[file_i+1])
        record_i++;
    }
    return records_returned;
  }
  
  private static void printUsage() {
    System.out.println("Reads a random sample of an input file. Sample is written to stdout");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("shape:<s> - Type of shapes stored in the file");
    System.out.println("outshape:<s> - Shapes to write to output");
    System.out.println("ratio:<r> - ratio of random sample to read [0, 1]");
    System.out.println("count:<s> - approximate number of records in the sample");
    System.out.println("size:<s> - approximate size of the sample in bytes");
    System.out.println("seed:<s> - random seed to use while reading the sample");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    Path[] inputFiles = params.getPaths();
    
    if (!params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    
    long size = params.getSize("size");
    float ratio = params.getFloat("ratio", -1.0f);
    TextSerializable stockObject = params.getShape("shape");
    if (stockObject == null)
      stockObject = new Text2();

    TextSerializable outputShape = params.getShape("outshape");
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
      sampleLocalWithSize(inputFiles, output, params);
    } else if (ratio != -1.0) {
      sampleMapReduceWithRatio(inputFiles, output, params);
    } else {
      // The only way to sample by count is using the local sampler
      sampleLocalByCount(inputFiles, output, params);
    }
  }
}
