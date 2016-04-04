/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;

/**
 * A slightly modified version of {@link TextOutputFormat} that performs special
 * conversion to text for objects of type {@link TextSerializable}.
 * @author Ahmed Eldawy
 *
 * @param <K> The type of key
 * @param <V> The type of value
 */
public class TextOutputFormat3<K, V> extends FileOutputFormat<K, V> {

  protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    public LineRecordWriter(DataOutputStream out) {
      this(out, "\t");
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else if (o instanceof TextSerializable) {
        // Serialize object to text
        Text to = new Text2();
        ((TextSerializable) o).toText(to);
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
    }

    @Override
    public synchronized void write(K key, V value)
      throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      if (!nullValue) {
        writeObject(value);
      }
      out.write(newline);
    }
    
    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
        InterruptedException {
      out.close();
    }
  }
  
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext task)
      throws IOException, InterruptedException {
    Configuration conf = task.getConfiguration();
    boolean isCompressed = getCompressOutput(task);
    String keyValueSeparator = conf.get("mapred.textoutputformat.separator", 
        "\t");
    if (!isCompressed) {
      Path file = getDefaultWorkFile(task, "");
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, task);
      return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
    } else {
      Class<? extends CompressionCodec> codecClass =
          getOutputCompressorClass(task, GzipCodec.class);
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      // build the filename including the extension
      Path file = getDefaultWorkFile(task, codec.getDefaultExtension());
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, task);
      return new LineRecordWriter<K, V>(new DataOutputStream
          (codec.createOutputStream(fileOut)),
          keyValueSeparator);
    }
  }
}

