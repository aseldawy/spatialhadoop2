/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;

/**
 * Reads the last n lines of a text file
 * @author eldawy
 *
 */
public class Tail {
  
  /**
   * Reads a maximum of n lines from the stream starting from its current
   * position and going backward.
   * 
   * @param in - An input stream. It'll be scanned from its current position
   *   backward till position 0
   * @param n - Maximum number of lines to return
   * @param stockObject - An object used to deserialize lines read. It can
   *   be set to <code>null</code> if output is also <code>null</code>. In this
   *   case, nothing is reported to the output.
   * @param output - An output collector used to report lines read.
   * @return - The position of the beginning of the earliest line read from
   *   buffer.
   * @throws IOException
   */
  public static<T extends TextSerializable> long tail(FSDataInputStream in,
      int n, T stockObject, ResultCollector<T> output)
          throws IOException {
    int lines_read = 0;
    long end = in.getPos();
    long offset_of_last_eol = end;
    long last_read_byte = end;
    
    LongWritable line_offset = new LongWritable();
    Text read_line = new Text();
    Text remainder_from_last_buffer = new Text();
    byte[] buffer = new byte[4096];
    
    while (last_read_byte > 0 && lines_read < n) {
      // Read next chunk from the back
      long first_byte_to_read = (last_read_byte - 1) -
          (last_read_byte - 1) % buffer.length;
      in.seek(first_byte_to_read);
      int bytes_to_read = (int) (last_read_byte - first_byte_to_read);
      in.read(buffer, 0, bytes_to_read);
      last_read_byte = first_byte_to_read;
      
      // Iterate over bytes in this buffer
      int i_last_byte_consumed_in_buffer = bytes_to_read;
      int i_last_byte_examined_in_buffer = bytes_to_read;
      while (i_last_byte_examined_in_buffer > 0 && lines_read < n) {
        byte byte_examined = buffer[--i_last_byte_examined_in_buffer];
        if (byte_examined == '\n' || byte_examined == '\r') {
          // Found an end of line character
          // Report this to output unless it's empty
          long offset_of_this_eol = first_byte_to_read + i_last_byte_examined_in_buffer;
          if (offset_of_last_eol - offset_of_this_eol > 1) {
            if (output != null) {
              read_line.clear();
              // +1 is to skip the EOL at the beginning
              read_line.append(buffer, i_last_byte_examined_in_buffer + 1,
                  i_last_byte_consumed_in_buffer - (i_last_byte_examined_in_buffer + 1));
              // Also append bytes remaining from last buffer
              if (remainder_from_last_buffer.getLength() > 0) {
                read_line.append(remainder_from_last_buffer.getBytes(), 0,
                    remainder_from_last_buffer.getLength());
              }
              line_offset.set(offset_of_this_eol + 1);
              stockObject.fromText(read_line);
              output.collect(stockObject);
            }
            lines_read++;
            remainder_from_last_buffer.clear();
          }
          i_last_byte_consumed_in_buffer = i_last_byte_examined_in_buffer;
          offset_of_last_eol = offset_of_this_eol;
        }
      }
      if (i_last_byte_consumed_in_buffer > 0) {
        // There are still some bytes not consumed in buffer
        if (remainder_from_last_buffer.getLength() == 0) {
          // Store whatever is remaining in remainder_from_last_buffer
          remainder_from_last_buffer.append(buffer, 0,
              i_last_byte_consumed_in_buffer);
        } else {
          // Prepend remaining bytes to Text
          Text t = new Text();
          t.append(buffer, 0, i_last_byte_consumed_in_buffer);
          t.append(remainder_from_last_buffer.getBytes(), 0,
              remainder_from_last_buffer.getLength());
          remainder_from_last_buffer = t;
        }
      }
    }
    
    if (lines_read < n && remainder_from_last_buffer.getLength() > 0) {
      // There is still one last line needs to be reported
      lines_read++;
      if (output != null) {
        read_line = remainder_from_last_buffer;
        line_offset.set(0);
        stockObject.fromText(read_line);
        output.collect(stockObject);
      }
      offset_of_last_eol = -1;
    }
    
    return offset_of_last_eol + 1;
  }

  /**
   * Reads a maximum of n non-empty lines from the end of the given file.
   * The position of the earliest line read is returned 
   * @param fs
   * @param file
   * @param n
   * @param stockObject
   * @param output
   * @return
   * @throws IOException
   */
  public static<T extends TextSerializable> long tail(FileSystem fs, Path file,
      int n, T stockObject, ResultCollector<T> output)
          throws IOException {
    FSDataInputStream in = null;
    try {
      in = fs.open(file);
      long length = fs.getFileStatus(file).getLen();
      in.seek(length);
      return tail(in, n, stockObject, output);
    } finally {
      if (in != null)
        in.close();
    }
  }
  
  public static void main(String[] args) throws IOException {
    OperationsParams cla = new OperationsParams(new GenericOptionsParser(args));
    JobConf conf = new JobConf(Sampler.class);
    Path inputFile = cla.getPath();
    FileSystem fs = inputFile.getFileSystem(conf);
    if (!fs.exists(inputFile)) {
      throw new RuntimeException("Input file does not exist");
    }
    int count = cla.getInt("count", 10);
    TextSerializable stockObject = cla.getShape("shape");
    if (stockObject == null)
      stockObject = new Text2();

    tail(fs, inputFile, count, stockObject, new ResultCollector<TextSerializable>() {

      @Override
      public void collect(TextSerializable value) {
        System.out.println(value);
      }
    });
  }
}
