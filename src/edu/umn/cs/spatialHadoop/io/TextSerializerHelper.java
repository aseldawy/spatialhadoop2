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
package edu.umn.cs.spatialHadoop.io;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.esri.core.geometry.ogc.OGCGeometry;

public final class TextSerializerHelper {
  /**
   * All possible chars for representing a number as a String
   */
  final static byte[] digits = {
    '0' , '1' , '2' , '3' , '4' , '5' ,
    '6' , '7' , '8' , '9' , 'a' , 'b' ,
    'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
    'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
    'o' , 'p' , 'q' , 'r' , 's' , 't' ,
    'u' , 'v' , 'w' , 'x' , 'y' , 'z'
  };
  
  final static boolean[] HexadecimalChars;
  final static boolean[] DecimalChars;
  
  /**64 bytes to append to a string if necessary*/
  final static byte[] ToAppend = new byte[64];
  
  static {
    HexadecimalChars = new boolean[256];
    DecimalChars = new boolean[256];
    for (char i = 'a'; i <= 'f'; i++)
      HexadecimalChars[i] = true;
    for (char i = 'A'; i <= 'F'; i++)
      HexadecimalChars[i] = true;
    for (char i = '0'; i <= '9'; i++) {
      DecimalChars[i] = true;
      HexadecimalChars[i] = true;
    }
    HexadecimalChars['-'] = true;
    DecimalChars['-'] = true;
    
    Arrays.fill(ToAppend, (byte)' ');
  }
  
  /**
   * Appends hex representation of the given number to the given string.
   * If append is set to true, a comma is also appended to the text.
   * @param i
   * @param t
   * @param appendComma
   */
  public static void serializeHexLong(long i, Text t, char toAppend) {
    // Calculate number of bytes needed to serialize the given long
    int bytes_needed = 0;
    long temp;
    if (i < 0) {
      bytes_needed++; // An additional
      temp = -i;
    } else {
      temp = i;
    }
    do {
      bytes_needed += 1;
      temp >>>= 4;
    } while (temp != 0);
    
    if (toAppend != '\0')
      bytes_needed++;

    // Reserve the bytes needed in the text
    t.append(ToAppend, 0, bytes_needed);
    // Extract the underlying buffer array and fill it directly
    byte[] buffer = t.getBytes();
    // Position of the next character to write in the text
    int position = t.getLength() - 1;
    
    if (toAppend != '\0')
      buffer[position--] = (byte) toAppend;
    
    final int shift = 4;
    final int radix = 1 << shift;
    final long mask = radix - 1;
    
    // Negative sign is prepended separately for negative numbers
    boolean negative = false;
    if (i < 0) {
      i = -i;
      negative = true;
    }
    do {
      buffer[position--] = digits[(int)(i & mask)];
      i >>>= shift;
    } while (i != 0);
    if (negative)
      buffer[position--] = '-';
  }
  
  /**
   * Parses only long from the given byte array (string). The long starts at
   * offset and is len characters long.
   * @param buf
   * @param offset
   * @param len
   * @return
   */
  public static long deserializeHexLong(byte[] buf, int offset, int len) {
    boolean negative = false;
    if (buf[offset] == '-') {
      negative = true;
      offset++;
      len--;
    }
    long i = 0;
    while (len-- > 0) {
      i <<= 4;
      if (buf[offset] <= '9')
        i |= buf[offset++] - '0';
      else
        i |= buf[offset++] - 'a' + 10;
    }
    return negative ? -i : i;
  }
  
  /**
   * Deserializes and consumes a long from the given text. Consuming means all
   * characters read for deserialization are removed from the given text.
   * If separator is non-zero, a long is read and consumed up to the first
   * occurrence of this separator. The separator is also consumed.
   * @param text
   * @param separator
   * @return
   */
  public static long consumeHexLong(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && HexadecimalChars[bytes[i]])
      i++;
    long l = deserializeHexLong(bytes, 0, i);
    // If the first char after the long is the separator, skip it
    if (i < text.getLength() && bytes[i] == separator)
      i++;
    // Shift bytes after the long
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return l;
  }
  
  enum DoubleParseState {BeforeDecimal, AfterDecimal, AfterExp};
  public static double deserializeDouble(byte[] buf, int offset, int len) {
    DoubleParseState state = DoubleParseState.BeforeDecimal;

    int exponent1 = 0; // Exponent part coming from the decimal point
    boolean exponent2Negative = false;
    int exponent2 = 0; // Exponent part written explicitly (e.g., E+32)

    boolean mantissaNegative = false;
    long mantissa = 0;
    
    while (len-- > 0) {
      if (buf[offset] >= '0' && buf[offset] <= '9') {
        switch(state) {
        case AfterDecimal:
          exponent1--;
          // Fall through
        case BeforeDecimal:
          mantissa = mantissa * 10 + (buf[offset] - '0');
          break;
        case AfterExp:
          exponent2 = exponent2 * 10 + (buf[offset] - '0');
          break;
        }
      } else if (buf[offset] == '.') {
        state = DoubleParseState.AfterDecimal;
      } else if (buf[offset] == 'e' || buf[offset] == 'E') {
        state = DoubleParseState.AfterExp;
      } else if (buf[offset] == '-') {
        if (state == DoubleParseState.BeforeDecimal) {
          mantissaNegative = true;
        } else if (state == DoubleParseState.AfterExp) {
          exponent2Negative = true;
        } else {
          throw new RuntimeException("Error parsing double "+
                new String(buf, offset, len)+" at position "+offset);
        }
      } else if (buf[offset] == '+') {
        // Just skip. The default sign is positive
      } else {
        throw new RuntimeException("Error parsing double "+
            new String(buf, offset, len)+" at position "+offset);
      }
      offset++;
    }
    
    if (mantissaNegative)
      mantissa = -mantissa;
    if (exponent2Negative)
      exponent2 = -exponent2;

    int exponent = exponent1 + exponent2;
    double d = mantissa;
    if (exponent > 0) {
      while (exponent-- != 0)
        d *= 10;
    } else if (exponent < 0) {
      while (exponent++ != 0)
        d /= 10;
    }
    return d;
  }
  
  /**
   * Deserializes and consumes a double from the given text. Consuming means all
   * characters read for deserialization are removed from the given text.
   * If separator is non-zero, a double is read and consumed up to the first
   * occurrence of this separator. The separator is also consumed.
   * @param text
   * @param separator
   * @return
   */
  public static double consumeDouble(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength()
        && ((bytes[i] >= '0' && bytes[i] <= '9') || bytes[i] == 'e'
            || bytes[i] == 'E' || bytes[i] == '-' || bytes[i] == '+' || bytes[i] == '.'))
      i++;
    double d = deserializeDouble(bytes, 0, i);
    if (i < text.getLength() && bytes[i] == separator)
      i++;
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return d;
  }
  
  /**
   * Appends hex representation of the given number to the given string.
   * If append is set to true, a comma is also appended to the text.
   * @param i
   * @param t
   * @param appendComma
   */
  public static void serializeDouble(double d, Text t, char toAppend) {
    byte[] bytes = Double.toString(d).getBytes();
    t.append(bytes, 0, bytes.length);
    if (toAppend != '\0') {
      t.append(new byte[] {(byte)toAppend}, 0, 1);
    }
  }
  
  public static void serializeLong(long i, Text t, char toAppend) {
    // Calculate number of bytes needed to serialize the given long
    int bytes_needed = 0;
    long temp;
    if (i < 0) {
      bytes_needed++; // An additional
      temp = -i;
    } else {
      temp = i;
    }
    do {
      bytes_needed += 1;
      temp /= 10;
    } while (temp != 0);
    
    if (toAppend != '\0')
      bytes_needed++;

    // Reserve the bytes needed in the text
    t.append(ToAppend, 0, bytes_needed);
    // Extract the underlying buffer array and fill it directly
    byte[] buffer = t.getBytes();
    // Position of the next character to write in the text
    int position = t.getLength() - 1;
    
    if (toAppend != '\0')
      buffer[position--] = (byte) toAppend;
    
    // Negative sign is prepended separately for negative numbers
    boolean negative = false;
    if (i < 0) {
      i = -i;
      negative = true;
    }
    do {
      int digit = (int) (i % 10);
      buffer[position--] = digits[digit];
      i /= 10;
    } while (i != 0);
    if (negative)
      buffer[position--] = '-';
  }
  
  public static long deserializeLong(byte[] buf, int offset, int len) {
    boolean negative = false;
    if (buf[offset] == '-') {
      negative = true;
      offset++;
      len--;
    }
    long i = 0;
    while (len-- > 0) {
      i *= 10;
      i += buf[offset++] - '0';
    }
    return negative ? -i : i;
  }
  
  public static long consumeLong(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && DecimalChars[bytes[i]])
      i++;
    long l = deserializeLong(bytes, 0, i);
    // If the first char after the long is the separator, skip it
    if (i < text.getLength() && bytes[i] == separator)
      i++;
    // Shift bytes after the long
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return l;
  }
  
  public static void serializeInt(int i, Text t, char toAppend) {
    // Calculate number of bytes needed to serialize the given long
    int bytes_needed = 0;
    int temp;
    if (i < 0) {
      bytes_needed++; // An additional
      temp = -i;
    } else {
      temp = i;
    }
    do {
      bytes_needed += 1;
      temp /= 10;
    } while (temp != 0);
    
    if (toAppend != '\0')
      bytes_needed++;

    // Reserve the bytes needed in the text
    t.append(ToAppend, 0, bytes_needed);
    // Extract the underlying buffer array and fill it directly
    byte[] buffer = t.getBytes();
    // Position of the next character to write in the text
    int position = t.getLength() - 1;
    
    if (toAppend != '\0')
      buffer[position--] = (byte) toAppend;
    
    // Negative sign is prepended separately for negative numbers
    boolean negative = false;
    if (i < 0) {
      i = -i;
      negative = true;
    }
    do {
      int digit = i % 10;
      buffer[position--] = digits[digit];
      i /= 10;
    } while (i != 0);
    if (negative)
      buffer[position--] = '-';
  }
  
  public static int deserializeInt(byte[] buf, int offset, int len) {
    boolean negative = false;
    if (buf[offset] == '-') {
      negative = true;
      offset++;
      len--;
    }
    int i = 0;
    while (len-- > 0) {
      i *= 10;
      i += buf[offset++] - '0';
    }
    return negative ? -i : i;
  }
  
  public static int consumeInt(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && DecimalChars[bytes[i]])
      i++;
    int l = deserializeInt(bytes, 0, i);
    // If the first char after the long is the separator, skip it
    if (i < text.getLength() && bytes[i] == separator)
      i++;
    // Shift bytes after the long
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return l;
  }
 
  private static final byte[] Separators = {'[', '#', ',', ']'};
  private static final int MapStart = 0, KeyValueSeparator = 1,
      FieldSeparator = 2, MapEnd = 3;
  
  public static void consumeMap(Text text, Map<String, String> tags) {
    tags.clear();
    if (text.getLength() > 0) {
      byte[] tagsBytes = text.getBytes();
      if (tagsBytes[0] != Separators[MapStart])
        return;
      int i1 = 1;
      while (i1 < text.getLength() && tagsBytes[i1] != Separators[MapEnd]) {
        int i2 = i1 + 1;
        while (i2 < text.getLength() && tagsBytes[i2] != Separators[KeyValueSeparator])
          i2++;
        String key = new String(tagsBytes, i1, i2 - i1);
        i1 = i2 + 1;
        
        i2 = i1 + 1;
        while (i2 < text.getLength() && tagsBytes[i2] != Separators[FieldSeparator] && tagsBytes[i2] != Separators[MapEnd])
          i2++;
        String value = new String(tagsBytes, i1, i2 - i1);
        tags.put(key, value);
        i1 = i2;
        if (i1 < text.getLength() && tagsBytes[i1] == Separators[FieldSeparator])
          i1++;
      }
      text.set(tagsBytes, i1, text.getLength() - i1);
    }
  }
  

  public static Text serializeMap(Text text, Map<String, String> tags) {
    if (!tags.isEmpty()) {
      boolean first = true;
      text.append(Separators, MapStart, 1);
      for (Map.Entry<String, String> entry : tags.entrySet()) {
        if (first) {
          first = false;
        } else {
          first = true;
          text.append(Separators, FieldSeparator, 1);
        }
        byte[] k = entry.getKey().getBytes();
        text.append(k, 0, k.length);
        text.append(Separators, KeyValueSeparator, 1);
        byte[] v = entry.getValue().getBytes();
        text.append(v, 0, v.length);
      }
      text.append(Separators, MapEnd, 1);
    }
    return text;
  }
  
  private static final byte[][] ShapeNames = { "LINESTRING".getBytes(),
    "POINT".getBytes(), "POLYGON".getBytes(), "MULTIPOINT".getBytes(),
    "MULTILINESTRING".getBytes(), "MULTIPOLYGON".getBytes(),
    "GEOMETRYCOLLECTION".getBytes() };

  public static OGCGeometry consumeGeometryESRI(Text text, char separator) {
 // Check whether this text is a Well Known Text (WKT) or a hexed string
    boolean wkt = false;
    byte[] bytes = text.getBytes();
    int length = text.getLength();
    int i_shape = 0;
    while (!wkt && i_shape < ShapeNames.length) {
      byte[] shapeName = ShapeNames[i_shape];
      if (length > shapeName.length) {
        int i = 0;
        while (i < shapeName.length && shapeName[i] == bytes[i])
          i++;
        if (i == shapeName.length) {
          wkt = true;
          break;
        }
      }
      i_shape++;
    }
    
    // Look for the terminator of the shape text
    int i1 = 0;
    if (bytes[i1] == '\'' || bytes[i1] == '\"') {
      separator = (char) bytes[i1++];
    }
    int i2 = i1;
    while (i2 < length && bytes[i2] != separator)
      i2++;
    
    String str = new String(bytes, i1, i2-i1);
    
    // Remove consumed bytes from the text
    text.set(bytes, i2, text.getLength() - i2);
    
    OGCGeometry geom = parseText(str);
    
    return geom;
  }
  
  public static OGCGeometry parseText(String str) {
    OGCGeometry geom = null;
    try {
      // Parse string as well known text (WKT)
      geom = OGCGeometry.fromText(str);
    } catch (IllegalArgumentException e) {
      try {
        // Error parsing from WKT, try hex string instead
        byte[] binary = hexToBytes(str);
        geom = OGCGeometry.fromBinary(ByteBuffer.wrap(binary));
      } catch (RuntimeException e1) {
        throw new RuntimeException("Cannot parse the shape: "+str, e1);
      }
    }
    return geom;
  }
  
  /**
   * Convert a string containing a hex string to a byte array of binary.
   * For example, the string "AABB" is converted to the byte array {0xAA, 0XBB}
   * @param hex
   * @return
   */
  public static byte[] hexToBytes(String hex) {
    byte[] bytes = new byte[(hex.length() + 1) / 2];
    for (int i = 0; i < hex.length(); i++) {
      byte x = (byte) hex.charAt(i);
      if (x >= '0' && x <= '9')
        x -= '0';
      else if (x >= 'a' && x <= 'f')
        x = (byte) ((x - 'a') + 0xa);
      else if (x >= 'A' && x <= 'F')
        x = (byte) ((x - 'A') + 0xA);
      else
        throw new RuntimeException("Invalid hex char "+x+" at position "+i);
      if (i % 2 == 0)
        x <<= 4;
      bytes[i / 2] |= x;
    }
    return bytes;
  }
  
  public static void serializeGeometry(Text text, OGCGeometry geom, char toAppend) {
    String str = bytesToHex(geom.asBinary().array());
    byte[] str_b = str.getBytes();
    text.append(str_b, 0, str_b.length);
    text.append(new byte[] {(byte) toAppend}, 0, 1);
  }
  
  private static final byte[] HexLookupTable = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };
  
  /**
   * Convert binary array to a hex string.
   * @param binary
   * @return
   */
  public static String bytesToHex(byte[] binary) {
    // Each byte is converted to two hex values
    byte[] hex = new byte[binary.length * 2];
    for (int i = 0; i < binary.length; i++) {
      hex[2*i] = HexLookupTable[(binary[i] & 0xFF) >>> 4];
      hex[2*i+1] = HexLookupTable[binary[i] & 0xF];
    }
    return new String(hex);
  }
}
