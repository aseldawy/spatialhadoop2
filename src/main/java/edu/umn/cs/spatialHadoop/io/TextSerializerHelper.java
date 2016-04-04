/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.io;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;

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
   * @param toAppend
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
    double d = Double.parseDouble(new String(bytes, 0, i));
    if (i < text.getLength() && bytes[i] == separator)
      i++;
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return d;
  }
  
  /**
   * Appends hex representation of the given number to the given string.
   * If append is set to true, a comma is also appended to the text.
   * @param d
   * @param t
   * @param toAppend
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
      if (i1 < text.getLength())
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
    OGCGeometry geom;
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
    if (toAppend != '\0')
      text.append(new byte[] {(byte) toAppend}, 0, 1);
  }
  
  private static final WKTReader wktReader = new WKTReader(new GeometryFactory());
  private static final WKBReader wkbReader = new WKBReader(new GeometryFactory());
  
  public static void serializeGeometry(Text text, Geometry geom, char toAppend) {
    String wkt = geom == null? "" : geom.toText();
    byte[] wkt_b = wkt.getBytes();
    text.append(wkt_b, 0, wkt_b.length);
    if (toAppend != '\0')
      text.append(new byte[] {(byte) toAppend}, 0, 1);
  }
  
  public static synchronized Geometry consumeGeometryJTS(Text text, char separator) {
    // Check whether this text is a Well Known Text (WKT) or a hexed string
    boolean wkt = false;
    byte[] bytes = text.getBytes();
    int length = text.getLength();
    Geometry geom;
    int i1, i2; // Start and end offset of the geometry being parsed
    int i_next; // Beginning of the next field
    boolean isWKT = false;
    boolean isHex = false;
    if (bytes[0] == '\'' || bytes[0] == '\"') {
      // A quoted string. Find terminating quote and trim the quotes
      i1 = 1;
      i2 = 2;
      while (i2 < length && bytes[i2] != bytes[0])
        i2++;
      if (i2 == length)
        throw new RuntimeException("Unterminated quoted string");
      i_next = i2 + 1;
      i2--; // Back one step to remove the terminating quote
      isWKT = true; // Assume any quoted string to be WKT
    } else {
      // Not a quoted string, check if the type is WKT
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

      if (i_shape < ShapeNames.length) {
        isWKT = true;
        // Look for the terminator of the shape text
        i1 = 0;
        i2 = 1;
        // Search for the first open parenthesis
        while (i2 < length && bytes[i2] != '(')
          i2++;
        if (i2 < length)
          i2++; // Skip the open parenthesis itself
        int nesting = 1;
        while (i2 < length && nesting > 0) {
          if (bytes[i2] == '(')
            nesting++;
          else if (bytes[i2] == ')')
            nesting--;
          i2++;
        }
        i_next = i2 + 1;
      } else {
        // Check if the type is hex-encoded WKB
        i1 = 0;
        i2 = 0;
        while (i2 < length && IsHex[bytes[i2]])
          i2++;
        isHex = i2 > 1;
        i_next = i2;
      }
    }

    String geom_text = new String(bytes, i1, i2);
    
    try {
      if (isWKT) {
        geom = wktReader.read(geom_text);
      } else if (isHex) {
        byte[] binary = hexToBytes(geom_text);
        geom = wkbReader.read(binary);
      } else {
        geom = null;
      }
    } catch (ParseException e) {
      throw new RuntimeException(String.format("Error parsing '%s'",geom_text), e);
    }

    // Remove consumed bytes from the text
    if (i_next >= text.getLength())
      text.clear();
    else {
      if (bytes[i_next] == separator)
        i_next++;
      text.set(bytes, i_next, length - i_next);
    }

    return geom;
  }
  
  private static final boolean[] IsHex = new boolean[256];
  
  private static final byte[] HexLookupTable = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };
  
  static {
    for (char c = '0'; c <= '9'; c++)
      IsHex[c] = true;
    for (char c = 'A'; c <= 'F'; c++)
      IsHex[c] = true;
    for (char c = 'a'; c <= 'f'; c++)
      IsHex[c] = true;
  }
  
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
