package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCPoint;

/**
 * A shape class that represents an OGC compliant geometry. The geometry is
 * enclosed inside the class and all calls are delegated to it. The class also
 * holds extra information for each records that could represent other columns
 * for each records. The text representation is assumed to be some kind of CSV.
 * The shape is always the first column in that CSV. The text representation of
 * the shape could be either a WTK (Well Known Text) or a binary representation.
 * The WKT can be generated with PostGIS using the function ST_AsText(geom). An
 * example may look like this:<br/>
 * <code>
 * POLYGON ((-89 43,-89 50,-97 50,-97 43,-89 43))
 * </code> The binary representation can be generated from PostGIS by selecting
 * the geom column using a normal select statement. When a shape is parsed, we
 * detect the format and use the appropriate parser. When writing to text, we
 * always use the binary representation as it is faster and more compact. For
 * binary serialization/deserialization, we use the PostGIS writer and parser.
 * 
 * @author Ahmed Eldawy
 * 
 */
public class OGCShape implements Shape {
  
  private static final Log LOG = LogFactory.getLog(OGCShape.class);
  
  private static final byte[][] ShapeNames = { "LINESTRING".getBytes(),
      "POINT".getBytes(), "POLYGON".getBytes(), "MULTIPOINT".getBytes(),
      "MULTILINESTRING".getBytes(), "MULTIPOLYGON".getBytes(),
      "GEOMETRYCOLLECTION".getBytes() };
  
  private static final byte[] Separator = {','};
  
  /**
   * The underlying geometry
   */
  public OGCGeometry geom;
  
  /**
   * Any extra data after the shape information
   */
  public String extra;
  
  public OGCShape() {
    this(null);
  }
  
  public OGCShape(OGCGeometry geom) {
    this.geom = geom;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = geom.asBinary().array();
    out.writeInt(bytes.length);
    out.write(bytes);
    if (extra == null) {
      out.writeInt(-1);
    } else {
      bytes = extra.getBytes();
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    byte[] bytes = new byte[size];
    in.readFully(bytes);
    geom = OGCGeometry.fromBinary(ByteBuffer.wrap(bytes));
    size = in.readInt();
    if (size == -1) {
      extra = null;
    } else {
      bytes = new byte[size];
      in.readFully(bytes);
      extra = new String(bytes);
    }
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
        throw new RuntimeException("Invalid hex char "+x);
      if (i % 2 == 0)
        x <<= 4;
      bytes[i / 2] |= x;
    }
    return bytes;
  }
  
  @Override
  public Text toText(Text text) {
    String str = bytesToHex(geom.asBinary().array());
    text.set(str);
    if (extra != null) {
      text.append(Separator, 0, Separator.length);
      byte[] extra_bytes = extra.getBytes();
      text.append(extra_bytes, 0, extra_bytes.length);
    }
    return text;
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
        // Cannot parse text. Just return null
      }
    }
    return geom;
  }


  @Override
  public void fromText(Text text) {
    try {
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
      byte terminator = Separator[0];
      int i1 = 0;
      if (bytes[i1] == '\'' || bytes[i1] == '\"') {
        terminator = bytes[i1++];
      }
      int i2 = i1;
      while (i2 < length && bytes[i2] != terminator)
        i2++;
      
      String str = new String(bytes, i1, i2-i1);
      geom = parseText(str);
      
      if (++i2 < length) {
        extra = new String(bytes, i2, length - i2);
      } else {
        extra = null;
      }
    } catch (RuntimeException e) {
      LOG.error("Error parsing: "+text);
      throw e;
    }
  }

  @Override
  public Rectangle getMBR() {
    if (geom.isEmpty())
      return null;
    com.esri.core.geometry.Polygon mbr = (com.esri.core.geometry.Polygon) geom.envelope().getEsriGeometry();
    int pointCount = mbr.getPointCount();
    double xmin = mbr.getPoint(0).getX();
    double ymin = mbr.getPoint(0).getY();
    double xmax = xmin, ymax = ymin;
    for (int i = 1; i < pointCount; i++) {
      com.esri.core.geometry.Point point = mbr.getPoint(i);
      if (point.getX() < xmin)
        xmin = point.getX();
      if (point.getX() > xmax)
        xmax = point.getX();
      if (point.getY() < ymin)
        ymin = point.getY();
      if (point.getY() > ymax)
        ymax = point.getY();
    }
    
    return new Rectangle(xmin, ymin, xmax, ymax);
  }

  @Override
  public double distanceTo(double x, double y) {
    OGCPoint point = new OGCPoint(new com.esri.core.geometry.Point(x, y), this.geom.getEsriSpatialReference());
    return this.geom.distance(point);
  }

  @Override
  public boolean isIntersected(Shape s) {
    Rectangle mbr = s.getMBR();
    ArrayList<OGCGeometry> points = new ArrayList<OGCGeometry>();
    points.add(new OGCPoint(new com.esri.core.geometry.Point(mbr.x1, mbr.y1), geom.getEsriSpatialReference()));
    points.add(new OGCPoint(new com.esri.core.geometry.Point(mbr.x2, mbr.y2), geom.getEsriSpatialReference()));
    
    OGCGeometryCollection all_points = new OGCConcreteGeometryCollection(points, geom.getEsriSpatialReference());
    
    return geom.intersects(all_points.envelope());
  }

  @Override
  public Shape clone() {
    OGCShape copy = new OGCShape(this.geom);
    copy.extra = this.extra;
    return copy;
  }
  
  @Override
  public String toString() {
    return super.toString()+","+extra;
  }
}
