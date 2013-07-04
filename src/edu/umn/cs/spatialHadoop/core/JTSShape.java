package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

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
public class JTSShape implements Shape {
  
  private static final Log LOG = LogFactory.getLog(JTSShape.class);
  
  private static final byte[][] ShapeNames = { "LINESTRING".getBytes(),
      "POINT".getBytes(), "POLYGON".getBytes(), "MULTIPOINT".getBytes(),
      "MULTILINESTRING".getBytes(), "MULTIPOLYGON".getBytes(),
      "GEOMETRYCOLLECTION".getBytes() };
  
  private static final byte[] Separator = {','};
  
  private final WKTReader wktReader = new WKTReader();
  private final WKBWriter wkbWriter = new WKBWriter();
  private final WKBReader wkbReader = new WKBReader();
  
  /**
   * The underlying geometry
   */
  public Geometry geom;
  
  /**
   * Any extra data after the shape information
   */
  public String extra;
  
  public JTSShape() {
    this(null);
  }
  
  public JTSShape(Geometry geom) {
    this.geom = geom;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] wkb = wkbWriter.write(geom);
    out.writeInt(wkb.length);
    out.write(wkb);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      byte[] wkb = new byte[in.readInt()];
      in.readFully(wkb);
      geom = wkbReader.read(wkb);
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  @Override
  public Text toText(Text text) {
    //TextSerializerHelper.serializeLong(0, text, '\t');
    String str = WKBWriter.bytesToHex(wkbWriter.write(geom));
    byte[] str_b = str.getBytes();
    text.append(str_b, 0, str_b.length);
    if (extra != null) {
      text.append(Separator, 0, Separator.length);
      byte[] extra_bytes = extra.getBytes();
      text.append(extra_bytes, 0, extra_bytes.length);
    }
    return text;
  }
  
  public Geometry parseText(String str) throws ParseException {
    Geometry geom = null;
    try {
      // Parse string as well known text (WKT)
      geom = wktReader.read(str);
    } catch (ParseException e) {
      try {
        // Error parsing from WKT, try hex string instead
        byte[] binary = WKBReader.hexToBytes(str);
        geom = wkbReader.read(binary);
      } catch (RuntimeException e1) {
        // Cannot parse text. Just return null
      }
    }
    return geom;
  }


  @Override
  public void fromText(Text text) {
    // Read and skip a long
//    TextSerializerHelper.consumeLong(text, '\t');
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
    } catch (ParseException e) {
      LOG.error("Error parsing: "+text);
      e.printStackTrace();
    }
  }

  @Override
  public Rectangle getMBR() {
    if (geom.isEmpty())
      return null;
    com.vividsolutions.jts.geom.Polygon mbr = (com.vividsolutions.jts.geom.Polygon) geom.getEnvelope();
    LineString mbrr = mbr.getExteriorRing();
    int pointCount = mbrr.getNumPoints();
    double xmin = mbrr.getPointN(0).getX();
    double ymin = mbrr.getPointN(0).getY();
    double xmax = xmin, ymax = ymin;
    for (int i = 1; i < pointCount; i++) {
      com.vividsolutions.jts.geom.Point point = mbrr.getPointN(i);
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
    return this.distanceTo(x, y);
  }

  @Override
  public boolean isIntersected(Shape s) {
    Rectangle mbr = s.getMBR();
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(mbr.x1, mbr.y1);
    coordinates[1] = new Coordinate(mbr.x1, mbr.y2);
    coordinates[2] = new Coordinate(mbr.x2, mbr.y2);
    coordinates[3] = new Coordinate(mbr.x2, mbr.y1);
    coordinates[4] = coordinates[0];
    Polygon mbrPoly = geom.getFactory().createPolygon(geom.getFactory().createLinearRing(coordinates), null);
    
    return geom.intersects(mbrPoly);
  }

  @Override
  public Shape clone() {
    JTSShape copy = new JTSShape(this.geom);
    copy.extra = this.extra;
    return copy;
  }
  
  @Override
  public String toString() {
    return super.toString()+","+extra;
  }
  
  public static void main(String[] args) throws ParseException {
    String str = "POLYGON ((-78.647906 35.31423899999999, -78.647785 35.31423899999999, -78.647785 35.314638, -78.647906 35.314638, -78.647906 35.31423899999999))";
    Geometry read = new WKTReader().read(str);
    System.out.println(read.toText());
  }
}
