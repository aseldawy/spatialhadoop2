package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A shape from tiger file.
 * @author aseldawy
 *
 */
public class TigerShape extends Rectangle {
  
  private static final double Precision = 1E+10;
  public long id;
  public int extraInfoLength;
  public byte[] extraInfo;

  public TigerShape() {
  }
  
  public TigerShape(Rectangle rect, long id) {
    super(rect);
    this.id = id;
  }

  public TigerShape(TigerShape ts) {
    super(ts);
    this.id = ts.id;
    this.extraInfoLength = ts.extraInfoLength;
    if (extraInfoLength > 0) {
      this.extraInfo = new byte[ts.extraInfo.length];
      System.arraycopy(ts.extraInfo, 0, this.extraInfo, 0, extraInfoLength);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    super.write(out);
    out.writeInt(extraInfoLength);
    if (extraInfoLength > 0) {
      out.write(extraInfo, 0, extraInfoLength);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    super.readFields(in);
    extraInfoLength = in.readInt();
    if (extraInfoLength > 0) {
      if (extraInfo == null || extraInfo.length < extraInfoLength) {
        // Get the next power of two for the new extraInfoLength
        int new_capacity = extraInfoLength;
        new_capacity |= new_capacity >> 1;
        new_capacity |= new_capacity >> 2;
        new_capacity |= new_capacity >> 4;
        new_capacity |= new_capacity >> 8;
        new_capacity |= new_capacity >> 16;
        new_capacity |= new_capacity >> 32;
        new_capacity++;
        extraInfo = new byte[new_capacity];
      }
      in.readFully(extraInfo, 0, extraInfoLength);
    }
  }

  @Override
  public int compareTo(Shape s) {
    TigerShape ts = (TigerShape) s;
    if (id < ts.id)
      return -1;
    if (id > ts.id)
      return 1;
    return 0;
  }
  
  @Override
  public boolean equals(Object obj) {
    return id == ((TigerShape)obj).id;
  }
  
  @Override
  public int hashCode() {
    return (int)id;
  }

  @Override
  public TigerShape clone() {
    return new TigerShape(this);
  }
  
  @Override
  public String toString() {
    return toText(new Text()).toString();
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, ',');
    TextSerializerHelper.serializeDouble((double)x1 / Precision, text, ',');
    TextSerializerHelper.serializeDouble((double)y1 / Precision, text, ',');
    TextSerializerHelper.serializeDouble((double)x2 / Precision, text, ',');
    TextSerializerHelper.serializeDouble((double)y2 / Precision, text, ',');
    // TODO handle the case when extraInfo contains a new line character
    if (extraInfoLength > 0)
      text.append(extraInfo, 0, extraInfoLength);
    return text;
  }

  @Override
  public void fromText(Text text) {
    this.id = TextSerializerHelper.consumeLong(text, ',');
    this.x1 = TextSerializerHelper.consumeDouble(text, ',');
    this.y1 = TextSerializerHelper.consumeDouble(text, ',');
    this.x2 = TextSerializerHelper.consumeDouble(text, ',');;
    this.y2 = TextSerializerHelper.consumeDouble(text, ',');
    extraInfoLength = text.getLength();
    if (extraInfo == null || extraInfo.length < extraInfoLength) {
      // Get the next power of two for the new extraInfoLength
      int new_capacity = extraInfoLength;
      new_capacity |= new_capacity >> 1;
      new_capacity |= new_capacity >> 2;
      new_capacity |= new_capacity >> 4;
      new_capacity |= new_capacity >> 8;
      new_capacity |= new_capacity >> 16;
      new_capacity |= new_capacity >> 32;
      new_capacity++;
      extraInfo = new byte[new_capacity];
    }
    System.arraycopy(text.getBytes(), 0, extraInfo, 0, extraInfoLength);
  }
}
