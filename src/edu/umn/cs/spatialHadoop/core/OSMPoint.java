package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;


public class OSMPoint extends Point {
  public long id;
  
  @Override
  public void fromText(Text text) {
    id = TextSerializerHelper.consumeLong(text, '\t');
    x = TextSerializerHelper.consumeDouble(text, '\t');
    y = TextSerializerHelper.consumeDouble(text, '\0');
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, '\t');
    TextSerializerHelper.serializeDouble(x, text, '\t');
    TextSerializerHelper.serializeDouble(y, text, '\0');
    return text;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    super.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readLong();
    super.readFields(in);
  }
  
  @Override
  public Point clone() {
    OSMPoint c = new OSMPoint();
    c.id = id;
    c.x = x;
    c.y = y;
    return c;
  }
}
