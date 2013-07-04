package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;


public class OSMPoint extends Point {
  public long id;
  public Map<String, String> tags = new HashMap<String, String>();
  private static final byte[] Separators = {'[', '#', ',', ']'};

  @Override
  public void fromText(Text text) {
    id = TextSerializerHelper.consumeLong(text, '\t');
    x = TextSerializerHelper.consumeDouble(text, '\t');
    y = TextSerializerHelper.consumeDouble(text, '\t');
    tags.clear();
    if (text.getLength() > 0) {
      byte[] tagsBytes = text.getBytes();
      if (tagsBytes[0] != '[')
        return;
      int i1 = 1;
      while (i1 < text.getLength() && tagsBytes[i1] != ']') {
        int i2 = i1 + 1;
        while (i2 < text.getLength() && tagsBytes[i2] != '#')
          i2++;
        String key = new String(tagsBytes, i1, i2 - i1);
        i1 = i2 + 1;
        
        i2 = i1 + 1;
        while (i2 < text.getLength() && tagsBytes[i2] != ',' && tagsBytes[i2] != ']')
          i2++;
        String value = new String(tagsBytes, i1, i2 - i1);
        tags.put(key, value);
        i1 = i2;
        if (i1 < text.getLength() && tagsBytes[i1] == ',')
          i1++;
      }
    }
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, '\t');
    TextSerializerHelper.serializeDouble(x, text, '\t');
    TextSerializerHelper.serializeDouble(y, text, tags.isEmpty() ? '\0' : '\t');
    if (!tags.isEmpty()) {
      boolean first = true;
      text.append(Separators, 0, 1);
      for (Map.Entry<String, String> entry : tags.entrySet()) {
        if (first) {
          first = false;
        } else {
          first = true;
          text.append(Separators, 2, 1);
        }
        byte[] k = entry.getKey().getBytes();
        text.append(k, 0, k.length);
        text.append(Separators, 1, 1);
        byte[] v = entry.getValue().getBytes();
        text.append(v, 0, v.length);
      }
      text.append(Separators, 3, 1);
    }
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
