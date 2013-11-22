package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class NASAPoint extends Point {
  
  private static final byte[] Separator = {','};
  
  /**Value stored at this point*/
  public int value;

  public NASAPoint() {
  }

  public NASAPoint(double x, double y, int value) {
    super(x, y);
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(value);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.value = in.readInt();
  }
  
  @Override
  public Text toText(Text text) {
    super.toText(text);
    text.append(Separator, 0, Separator.length);
    TextSerializerHelper.serializeInt(value, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    super.fromText(text);
    byte[] bytes = text.getBytes();
    text.set(bytes, 1, text.getLength() - 1);
    value = TextSerializerHelper.consumeInt(text, '\0');
  }
  
  @Override
  public String toString() {
    return super.toString() + " - "+value;
  }
  
  public static void main(String[] args) {
    Point pt = new Point(-120.03958630302645,39.99999999640791);
    Rectangle rect = new Rectangle(-123.58434601185886,34.94999999686139,-120.03958630302645,1.7976931348623157E308);
    System.out.println(pt.isIntersected(rect));
    System.out.println(rect.getIntersection(pt));
  }
}
