package edu.umn.cs.spatialHadoop.core;

import java.awt.Color;
import java.awt.Graphics;
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
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, boolean vflip, double scale) {
    final int MinValue = 7500;
    final int MaxValue = 16000;
    int imageX = (int) ((this.x - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int imageY = (int) (((vflip? -this.y : this.y) - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    
    if (value > 0 && imageX >= 0 && imageX < imageWidth && imageY >= 0 && imageY < imageHeight) {
      Color color;
      if (value < MinValue) {
        color = Color.BLACK;
      } else if (value < MaxValue) {
        float ratio = 0.78f - 0.78f * (value - MinValue) / (MaxValue - MinValue);
        color = Color.getHSBColor(ratio, 0.5f, 1.0f);
      } else {
        color = Color.WHITE;
      }
      g.setColor(color);
      g.fillRect(imageX, imageY, 1, 1);
    }
  }
}
