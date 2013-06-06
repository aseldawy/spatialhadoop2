package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class Partition extends Rectangle {
  /**Name of the file that contains the data*/
  public String filename;
  
  public Partition() {}
  
  public Partition(String filename, Rectangle mbr) {
    this.filename = filename;
    this.set(mbr);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(filename);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    filename = in.readUTF();
  }
  
  @Override
  public Text toText(Text text) {
    super.toText(text);
    byte[] temp = (","+filename).getBytes();
    text.append(temp, 0, temp.length);
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    super.fromText(text);
    // Skip the comma and read filename
    filename = new String(text.getBytes(), 1, text.getLength() - 1);
  }
  
  @Override
  public Partition clone() {
    return new Partition(filename, this);
  }
}