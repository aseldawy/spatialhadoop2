package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.esri.core.geometry.ogc.OGCGeometry;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class OSMPolygon extends OGCShape {
  public long id;
  
  public OSMPolygon() {}
  
  public OSMPolygon(OGCGeometry geom) {
    super(geom);
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, '\t');
    return super.toText(text);
  }
  
  @Override
  public void fromText(Text text) {
    id = TextSerializerHelper.consumeLong(text, '\t');
    super.fromText(text);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    super.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    super.readFields(in);
  }
  
  @Override
  public Shape clone() {
    OSMPolygon c = new OSMPolygon();
    c.id = this.id;
    c.geom = this.geom;
    return c;
  }
}
