/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.vividsolutions.jts.geom.Geometry;

import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class OSMPolygon extends OGCJTSShape implements WritableComparable<OSMPolygon> {
  private static final char SEPARATOR = '\t';
  public long id;
  public Map<String, String> tags;
  
  public OSMPolygon() {
    tags = new HashMap<String, String>();
  }
  
  public OSMPolygon(Geometry geom) {
    super(geom);
    tags = new HashMap<String, String>();
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, SEPARATOR);
    TextSerializerHelper.serializeGeometry(text, geom, SEPARATOR);
    TextSerializerHelper.serializeMap(text, tags);
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    id = TextSerializerHelper.consumeLong(text, SEPARATOR);
    this.geom = TextSerializerHelper.consumeGeometryJTS(text, SEPARATOR);
    // Read the tags
    tags.clear();
    TextSerializerHelper.consumeMap(text, tags);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    super.write(out);
    out.writeInt(tags.size());
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      out.writeUTF(tag.getKey());
      out.writeUTF(tag.getValue());
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    super.readFields(in);
    tags.clear();
    int size = in.readInt();
    while (size-- > 0) {
      String key = in.readUTF();
      String value = in.readUTF();
      tags.put(key, value);
    }
  }
  
  @Override
  public Shape clone() {
    OSMPolygon c = new OSMPolygon();
    c.id = this.id;
    c.geom = this.geom;
    c.tags = new HashMap<String, String>(tags);
    return c;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    return ((OSMPolygon)obj).id == this.id;
  }

  @Override
  public int compareTo(OSMPolygon poly) {
    if (this.id < poly.id)
      return -1;
    if (this.id > poly.id)
      return 1;
    return 0;
  }
  
  @Override
  public int hashCode() {
    return (int) (this.id % Integer.MAX_VALUE);
  }
  
  @Override
  public String toString() {
    return this.id + "\t" + this.geom.toText();
  }
}
