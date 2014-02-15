/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.esri.core.geometry.ogc.OGCGeometry;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class OSMPolygon extends OGCShape implements WritableComparable<OSMPolygon> {
  public long id;
  public Map<String, String> tags;
  
  public OSMPolygon() {
    tags = new HashMap<String, String>();
  }
  
  public OSMPolygon(OGCGeometry geom) {
    super(geom);
    tags = new HashMap<String, String>();
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, '\t');
    return super.toText(text);
  }
  
  @Override
  public void fromText(Text text) {
    id = TextSerializerHelper.consumeLong(text, '\t');
    this.geom = TextSerializerHelper.consumeGeometryESRI(text, '\t');
    // Remove the separator
    text.set(text.getBytes(), 1, text.getLength() - 1);
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
}
