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
  
  public static void main(String[] args) {
    OSMPolygon osmPolygon = new OSMPolygon();
    osmPolygon.fromText(new Text("24210654\tLINESTRING (-0.0577657 52.13279, -0.050634 52.135061, -0.0502107 52.1348983, -0.0501295 52.1330193, -0.0500104 52.1313194, -0.0494394 52.1240568, -0.0493721 52.1232002, -0.0493193 52.1225283, -0.0493023 52.1223117, -0.0492408 52.1212311, -0.0492226 52.1210101, -0.0491421 52.1200349, -0.0490886 52.1193859, -0.0490691 52.1191495, -0.0496187 52.1188248, -0.0498146 52.1187091, -0.0501721 52.1183965, -0.0501849 52.1183853, -0.0501872 52.1183786, -0.0503186 52.1180678, -0.0503542 52.1179206, -0.0503574 52.1177076, -0.050318 52.1175182, -0.0502568 52.1173377, -0.0500975 52.1171023, -0.0498567 52.1168515, -0.0494812 52.1166585, -0.0489563 52.1164301, -0.0488739 52.1163482, -0.048868 52.1162634, -0.0487581 52.1146915, -0.0486194 52.1125132, -0.0483745 52.1090856, -0.0481841 52.1072196, -0.0480658 52.1056749, -0.0480196 52.1051892, -0.0478171 52.1045764, -0.0476551 52.1044653, -0.0475583 52.1044732, -0.0471493 52.1046668, -0.0466877 52.1047821, -0.0462947 52.1044401, -0.0463644 52.1061158, -0.0463837 52.1065787, -0.0463907 52.1067472, -0.0465396 52.1083216, -0.0465631 52.108875, -0.0466147 52.1100919, -0.0466244 52.1102145, -0.0467014 52.1111894, -0.0467529 52.1118419, -0.0467909 52.1123229, -0.0467941 52.1123635, -0.0468472 52.1131085, -0.0468955 52.1137863, -0.0469715 52.1151632, -0.0470404 52.116411, -0.0469931 52.116502, -0.0462836 52.1168855, -0.0461375 52.1170154, -0.0459918 52.1172256, -0.0459678 52.1172603, -0.0459447 52.1173605, -0.045905 52.117533, -0.0458973 52.1176895, -0.0458878 52.1178833, -0.0458977 52.1182575, -0.0463755 52.1185984, -0.0464312 52.1186382, -0.0466117 52.1187515, -0.0466995 52.1188036, -0.0468873 52.1189161, -0.0472913 52.1191659, -0.0472971 52.1193232, -0.0474653 52.1219862, -0.0475883 52.1234301, -0.0476982 52.1235041, -0.0477384 52.1235479, -0.0478287 52.1236463, -0.0476401 52.1237711, -0.0476428 52.1238096, -0.0479403 52.1281076, -0.0480046 52.1290363, -0.0480339 52.1294596, -0.0480356 52.1294842, -0.0480392 52.1295354, -0.0481192 52.1306912, -0.048154 52.1311937, -0.0481917 52.1316477, -0.0483136 52.1331148, -0.0483251 52.1332528, -0.0483557 52.133746, -0.0479199 52.1342933, -0.0478419 52.1343556, -0.0470917 52.1340987, -0.0465048 52.1339916, -0.0460596 52.1339441, -0.0457841 52.1339606, -0.0450623 52.1340601, -0.0443103 52.1340978, -0.0439608 52.1340466, -0.0425153 52.133697, -0.04113 52.1336381, -0.0404323 52.1335732, -0.0398796 52.1335226, -0.0390829 52.1335169, -0.0388125 52.1334759, -0.0386403 52.1334237, -0.0385121 52.133416, -0.0379263 52.1336598, -0.0376691 52.1344641, -0.0371481 52.1356631, -0.0354429 52.1377422, -0.0379526 52.1392829, -0.0391794 52.1411385, -0.0394137 52.1421059, -0.0405036 52.1435102, -0.041259 52.1442866, -0.0378351 52.1463878, -0.0410741 52.147918, -0.0428826 52.1475191, -0.0471972 52.1466033, -0.0472739 52.1467288, -0.0470207 52.1470355, -0.0469684 52.1476344, -0.0465108 52.1477562, -0.0461479 52.1484175, -0.0458773 52.149129, -0.0457901 52.1496965, -0.0470754 52.1496586, -0.0472377 52.1496828, -0.0472954 52.1497572, -0.0474614 52.1509084, -0.0474244 52.1515937, -0.0483866 52.1516287, -0.0485582 52.1516349, -0.0486762 52.1516351, -0.0496477 52.1516369, -0.0498974 52.1516852, -0.0500554 52.1518831, -0.0502139 52.1520187, -0.0504454 52.1520985, -0.0506934 52.1521036, -0.0509225 52.1520488, -0.0511269 52.1520221, -0.0521206 52.1521255, -0.0522333 52.1521372, -0.0518846 52.1526083, -0.0513697 52.1530948, -0.0507951 52.1534904, -0.0502217 52.153782, -0.0497925 52.1540763, -0.0488723 52.1548456, -0.0482047 52.1554204, -0.0502592 52.1555815, -0.0532158 52.1557258, -0.0571447 52.1558778, -0.0572752 52.1500691, -0.0600823 52.1498687, -0.0601984 52.148683, -0.0582757 52.1475962, -0.0557096 52.1467033, -0.0558742 52.1444538, -0.0589901 52.1417421, -0.0613725 52.1427309, -0.0637813 52.1426315, -0.0629325 52.1405946, -0.0625134 52.138894, -0.0609301 52.1351854, -0.0608397 52.1349687, -0.0607592 52.134776, -0.0596392 52.1349848, -0.059117 52.1339102, -0.0602932 52.1336682, -0.0598752 52.1324874, -0.0577657 52.13279)\t"));
    System.out.println(osmPolygon);
  }
}
