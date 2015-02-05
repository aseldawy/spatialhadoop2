/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A UDF for Pig to parse an XML element representing a node from OSM XML file.
 * @author Ahmed Eldawy
 *
 */
public class OSMNode extends EvalFunc<Tuple> {
  private DocumentBuilderFactory dbFactory;
  private DocumentBuilder dBuilder;

  public OSMNode() throws ParserConfigurationException {
    dbFactory = DocumentBuilderFactory.newInstance();
    dBuilder = dbFactory.newDocumentBuilder();
  }

  @Override
  public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try {
      String str = (String)input.get(0);
      ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes("UTF8"));
      Document doc = dBuilder.parse(in);
      
      Element rootElement = doc.getDocumentElement();
      rootElement.normalize();
      if (rootElement.getNodeName() != "node")
        throw new RuntimeException("Passed element must be <node>");
      
      // Get all node attributes
      Long id = rootElement.getAttribute("id").isEmpty() ? null :
        Long.parseLong(rootElement.getAttribute("id"));
      Double lat = rootElement.getAttribute("lat").isEmpty() ? null :
        Double.parseDouble(rootElement.getAttribute("lat"));
      Double lon = rootElement.getAttribute("lon").isEmpty() ? null :
        Double.parseDouble(rootElement.getAttribute("lon"));
      String user = rootElement.getAttribute("user");
      Long uid = rootElement.getAttribute("uid").isEmpty() ? null :
        Long.parseLong(rootElement.getAttribute("uid"));
      Boolean visible = rootElement.getAttribute("true").isEmpty() ? null :
        Boolean.parseBoolean(rootElement.getAttribute("true"));
      Integer version = rootElement.getAttribute("version").isEmpty() ? null :
        Integer.parseInt(rootElement.getAttribute("version"));
      Integer changeset = rootElement.getAttribute("changeset").isEmpty()? null:
          Integer.parseInt(rootElement.getAttribute("changeset"));
      String timestamp = rootElement.getAttribute("timestamp");
      
      // Get all contained tags
      NodeList tags = rootElement.getElementsByTagName("tag");
      Map<String, String> tagsMap = new HashMap<String, String>();
      for (int i = 0; i < tags.getLength(); i++) {
        Node tag = tags.item(i);
        if (tag.getNodeType() == Node.ELEMENT_NODE) {
          String key = ((Element)tag).getAttribute("k");
          String value = ((Element)tag).getAttribute("v");
          key = key.replaceAll("[\\s]", " ").replaceAll("[\\\'\\\"#,]", "_");
          value = value.replaceAll("[\\s]", " ").replaceAll("[\\\'\\\"#,]", "_");
          tagsMap.put(key, value);
        }
      }
      
      // Create the tuple to be returned
      Tuple newTuple = TupleFactory.getInstance().newTuple(10);
      newTuple.set(0, id);
      newTuple.set(1, lat);
      newTuple.set(2, lon);
      newTuple.set(3, user);
      newTuple.set(4, uid);
      newTuple.set(5, visible);
      newTuple.set(6, version);
      newTuple.set(7, changeset);
      newTuple.set(8, timestamp);
      newTuple.set(9, tagsMap);
      return newTuple;
    } catch (Exception e) {
      throw new IOException("Error parsing: "+input.get(0), e);
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema nodeSchema = new Schema();
      nodeSchema.add(new Schema.FieldSchema("id", DataType.LONG));
      nodeSchema.add(new Schema.FieldSchema("lat", DataType.DOUBLE));
      nodeSchema.add(new Schema.FieldSchema("lon", DataType.DOUBLE));
      nodeSchema.add(new Schema.FieldSchema("user", DataType.CHARARRAY));
      nodeSchema.add(new Schema.FieldSchema("uid", DataType.LONG));
      nodeSchema.add(new Schema.FieldSchema("visible", DataType.BOOLEAN));
      nodeSchema.add(new Schema.FieldSchema("version", DataType.INTEGER));
      nodeSchema.add(new Schema.FieldSchema("changeset", DataType.INTEGER));
      nodeSchema.add(new Schema.FieldSchema("timestamp", DataType.CHARARRAY));
      Schema tagSchema = new Schema();
      tagSchema.add(new Schema.FieldSchema("value", DataType.CHARARRAY));
      FieldSchema temp = new Schema.FieldSchema("tags", tagSchema);
      temp.type = DataType.MAP;
      nodeSchema.add(temp);

      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
          .getName().toLowerCase(), input), nodeSchema, DataType.TUPLE));
    } catch (Exception e) {
      return null;
    }
  }
  
  public static void main(String[] args) throws IOException, ParserConfigurationException {
    String text = "ahmed\t\nmohamed''\"";
    System.out.println(text);
    System.out.println(text.replaceAll("[\\s]", " ").replaceAll("[\\\'\\\"]", "_"));
  }
}
