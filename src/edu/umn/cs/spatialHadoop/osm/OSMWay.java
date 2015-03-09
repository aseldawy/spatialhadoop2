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
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
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
 * A UDF for Pig to parse an XML node representing a way in OSM XML file.
 * @author Ahmed Eldawy
 *
 */
public class OSMWay extends EvalFunc<Tuple> {
  private DocumentBuilderFactory dbFactory;
  private DocumentBuilder dBuilder;

  public OSMWay() throws ParserConfigurationException {
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
      if (rootElement.getNodeName() != "way")
        throw new RuntimeException("Passed element must be <way>");
      
      // Get all node attributes
      Long id = rootElement.getAttribute("id").isEmpty() ? null :
        Long.parseLong(rootElement.getAttribute("id"));
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
      
      // Get all contained nodes
      NodeList nodes = rootElement.getElementsByTagName("nd");
      DataBag nodesBag = BagFactory.getInstance().newDefaultBag();
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);
        if (node.getNodeType() == Node.ELEMENT_NODE) {
          int node_pos = (int) (nodesBag.size() + 1);
          long node_id = Long.parseLong(((Element)node).getAttribute("ref"));
          Tuple nodeTuple = TupleFactory.getInstance().newTuple(2);
          nodeTuple.set(0, node_pos);
          nodeTuple.set(1, node_id);
          nodesBag.add(nodeTuple);
        }
      }
      
      
      // Create the tuple to be returned
      Tuple newTuple = TupleFactory.getInstance().newTuple(10);
      newTuple.set(0, id);
      newTuple.set(1, user);
      newTuple.set(2, uid);
      newTuple.set(3, visible);
      newTuple.set(4, version);
      newTuple.set(5, changeset);
      newTuple.set(6, timestamp);
      newTuple.set(7, tagsMap);
      newTuple.set(8, nodesBag);
      return newTuple;
    } catch (Exception e) {
      throw new IOException("Error parsing: "+input.get(0), e);
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema waySchema = new Schema();
      waySchema.add(new Schema.FieldSchema("id", DataType.LONG));
      waySchema.add(new Schema.FieldSchema("user", DataType.CHARARRAY));
      waySchema.add(new Schema.FieldSchema("uid", DataType.LONG));
      waySchema.add(new Schema.FieldSchema("visible", DataType.BOOLEAN));
      waySchema.add(new Schema.FieldSchema("version", DataType.INTEGER));
      waySchema.add(new Schema.FieldSchema("changeset", DataType.INTEGER));
      waySchema.add(new Schema.FieldSchema("timestamp", DataType.CHARARRAY));
      Schema tagSchema = new Schema();
      tagSchema.add(new Schema.FieldSchema("value", DataType.CHARARRAY));
      FieldSchema temp = new Schema.FieldSchema("tags", tagSchema);
      temp.type = DataType.MAP;
      waySchema.add(temp);
      
      Schema nodeSchema = new Schema();
      nodeSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
      nodeSchema.add(new Schema.FieldSchema("node_id", DataType.LONG));
      temp = new Schema.FieldSchema("nodes", nodeSchema);
      temp.type = DataType.BAG;
      waySchema.add(temp);
      
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
          .getName().toLowerCase(), input), waySchema, DataType.TUPLE));
    } catch (Exception e) {
      return null;
    }
  }
  
  public static void main(String[] args) throws IOException, ParserConfigurationException {
    String xml = "<way id='157358729' user='Mulad' uid='502142' visible='true' version='1' changeset='11134075' timestamp='2012-03-29T06:39:53Z'>" +
                 "<nd ref='1695772908'/>" +
                 "<nd ref='1695772884'/>" +
                 "<nd ref='1695772853'/>" +
                 "<tag k='highway' v='footway'/>"+
                 "</way>";
    Tuple tuple = TupleFactory.getInstance().newTuple(xml);
    System.out.println(new OSMWay().exec(tuple));
  }
}
