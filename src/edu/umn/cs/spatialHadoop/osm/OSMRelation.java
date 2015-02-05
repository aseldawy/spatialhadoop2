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
 * A UDF for Pig to parse an XML element representing a relation from OSM XML file.
 * @author Ahmed Eldawy
 *
 */
public class OSMRelation extends EvalFunc<Tuple> {
  private DocumentBuilderFactory dbFactory;
  private DocumentBuilder dBuilder;

  public OSMRelation() throws ParserConfigurationException {
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
      if (rootElement.getNodeName() != "relation")
        throw new RuntimeException("Passed element must be <relation>");
      
      // Get all node attributes
      Long id = rootElement.getAttribute("id").isEmpty() ? null :
        Long.parseLong(rootElement.getAttribute("id"));
      String user = rootElement.getAttribute("user");
      Long uid = rootElement.getAttribute("uid").isEmpty() ? null :
        Long.parseLong(rootElement.getAttribute("uid"));
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
      
      // Get all contained members
      NodeList members = rootElement.getElementsByTagName("member");
      DataBag memberBag = BagFactory.getInstance().newDefaultBag();
      for (int i = 0; i < members.getLength(); i++) {
        Node member = members.item(i);
        if (member.getNodeType() == Node.ELEMENT_NODE) {
          long member_id = Long.parseLong(((Element)member).getAttribute("ref"));
          String member_type = ((Element)member).getAttribute("type");
          String member_role = ((Element)member).getAttribute("role");
          Tuple memberTuple = TupleFactory.getInstance().newTuple(3);
          memberTuple.set(0, member_type);
          memberTuple.set(1, member_id);
          memberTuple.set(2, member_role);
          memberBag.add(memberTuple);
        }
      }

      // Create the tuple to be returned
      Tuple newTuple = TupleFactory.getInstance().newTuple(8);
      newTuple.set(0, id);
      newTuple.set(1, user);
      newTuple.set(2, uid);
      newTuple.set(3, version);
      newTuple.set(4, changeset);
      newTuple.set(5, timestamp);
      newTuple.set(6, memberBag);
      newTuple.set(7, tagsMap);
      return newTuple;
    } catch (Exception e) {
      throw new IOException("Error parsing: "+input.get(0), e);
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema relationSchema = new Schema();
      relationSchema.add(new Schema.FieldSchema("id", DataType.LONG));
      relationSchema.add(new Schema.FieldSchema("user", DataType.CHARARRAY));
      relationSchema.add(new Schema.FieldSchema("uid", DataType.LONG));
      relationSchema.add(new Schema.FieldSchema("version", DataType.INTEGER));
      relationSchema.add(new Schema.FieldSchema("changeset", DataType.INTEGER));
      relationSchema.add(new Schema.FieldSchema("timestamp", DataType.CHARARRAY));

      Schema memberSchema = new Schema();
      memberSchema.add(new Schema.FieldSchema("member_type", DataType.CHARARRAY));
      memberSchema.add(new Schema.FieldSchema("member_id", DataType.LONG));
      memberSchema.add(new Schema.FieldSchema("member_role", DataType.CHARARRAY));
      FieldSchema temp = new Schema.FieldSchema("members", memberSchema);
      temp.type = DataType.BAG;
      relationSchema.add(temp);
      
      Schema tagSchema = new Schema();
      tagSchema.add(new Schema.FieldSchema("value", DataType.CHARARRAY));
      temp = new Schema.FieldSchema("tags", tagSchema);
      temp.type = DataType.MAP;
      relationSchema.add(temp);
      
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
          .getName().toLowerCase(), input), relationSchema, DataType.TUPLE));
    } catch (Exception e) {
      return null;
    }
  }
}
