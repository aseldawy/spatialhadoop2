/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.awt.Color;
import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A data type used to index edges for TAREEQ project.
 * @author Ahmed Eldawy
 */
public class OSMEdge implements Shape {
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(OSMEdge.class);
  
  public long edgeId;
  public long nodeId1;
  public double lat1, lon1;
  public long nodeId2;
  public double lat2, lon2;
  public long wayId;
  public String tags;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(edgeId);
    out.writeLong(nodeId1);
    out.writeDouble(lat1);
    out.writeDouble(lon1);
    out.writeLong(nodeId2);
    out.writeDouble(lat2);
    out.writeDouble(lon2);
    out.writeLong(wayId);
    out.writeUTF(tags);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    edgeId = in.readLong();
    nodeId1 = in.readLong();
    lat1 = in.readDouble();
    lon1 = in.readDouble();
    nodeId2 = in.readLong();
    lat2 = in.readDouble();
    lon2 = in.readDouble();
    wayId = in.readLong();
    tags = in.readUTF();
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(edgeId, text, ',');
    TextSerializerHelper.serializeLong(nodeId1, text, ',');
    TextSerializerHelper.serializeDouble(lat1, text, ',');
    TextSerializerHelper.serializeDouble(lon1, text, ',');
    TextSerializerHelper.serializeLong(nodeId2, text, ',');
    TextSerializerHelper.serializeDouble(lat2, text, ',');
    TextSerializerHelper.serializeDouble(lon2, text, ',');
    TextSerializerHelper.serializeLong(wayId, text, ',');
    if (tags != null) {
      byte[] tagsBytes = tags.getBytes();
      text.append(tagsBytes, 0, tagsBytes.length);
    }
    return text;
  }

  @Override
  public void fromText(Text text) {
    edgeId = TextSerializerHelper.consumeLong(text, ',');
    nodeId1 = TextSerializerHelper.consumeLong(text, ',');
    lat1 = TextSerializerHelper.consumeDouble(text, ',');
    lon1 = TextSerializerHelper.consumeDouble(text, ',');
    nodeId2 = TextSerializerHelper.consumeLong(text, ',');
    lat2 = TextSerializerHelper.consumeDouble(text, ',');
    lon2 = TextSerializerHelper.consumeDouble(text, ',');
    wayId = TextSerializerHelper.consumeLong(text, ',');
    tags = text.toString();
  }

  @Override
  public Rectangle getMBR() {
    double min_lon, max_lon;
    if (lon1 < lon2) {
      min_lon = lon1;
      max_lon = lon2;
    } else {
      min_lon = lon2;
      max_lon = lon1;
    }
    double min_lat, max_lat;
    if (lat1 < lat2) {
      min_lat = lat1;
      max_lat = lat2;
    } else {
      min_lat = lat2;
      max_lat = lat1;
    }
    return new Rectangle(min_lon, min_lat, max_lon, max_lat);
  }

  @Override
  public double distanceTo(double x, double y) {
    return getMBR().distanceTo(x, y);
  }

  @Override
  public boolean isIntersected(Shape s) {
    return getMBR().isIntersected(s);
  }

  @Override
  public OSMEdge clone() {
    OSMEdge c = new OSMEdge();
    c.edgeId = this.edgeId;
    c.nodeId1 = this.nodeId1;
    c.lat1 = this.lat1;
    c.lon1 = this.lon1;
    c.nodeId2 = this.nodeId2;
    c.lat2 = this.lat2;
    c.lon2 = this.lon2;
    c.wayId = this.wayId;
    c.tags = this.tags;
    return c;
  }
  
  public double getLength() {
    // TODO use a more accurate function that takes earth roundness into effect
    double dlat = this.lat1 - this.lat2;
    double dlon = this.lon1 - this.lon2;
    return Math.sqrt(dlat*dlat + dlon*dlon);
  }
  
  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    int x1 = (int) ((this.lon1 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int y1 = (int) ((this.lat1 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    int x2 = (int) ((this.lon2 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int y2 = (int) ((this.lat2 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    Color shape_color = g.getColor();
    
    // Compute alpha to use based on edge length and image scale
    double geom_alpha = this.getLength() * scale;
    int color_alpha = geom_alpha > 1.0 ? 255 : (int) Math.round(geom_alpha * 255);
    if (color_alpha == 0)
      return;

    g.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
    g.drawLine(x1, y1, x2, y2);
  }
  
  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    int x1 = (int) Math.round(this.lon1 * xscale);
    int y1 = (int) Math.round(this.lat1 * yscale);
    int x2 = (int) Math.round(this.lon2 * xscale);
    int y2 = (int) Math.round(this.lat2 * yscale);
    g.drawLine(x1, y1, x2, y2);
  }
}
