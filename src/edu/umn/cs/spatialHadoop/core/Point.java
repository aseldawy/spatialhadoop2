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

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A class that holds coordinates of a point.
 * @author aseldawy
 *
 */
public class Point implements Shape, Comparable<Point> {
	public double x;
	public double y;

	public Point() {
		this(0, 0);
	}
	
	public Point(double x, double y) {
	  set(x, y);
	}
	

	/**
	 * A copy constructor from any shape of type Point (or subclass of Point)
	 * @param s
	 */
	public Point(Point s) {
	  this.x = s.x;
	  this.y = s.y;
  }

  public void set(double x, double y) {
		this.x = x;
		this.y = y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}

	public void readFields(DataInput in) throws IOException {
		this.x = in.readDouble();
		this.y = in.readDouble();
	}

	public int compareTo(Shape s) {
	  Point pt2 = (Point) s;

	  // Sort by id
	  double difference = this.x - pt2.x;
		if (difference == 0) {
			difference = this.y - pt2.y;
		}
		if (difference == 0)
		  return 0;
		return difference > 0 ? 1 : -1;
	}
	
	public boolean equals(Object obj) {
		Point r2 = (Point) obj;
		return this.x == r2.x && this.y == r2.y;
	}
	
	public double distanceTo(Point s) {
		double dx = s.x - this.x;
		double dy = s.y - this.y;
		return Math.sqrt(dx*dx+dy*dy);
	}
	
	@Override
	public Point clone() {
	  return new Point(this.x, this.y);
	}

	/**
	 * Returns the minimal bounding rectangle of this point. This method returns
	 * the smallest rectangle that contains this point. For consistency with
	 * other methods such as {@link Rectangle#isIntersected(Shape)}, the rectangle
	 * cannot have a zero width or height. Thus, we use the method
	 * {@link Math#ulp(double)} to compute the smallest non-zero rectangle that
	 * contains this point. In other words, for a point <code>p</code> the
	 * following statement should return true.
	 * <code>p.getMBR().isIntersected(p);</code>
	 */
  @Override
  public Rectangle getMBR() {
    return new Rectangle(x, y, x + Math.ulp(x), y + Math.ulp(y));
  }

  @Override
  public double distanceTo(double px, double py) {
    double dx = x - px;
    double dy = y - py;
    return Math.sqrt(dx * dx + dy * dy);
  }

  public Shape getIntersection(Shape s) {
    return getMBR().getIntersection(s);
  }

  @Override
  public boolean isIntersected(Shape s) {
    return getMBR().isIntersected(s);
  }
  
  @Override
  public String toString() {
    return "Point: ("+x+","+y+")";
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeDouble(x, text, ',');
    TextSerializerHelper.serializeDouble(y, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    x = TextSerializerHelper.consumeDouble(text, ',');
    y = TextSerializerHelper.consumeDouble(text, '\0');
  }

  @Override
  public int compareTo(Point o) {
    if (x < o.x)
      return -1;
    if (x > o.x)
      return 1;
    if (y < o.y)
      return -1;
    if (y > o.y)
      return 1;
    return 0;
  }

  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
  		int imageHeight, double scale) {
    int imageX = (int) Math.round((this.x - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int imageY = (int) Math.round((this.y - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    g.fillRect(imageX, imageY, 1, 1);  	
  }

}
