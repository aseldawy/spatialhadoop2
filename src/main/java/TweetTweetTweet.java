/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.hadoop.io.Text;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Eldawy
 *
 */
public class TweetTweetTweet implements Shape {
  Tweet tweet1, tweet2, tweet3;
  private final byte[] Tab = "\t".getBytes();

  public TweetTweetTweet() {
    tweet1 = new Tweet();
    tweet2 = new Tweet();
    tweet3 = new Tweet();
  }

  public TweetTweetTweet(Tweet tweet1, Tweet tweet2, Tweet tweet3) {
    this.tweet1 = new Tweet(tweet1);
    this.tweet2 = new Tweet(tweet2);
    this.tweet3 = new Tweet(tweet3);
  }

  @Override
  public void fromText(Text text) {
    tweet1.fromText(text);
    // Skip the Tab
    text.set(text.getBytes(), 1, text.getLength() - 1);
    tweet2.fromText(text);
    // Skip the Tab
    text.set(text.getBytes(), 1, text.getLength() - 1);
    tweet3.fromText(text);
  }
  
  @Override
  public Text toText(Text text) {
    tweet1.toText(text);
    text.append(Tab, 0, Tab.length);
    tweet2.toText(text);
    text.append(Tab, 0, Tab.length);
    tweet3.toText(text);
    return text;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    tweet1.write(out);
    tweet2.write(out);
    tweet3.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    tweet1.readFields(in);
    tweet2.readFields(in);
    tweet3.readFields(in);
  }
  
  @Override
  public TweetTweetTweet clone() {
    return new TweetTweetTweet(tweet1, tweet2, tweet3);
  }

  @Override
  public Rectangle getMBR() {
    Rectangle mbr = new Rectangle(tweet1.x, tweet1.y, tweet1.x, tweet1.y);
    mbr.expand(tweet2.x, tweet2.y);
    mbr.expand(tweet3.x, tweet3.y);
    return mbr;
  }

  @Override
  public boolean isIntersected(Shape s) {
    return this.getMBR().isIntersected(s);
  }

  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    int x1 = (int) Math.round(this.tweet1.x * xscale);
    int y1 = (int) Math.round(this.tweet1.y * yscale);
    int x2 = (int) Math.round(this.tweet2.x * xscale);
    int y2 = (int) Math.round(this.tweet2.y * yscale);
    int x3 = (int) Math.round(this.tweet3.x * xscale);
    int y3 = (int) Math.round(this.tweet3.y * yscale);
    g.drawLine(x1, y1, x2, y2);
    g.drawLine(x2, y2, x3, y3);
    g.drawLine(x3, y3, x1, y1);
  }

  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth, int imageHeight, double scale) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public double distanceTo(double x, double y) {
    return Math.min(tweet1.distanceTo(x, y), tweet2.distanceTo(x, y));
  }
}
