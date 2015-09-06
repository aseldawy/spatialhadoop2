/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;

/**
 * @author Eldawy
 *
 */
public class Tweet extends Point {
  public String details;

  public Tweet() {
  }

  public Tweet(Tweet tweet) {
    this.details = tweet.details;
    this.x = tweet.x;
    this.y = tweet.y;
  }

  @Override
  public void fromText(Text text) {
    this.details = text.toString();
    int last_comma = this.details.lastIndexOf(',');
    int next_to_last_comma = this.details.lastIndexOf(',', last_comma - 1);
    this.x = Double.parseDouble(this.details.substring(last_comma + 1, this.details.length()));
    this.y = Double.parseDouble(this.details.substring(next_to_last_comma + 1, last_comma));
  }
  
  @Override
  public Text toText(Text text) {
    byte[] bytes = this.details.getBytes();
    text.append(bytes, 0, bytes.length);
    return text;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(this.details);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.details = in.readUTF();
  }
  
  @Override
  public Tweet clone() {
    return new Tweet(this);
  }
  
  @Override
  public String toString() {
    return details;
  }
}
