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

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;

/**
 * @author Eldawy
 *
 */
public class Tweet extends Point {
  protected long id;

  public Tweet() {
  }

  public Tweet(Tweet tweet) {
    this.id = tweet.id;
    this.x = tweet.x;
    this.y = tweet.y;
  }

  @Override
  public void fromText(Text text) {
    this.id = TextSerializerHelper.consumeLong(text, ',');
    this.y = TextSerializerHelper.consumeDouble(text, ',');
    this.x = TextSerializerHelper.consumeDouble(text, '\0');
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, ',');
    TextSerializerHelper.serializeDouble(y, text, ',');
    TextSerializerHelper.serializeDouble(x, text, '\0');
    return text;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    super.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    super.readFields(in);
  }
  
  @Override
  public Tweet clone() {
    return new Tweet(this);
  }

  @Override
  public String toString() {
    return Long.toString(id);
  }
}
