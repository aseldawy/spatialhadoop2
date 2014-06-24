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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * @author Eldawy
 *
 */
public class Tweet extends Point {
  public long id;

  public Tweet() {
  }

  public Tweet(double x, double y) {
    super(x, y);
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
}
