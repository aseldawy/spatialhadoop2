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
package edu.umn.cs.spatialHadoop.io;

import org.apache.hadoop.io.Text;

/**
 * A modified version of Text which is optimized for appends.
 * @author eldawy
 *
 */
public class Text2 extends Text implements TextSerializable {

  public Text2() {
  }

  public Text2(String string) {
    super(string);
  }

  public Text2(Text utf8) {
    super(utf8);
  }

  public Text2(byte[] utf8) {
    super(utf8);
  }

  @Override
  public Text toText(Text text) {
    text.append(getBytes(), 0, getLength());
    return text;
  }

  @Override
  public void fromText(Text text) {
    this.set(text);
  }
}
