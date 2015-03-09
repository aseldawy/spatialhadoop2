/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
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
