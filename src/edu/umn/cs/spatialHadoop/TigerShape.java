/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.OGCJTSShape;

/**
 * A shape from tiger file.
 * @author aseldawy
 *
 */
public class TigerShape extends OGCJTSShape {
  private String originalText;
  
  @Override
  public void fromText(Text text) {
    originalText = text.toString();
    byte[] bytes = text.getBytes();
    int i = 0;
    while (i < text.getLength() && bytes[i] != ',') {
      i++;
    }
    text.set(bytes, 0, i);
    super.fromText(text);
  }
  
  @Override
  public Text toText(Text text) {
    text.set(originalText);
    return text;
  }
}
