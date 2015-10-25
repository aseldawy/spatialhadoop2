/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Checks if the given map contains any combination of the given keys and values
 * @author Ahmed Eldawy
 *
 */
public class HasTag extends EvalFunc<Boolean> {

  public HasTag() throws ParserConfigurationException {
  }

  @Override
  public Boolean exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;

    if (input.size() < 2)
      throw new IOException("HasTag takes at least two parameters");
    
    Map<String, String> tags = (Map<String, String>) input.get(0);
    String keys = (String)input.get(1);
    String values = input.size() > 2 ? (String)input.get(2) : null;

    return hasTag(tags, keys, values);
  }

  public static boolean hasTag(Map<String, String> tags, String keys, String values) {
    if (values == null) {
      for (Map.Entry<String, String> entry : tags.entrySet())
        if (keys.contains(entry.getKey()))
          return true;
    } else {
      for (Map.Entry<String, String> entry : tags.entrySet())
        if (keys.contains(entry.getKey()) && values.contains(entry.getValue()))
          return true;
    }
    return false;
  }
}
