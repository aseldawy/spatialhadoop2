/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    if (input.size() != 3)
      throw new IOException("HasTag takes three parameters");
    
    Map<String, String> tags = (Map<String, String>) input.get(0);
    Set<String> keys = new HashSet<String>(Arrays.asList(((String)input.get(1)).split(",")));
    Set<String> values = new HashSet<String>(Arrays.asList(((String)input.get(2)).split(",")));
    
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      if (keys.contains(entry.getKey()) && values.contains(entry.getValue())) {
        return true;
      }
    }
    return false;
  }
}
