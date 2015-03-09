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
 * Converts a Key-value hash map to JSON format.
 * @author Ahmed Eldawy
 *
 */
public class MapToJson extends EvalFunc<String> {

  public MapToJson() throws ParserConfigurationException {
  }

  @Override
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() != 1)
      throw new IOException("Invalid number of arguments "+input.size());

    if (!(input.get(0) instanceof Map))
      throw new IOException("Invalid argument type "+input.get(0).getClass());
    
    Map<String, String> tags = (Map<String, String>) input.get(0);
    StringBuffer result = new StringBuffer();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      result.append("{");
      result.append('"'+entry.getKey()+'"');
      result.append('=');
      result.append('"'+entry.getValue()+'"');
      result.append("}");
    }
    return result.toString();
  }
}
