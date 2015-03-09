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
 * Implementing this interface allows objects to be converted easily
 * to and from a string.
 * @author aseldawy
 *
 */
public interface TextSerializable {
  /**
   * Store current object as string in the given text appending text already there.
   * @param text
   */
  public Text toText(Text text);
  
  /**
   * Retrieve information from the given text.
   * @param text
   */
  public void fromText(Text text);
}
