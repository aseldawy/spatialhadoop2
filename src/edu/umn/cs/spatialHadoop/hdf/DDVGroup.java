/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * @author Eldawy
 *
 */
public class DDVGroup extends DataDescriptor {

  /** Tags of the members of the VGroup*/
  protected int[] tags;
  /** Reference number of the members of the VGroup*/
  protected int[] refs;
  
  /** Overall name of the group */
  protected String name;
  
  /** Name of the class */
  protected String klass;
  
  /** Extension tag */
  protected int extag;
  /** Extension reference number */
  protected int exref;
  /** Version number of DFTAG_VH information */
  protected int version;
  
  public DDVGroup() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    in.seek(offset);
    int numElements = in.readUnsignedShort();
    this.tags = new int[numElements];
    for (int i = 0; i < numElements; i++)
      this.tags[i] = in.readUnsignedShort();
    this.refs = new int[numElements];
    for (int i = 0; i < numElements; i++)
      this.refs[i] = in.readUnsignedShort();
    
    // Read the name
    int nameLength = in.readUnsignedShort();
    byte[] tempBytes = new byte[nameLength];
    in.readFully(tempBytes, 0, nameLength);
    this.name = new String(tempBytes, 0, nameLength);
    
    // Read the class
    int classLength = in.readUnsignedShort();
    if (classLength > tempBytes.length)
      tempBytes = new byte[classLength];
    in.readFully(tempBytes, 0, classLength);
    this.klass = new String(tempBytes, 0, classLength);
    
    this.extag = in.readUnsignedShort();
    this.exref = in.readUnsignedShort();
    this.version = in.readUnsignedShort();
  }

  @Override
  public String toString() {
    return String.format("VGroup name: '%s', class: '%s'", name, klass);
  }
}
