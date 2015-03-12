/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Ahmed Eldawy
 *
 */
public class DDVGroup extends DataDescriptor {

  /**A list of all referenced data descriptors*/
  protected DDID[] referencedDDs;
  
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
  

  DDVGroup(HDFFile hdfFile, int tagID, int refNo, int offset, int length,
      boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    int numElements = input.readUnsignedShort();
    int[] tags = new int[numElements];
    for (int i = 0; i < numElements; i++)
      tags[i] = input.readUnsignedShort();
    int[] refs = new int[numElements];
    for (int i = 0; i < numElements; i++)
      refs[i] = input.readUnsignedShort();

    this.referencedDDs = new DDID[numElements];
    for (int i = 0; i < numElements; i++)
      this.referencedDDs[i] = new DDID(tags[i], refs[i]);
    
    // Read the name
    int nameLength = input.readUnsignedShort();
    byte[] tempBytes = new byte[nameLength];
    input.readFully(tempBytes, 0, nameLength);
    this.name = new String(tempBytes, 0, nameLength);
    
    // Read the class
    int classLength = input.readUnsignedShort();
    if (classLength > tempBytes.length)
      tempBytes = new byte[classLength];
    input.readFully(tempBytes, 0, classLength);
    this.klass = new String(tempBytes, 0, classLength);
    
    this.extag = input.readUnsignedShort();
    this.exref = input.readUnsignedShort();
    this.version = input.readUnsignedShort();
  }
  
  public String getName() throws IOException {
    lazyLoad();
    return name;
  }

  public DataDescriptor[] getContents() throws IOException {
    lazyLoad();
    DataDescriptor[] contents = new DataDescriptor[referencedDDs.length];
    for (int i = 0; i < contents.length; i++)
      contents[i] = hdfFile.retrieveElementByID(referencedDDs[i]);
    return contents;
  }

  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("VGroup name: '%s', contents %s", name, Arrays.toString(referencedDDs));
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }
}
