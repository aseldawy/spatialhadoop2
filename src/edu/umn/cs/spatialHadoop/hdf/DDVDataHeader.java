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
 * Header of VData
 * @author Ahmed Eldawy
 *
 */
public class DDVDataHeader extends DataDescriptor {

  /** Field indicating interlace scheme used */
  protected int interlace;
  /** Number of entries */
  protected int nvert;
  /** Size of one Vdata entry */
  protected int ivsize;
  /** Field indicating the data type of the nth field of the Vdata*/
  protected int[] types;
  /** Size in bytes of the nth field of the Vdata*/
  protected int[] isizes;
  /** Offset of the nth field within the Vdata*/
  protected int[] offsets;
  /** Order of the nth field of the Vdata*/
  protected int[] order;
  /** Names of the fields */
  protected String[] fieldNames;
  /** Name */
  protected String name;
  /** Class */
  protected String klass;
  /** Extension tag */
  protected int extag;
  /** Extension reference number */
  protected int exref;
  /** Version number of DFTAG_VH information */
  protected int version;

  public DDVDataHeader() {
  }
  
  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    in.seek(offset);
    this.interlace = in.readUnsignedShort();
    this.nvert = in.readInt();
    this.ivsize = in.readUnsignedShort();
    int nfields = in.readUnsignedShort();
    this.types = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.types[i] = in.readUnsignedShort();
    this.isizes = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.isizes[i] = in.readUnsignedShort();
    this.offsets = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.offsets[i] = in.readUnsignedShort();
    this.order = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.order[i] = in.readUnsignedShort();
    int maxLength = 0;
    int[] fieldNameLength = new int[nfields];
    for (int i = 0; i < nfields; i++) {
      fieldNameLength[i] = in.readUnsignedShort();
      if (fieldNameLength[i] > maxLength)
        maxLength = fieldNameLength[i];
    }
    byte[] nameBytes = new byte[maxLength];
    fieldNames = new String[nfields];
    for (int i = 0; i < nfields; i++) {
      in.readFully(nameBytes, 0, fieldNameLength[i]);
      fieldNames[i] = new String(nameBytes, 0, fieldNameLength[i]);
    }
    int nameLength = in.readUnsignedShort();
    if (nameLength > nameBytes.length)
      nameBytes = new byte[nameLength];
    in.readFully(nameBytes, 0, nameLength);
    name = new String(nameBytes, 0, nameLength);
    
    int classLength = in.readUnsignedShort();
    if (classLength > nameBytes.length)
      nameBytes = new byte[classLength];
    in.readFully(nameBytes, 0, classLength);
    klass = new String(nameBytes, 0, classLength);

    this.extag = in.readUnsignedShort();
    this.exref = in.readUnsignedShort();
    this.version = in.readUnsignedShort();
  }
  
  @Override
  public String toString() {
    return String.format("VHeader with %d fields with type %d and name '%s', overall name '%s'", types.length, types[0], fieldNames[0], name);
  }

}
