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
  protected int[] sizes;
  /** Offset of the nth field within the Vdata (offset in file)*/
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

  DDVDataHeader(HDFFile hdfFile, int tagID, int refNo, int offset,
      int length, boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    this.interlace = input.readUnsignedShort();
    this.nvert = input.readInt();
    this.ivsize = input.readUnsignedShort();
    int nfields = input.readUnsignedShort();
    this.types = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.types[i] = input.readUnsignedShort();
    this.sizes = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.sizes[i] = input.readUnsignedShort();
    this.offsets = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.offsets[i] = input.readUnsignedShort();
    this.order = new int[nfields];
    for (int i = 0; i < nfields; i++)
      this.order[i] = input.readUnsignedShort();
    fieldNames = new String[nfields];
    byte[] nameBytes = new byte[1024];
    for (int i = 0; i < nfields; i++) {
      int fieldNameLength = input.readUnsignedShort();
      if (fieldNameLength > nameBytes.length)
        nameBytes = new byte[fieldNameLength];
      input.readFully(nameBytes, 0, fieldNameLength);
      fieldNames[i] = new String(nameBytes, 0, fieldNameLength);
    }
    int nameLength = input.readUnsignedShort();
    if (nameLength > nameBytes.length)
      nameBytes = new byte[nameLength];
    input.readFully(nameBytes, 0, nameLength);
    name = new String(nameBytes, 0, nameLength);
    
    int classLength = input.readUnsignedShort();
    if (classLength > nameBytes.length)
      nameBytes = new byte[classLength];
    input.readFully(nameBytes, 0, classLength);
    klass = new String(nameBytes, 0, classLength);

    this.extag = input.readUnsignedShort();
    this.exref = input.readUnsignedShort();
    this.version = input.readUnsignedShort();
  }
  
  public int getEntryCount() throws IOException {
    lazyLoad();
    return nvert;
  }
  
  public Object getEntryAt(int i) throws IOException {
    lazyLoad();
    if (i >= nvert)
      throw new ArrayIndexOutOfBoundsException(i);
    // Read the corresponding data in VSet
    DDVSet vset = (DDVSet) hdfFile.retrieveElementByID(
        new DDID(HDFConstants.DFTAG_VS, this.refNo));
    byte[] data = vset.getData();
    int offset = i * ivsize;
    Object[] fields = new Object[types.length];
    for (int iField = 0; iField < fields.length; iField++) {
      switch (types[iField]) {
      case HDFConstants.DFNT_CHAR:
        fields[iField] = new String(data, offset, sizes[iField]);
        break;
      case HDFConstants.DFNT_UINT8: fields[iField] = data[offset] < 0 ? (int) data[offset] + 256 : (int)data[offset]; break;
      case HDFConstants.DFNT_INT16: fields[iField] = HDFConstants.readSignedShort(data, offset); break;
      case HDFConstants.DFNT_UINT16: fields[iField] = HDFConstants.readUnsignedShort(data, offset); break;
      case HDFConstants.DFNT_INT32: fields[iField] = HDFConstants.readInt32(data, offset); break;
      default: throw new RuntimeException("Unsupported type "+types[iField]);
      }
      offset += sizes[iField];
    }
    if (fields.length == 1)
      return fields[0];
    return fields;
  }
  
  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("VHeader with %d fields with type %d, size %d, and name '%s', overall name '%s'", types.length, types[0], sizes[0], fieldNames[0], name);
    } catch (IOException e) {
      return "Error reading "+super.toString();
    }
  }

  public String getName() throws IOException {
    lazyLoad();
    return name;
  }


}
