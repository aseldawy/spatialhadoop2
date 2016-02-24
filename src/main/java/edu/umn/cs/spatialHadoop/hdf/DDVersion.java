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
 * Data descriptor for the library version number. It contains the complete
 * version number and a descriptive string for the latest version of the HDF
 * library used to write the file.
 * @author Ahmed Eldawy
 *
 */
public class DDVersion extends DataDescriptor {

  /** Major version number */
  public int majorVersion;
  
  /** Minor version number */
  public int minorVersion;
  
  /** Release number */
  public int release;
  
  /**
   * A descriptive string for the latest version of the HDF library used to
   * write to the file
   */
  public String name;

  DDVersion(HDFFile hdfFile, int tagID, int refNo, int offset, int length,
      boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    this.majorVersion = input.readInt();
    this.minorVersion = input.readInt();
    this.release = input.readInt();
    byte[] nameBytes = new byte[getLength() - 12];
    input.readFully(nameBytes);
    name = new String(nameBytes);
  }
  
  @Override
  public String toString() {
    try {
      lazyLoad();
      return String.format("Version %d.%d.%d '%s'", majorVersion, minorVersion, release, name);
    } catch (IOException e) {
      return "Error loading "+super.toString();
    }
  }

}
