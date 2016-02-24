/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

/**
 * The full ID of a data descriptor. This class is immutable and its contents
 * are not allowed to change.
 * @author Ahmed Eldawy
 *
 */
public class DDID {
  public final int tagID;
  public final int refNo;
  
  public DDID(int tag, int refNo) {
    super();
    this.tagID = tag;
    this.refNo = refNo;
  }
  
  @Override
  public int hashCode() {
    return tagID + refNo;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DDID))
      return false;
    DDID other = (DDID) obj;
    return this.tagID == other.tagID && this.refNo == other.refNo;
  }
  
  @Override
  public String toString() {
    return String.format("DD<%d,%d>", tagID, refNo);
  }
}