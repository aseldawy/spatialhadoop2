/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.hdf;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * A data descriptor for linked block table
 * @author Ahmed Eldawy
 *
 */
public class DDLinkedBlock extends DataDescriptor {
  
  /**Raw data*/
  public byte[] data;
  
  /**Reference number for next table. Zero if no more linked block tables*/
  protected int nextRef;
  
  /**A list of all block references stored in this table*/
  protected int[] blockReferences;
  
  /**
   * @param hdfFile
   * @param tagID
   * @param refNo
   * @param offset
   * @param length
   * @param extended
   */
  public DDLinkedBlock(HDFFile hdfFile, int tagID, int refNo, int offset,
      int length, boolean extended) {
    super(hdfFile, tagID, refNo, offset, length, extended);
  }

  @Override
  protected void readFields(DataInput input) throws IOException {
    // Note. We cannot parse the data at this point because a linked block
    // might refer to either a linked block table or data block according
    // to the context in which it appears
    data = new byte[getLength()];
    input.readFully(data);
  }
  
  /**
   * Reads all the data contained in this object
   * @return
   * @throws IOException
   */
  protected byte[] readContainedData() throws IOException {
    lazyLoad();
    return data;
  }

  /**
   * Retrieve the references of all contained block if this linked block is
   * a linked block table.
   * @return 
   * @throws IOException 
   */
  public int[] getBlockReferences() throws IOException {
    lazyLoad();
    
    if (blockReferences == null) {
      // Lazy parse
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
      nextRef = in.readUnsignedShort();
      blockReferences = new int[(data.length - 2) / 2];
      for (int i_blk = 0; i_blk < blockReferences.length; i_blk++)
        blockReferences[i_blk] = in.readUnsignedShort();
      in.close();
    }
    
    return blockReferences;
  }

}
