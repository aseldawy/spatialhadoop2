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
import java.io.InputStream;

/**
 * An abstract class for any data descriptor
 * @author Ahmed Eldawy
 *
 */
public abstract class DataDescriptor {
  
  /**Whether the information has been loaded from file or not*/
  protected boolean loaded;
  
  /**Whether this block is extended or not*/
  protected final boolean extended;
  
  /**ID (type) of this tag*/
  public final int tagID;
  
  /**Reference number. Unique ID within this tag*/
  public final int refNo;
  
  /**Offset of this data descriptor in the HDF file*/
  public final int offset;
  
  /**Number of bytes in this data descriptor*/
  private final int length;
  
  /**
   * Only set if this is an extended block and it tells the size of the extended
   * data
   */
  private int extendedLength;
  
  /**The HDFFile that contains this group*/
  protected HDFFile hdfFile;

  DataDescriptor(HDFFile hdfFile, int tagID, int refNo,
      int offset, int length, boolean extended) {
    this.hdfFile = hdfFile;
    this.tagID = tagID;
    this.refNo = refNo;
    this.offset = offset;
    this.length = length;
    this.extended = extended;
    this.loaded = false;
  }
  
  protected void lazyLoad() throws IOException {
    if (!loaded) {
      hdfFile.inStream.seek(offset);
      if (!extended) {
        // Read from the input file directly
        readFields(hdfFile.inStream);
      } else {
        // Extended block. Need to retrieve extended data first
        int extensionType = hdfFile.inStream.readUnsignedShort();
        switch (extensionType) {
        case HDFConstants.SPECIAL_COMP:
          // Compressed data
          readCompressedData(); break;
        case HDFConstants.SPECIAL_CHUNKED:
          // Chunked data
          readChunkedData(); break;
        case HDFConstants.SPECIAL_LINKED:
          // Linked data
          readLinkedData(); break;
        default:
          // Not supported
          throw new RuntimeException("Unsupported extension type "+extensionType);
        }
      }
      loaded = true;
    }
  }

  /**
   * Read extended block that is available as compressed data
   * @throws IOException
   */
  private void readCompressedData() throws IOException {
    int compressionVersion = hdfFile.inStream.readUnsignedShort();
    extendedLength = hdfFile.inStream.readInt();
    int linkedRefNo = hdfFile.inStream.readUnsignedShort();
    int modelType = hdfFile.inStream.readUnsignedShort();
    int compressionType = hdfFile.inStream.readUnsignedShort();
    if (compressionType == HDFConstants.COMP_CODE_DEFLATE) {
      int deflateLevel = hdfFile.inStream.readUnsignedShort();
      // Retrieve the associated compressed block
      DDID linkedBlockID = new DDID(HDFConstants.DFTAG_COMPRESSED, linkedRefNo);
      DDCompressedBlock dataBlock =
          (DDCompressedBlock) hdfFile.retrieveElementByID(linkedBlockID);
      InputStream decompressedData = dataBlock.decompressDeflate(deflateLevel);
      readFields(new DataInputStream(decompressedData));
    } else {
      throw new RuntimeException("Unsupported compression "+compressionType);
    }
  }
  
  /**
   * Read chunked data
   * @throws IOException
   */
  private void readChunkedData() throws IOException {
    int sp_tag_head_len = hdfFile.inStream.readInt();
    int version = hdfFile.inStream.readUnsignedByte();
    int flag = hdfFile.inStream.readInt();
    // Valid logical length of the entire element. The logical physical length
    // is this value multiplied by nt_size. The actual physical length used for
    // storage can be greater than the dataset size due to the presence of ghost
    // areas in chunks. Partial chunks are not distinguished from regular
    // chunks.
    int elem_total_length = hdfFile.inStream.readInt();
    // Logical size of data chunks
    int chunk_size = hdfFile.inStream.readInt();
    // Number type size. i.e., the size of the data type
    int nt_size = hdfFile.inStream.readInt();
    // ID of the chunk table
    int tag = hdfFile.inStream.readUnsignedShort();
    int ref = hdfFile.inStream.readUnsignedShort();
    DDID chunkTableID = new DDID(tag, ref);
    tag = hdfFile.inStream.readUnsignedShort();
    ref = hdfFile.inStream.readUnsignedShort();
    // For future use. Special table for 'ghost' chunks.
    DDID specialTableID = new DDID(tag, ref);
    // Number of dimensions of the chunked element
    int nDims = hdfFile.inStream.readUnsignedShort();
    int[] flags = new int[nDims];
    int[] dimensionLengths = new int[nDims];
    int[] chunkLengths = new int[nDims];
    for (int i = 0; i < nDims; i++) {
      flags[i] = hdfFile.inStream.readInt();
      dimensionLengths[i] = hdfFile.inStream.readInt();
      chunkLengths[i] = hdfFile.inStream.readInt();
    }
    
    // Retrieve the chunk table to read chunked data
    DDVDataHeader chunkTable = (DDVDataHeader) hdfFile.retrieveElementByID(chunkTableID);
    int numChunks = chunkTable.getEntryCount();
    if (numChunks == 1) {
      // For now, we can handle only data consisting of one chunk
      Object[] chunkInformation = (Object[]) chunkTable.getEntryAt(0);
      DDID chunkedID = new DDID((Integer)chunkInformation[1], (Integer)chunkInformation[2]);
      DDChunkData chunkObject = (DDChunkData) hdfFile.retrieveElementByID(chunkedID);
      byte[] dataInChunk = chunkObject.getData();
      this.extendedLength = dataInChunk.length;
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dataInChunk));
      this.readFields(dis);
      dis.close();
    } else {
      // TODO use a more memory-friendly technique that avoids reading all
      // chunks in memory before parsing
      byte[] allData = new byte[elem_total_length*nt_size];
      this.extendedLength = 0;
      for (int i_chunk = 0; i_chunk < numChunks; i_chunk++) {
        // Read data in this chunk
        Object[] chunkInformation = (Object[]) chunkTable.getEntryAt(i_chunk);
        DDID chunkedID = new DDID((Integer)chunkInformation[1], (Integer)chunkInformation[2]);
        DDChunkData chunkObject = (DDChunkData) hdfFile.retrieveElementByID(chunkedID);
        if (chunkObject == null) {
          // TODO fill in the array with fillValue
          extendedLength += chunk_size * nt_size; // Skip this part in the array
        } else {
          byte[] dataInChunk = chunkObject.getData();
          // Append to the allData
          System.arraycopy(dataInChunk, 0, allData, extendedLength, dataInChunk.length);
          extendedLength += dataInChunk.length;
        }
      }
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(allData));
      this.readFields(dis);
      dis.close();
    }
  }
  
  private void readLinkedData() throws IOException {
    // Length of the entire element
    int length = hdfFile.inStream.readInt();
    // Length of successive data blocks
    int blk_len = hdfFile.inStream.readInt();
    // Number of blocks per block table
    int num_blk = hdfFile.inStream.readInt();
    // Reference number of first block table
    int link_ref = hdfFile.inStream.readUnsignedShort();
    // Length of the first data block (not correct)
    int first_len = length - blk_len * (num_blk - 1);
    
    DDLinkedBlock linkedBlockTable = (DDLinkedBlock) hdfFile.retrieveElementByID(new DDID(HDFConstants.DFTAG_LINKED, link_ref));
    int[] blockReferences = linkedBlockTable.getBlockReferences();
    
    int total_length = 0;
    for (int i = 0; i < blockReferences.length; i++) {
      DDID id = new DDID(HDFConstants.DFTAG_LINKED, blockReferences[i]);
      DataDescriptor dataBlock = hdfFile.retrieveElementByID(id);
      total_length += dataBlock.getLength();
    }
    
    this.extendedLength = 0;
    byte[] allData = new byte[total_length];
    for (int i = 0; i < blockReferences.length; i++) {
      DDID id = new DDID(HDFConstants.DFTAG_LINKED, blockReferences[i]);
      DDLinkedBlock dataBlock = (DDLinkedBlock) hdfFile.retrieveElementByID(id);
      byte[] data = dataBlock.readContainedData();
      System.arraycopy(data, 0, allData, extendedLength, data.length);
      extendedLength += data.length;
    }
    
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(allData));
    this.readFields(dis);
    dis.close();
  }
  
  /**
   * Returns the true length of the underlying data. If this block is extended
   * with a compressed block, the uncompressed size is returned. Otherwise, the
   * size of this block as mentioned in the HDF file will be returned.
   * @return
   * @throws IOException 
   */
  public int getLength() throws IOException {
    if (extended) {
      if (extendedLength == 0) {
        // Not set yet. Set and cache it
        hdfFile.inStream.seek(offset);
        // Extended block. Need to retrieve extended data first
        int extensionType = hdfFile.inStream.readUnsignedShort();
        switch (extensionType) {
        case HDFConstants.SPECIAL_COMP:
          // Compressed data
          /*int compressionVersion = */hdfFile.inStream.readUnsignedShort();
          extendedLength = hdfFile.inStream.readInt();
          break;
        case HDFConstants.SPECIAL_CHUNKED:
          // Chunked data
          /*int sp_tag_head_len = */hdfFile.inStream.readInt();
          /*int version = */hdfFile.inStream.readUnsignedByte();
          /*int flag = */hdfFile.inStream.readInt();
          int elem_total_length = hdfFile.inStream.readInt();
          /*int chunk_size = */hdfFile.inStream.readInt();
          // Number type size. i.e., the size of the data type
          int nt_size = hdfFile.inStream.readInt();
          extendedLength = elem_total_length * nt_size;
          break;
        case HDFConstants.SPECIAL_LINKED:
          // Linked data
          this.extendedLength = hdfFile.inStream.readInt();
          break;
        default:
          // Not supported
          throw new RuntimeException("Unsupported extension type "+extensionType);
        }
      }
      return extendedLength;
    }
    return length;
  }
  
  /**
   * Reads information of this data descriptor from the input.
   * @param input data input from where to read information
   * @throws IOException
   */
  protected abstract void readFields(DataInput input) throws IOException;

  @Override
  public String toString() {
    return String.format("<%d,%d> offset: %d, length: %d", tagID, refNo, offset, length);
  }
  
  
  /**
   * Read extended block that is available as compressed data
   * @throws IOException
   */
  private int readCompressedData(byte[] data, int offset, int length) throws IOException {
    /*int compressionVersion = */hdfFile.inStream.readUnsignedShort();
    /*int extendedLength = */hdfFile.inStream.readInt();
    int linkedRefNo = hdfFile.inStream.readUnsignedShort();
    /*int modelType = */hdfFile.inStream.readUnsignedShort();
    int compressionType = hdfFile.inStream.readUnsignedShort();
    if (compressionType == HDFConstants.COMP_CODE_DEFLATE) {
      int deflateLevel = hdfFile.inStream.readUnsignedShort();
      // Retrieve the associated compressed block
      DDID linkedBlockID = new DDID(HDFConstants.DFTAG_COMPRESSED, linkedRefNo);
      DDCompressedBlock dataBlock =
          (DDCompressedBlock) hdfFile.retrieveElementByID(linkedBlockID);
      InputStream decompressedData = dataBlock.decompressDeflate(deflateLevel);
      int totalBytesRead = 0;
      int numBytesRead;
      do {
        numBytesRead = decompressedData.read(data, offset, length);
        offset += numBytesRead;
        length -= numBytesRead;
        totalBytesRead += numBytesRead;
      } while (numBytesRead > 0 && length > 0);
      return totalBytesRead;
    } else {
      throw new RuntimeException("Unsupported compression "+compressionType);
    }
  }
  
  
  private int readChunkedData(byte[] buf, int bufOff, int bufLen) throws IOException {
    /*int sp_tag_head_len = */hdfFile.inStream.readInt();
    /*int version = */hdfFile.inStream.readUnsignedByte();
    /*int flag = */hdfFile.inStream.readInt();
    // Valid logical length of the entire element. The logical physical length
    // is this value multiplied by nt_size. The actual physical length used for
    // storage can be greater than the dataset size due to the presence of ghost
    // areas in chunks. Partial chunks are not distinguished from regular
    // chunks.
    int elem_total_length = hdfFile.inStream.readInt();
    // Logical size of data chunks
    int chunk_size = hdfFile.inStream.readInt();
    // Number type size. i.e., the size of the data type
    int nt_size = hdfFile.inStream.readInt();
    // ID of the chunk table
    int tag = hdfFile.inStream.readUnsignedShort();
    int ref = hdfFile.inStream.readUnsignedShort();
    DDID chunkTableID = new DDID(tag, ref);
    tag = hdfFile.inStream.readUnsignedShort();
    ref = hdfFile.inStream.readUnsignedShort();
    // For future use. Special table for 'ghost' chunks.
    DDID specialTableID = new DDID(tag, ref);
    // Number of dimensions of the chunked element
    int nDims = hdfFile.inStream.readUnsignedShort();
    int[] flags = new int[nDims];
    int[] dimensionLengths = new int[nDims];
    int[] chunkLengths = new int[nDims];
    for (int i = 0; i < nDims; i++) {
      flags[i] = hdfFile.inStream.readInt();
      dimensionLengths[i] = hdfFile.inStream.readInt();
      chunkLengths[i] = hdfFile.inStream.readInt();
    }
    
    // Retrieve the chunk table to read chunked data
    DDVDataHeader chunkTable = (DDVDataHeader) hdfFile.retrieveElementByID(chunkTableID);
    int numChunks = chunkTable.getEntryCount();
    int totalBytesRead = 0;
    int chunkSizeInBytes = chunk_size * nt_size;
    // TODO use a more memory-friendly technique that avoids reading all
    // chunks in memory before parsing
    for (int i_chunk = 0; i_chunk < numChunks && bufLen > 0; i_chunk++) {
      // Read data in this chunk
      Object[] chunkInformation = (Object[]) chunkTable.getEntryAt(i_chunk);
      DDID chunkedID = new DDID((Integer)chunkInformation[1], (Integer)chunkInformation[2]);
      DDChunkData chunkObject = (DDChunkData) hdfFile.retrieveElementByID(chunkedID);
      if (chunkObject == null) {
        // TODO fill in the array with fillValue
        // Skip the corresponding part in the array
      } else {
        chunkObject.readData(buf, bufOff, chunkSizeInBytes);
      }
      // Advance to next part in the array
      totalBytesRead += chunkSizeInBytes;
      bufOff += chunkSizeInBytes;
      bufLen -= chunkSizeInBytes;
    }
    return totalBytesRead;
  }
  
  private int readLinkedData(byte[] buf, int bufOff, int bufLen) throws IOException {
    // Length of the entire element
    int length = hdfFile.inStream.readInt();
    // Length of successive data blocks
    int blk_len = hdfFile.inStream.readInt();
    // Number of blocks per block table
    int num_blk = hdfFile.inStream.readInt();
    // Reference number of first block table
    int link_ref = hdfFile.inStream.readUnsignedShort();
    // Length of the first data block (not correct)
    int first_len = length - blk_len * (num_blk - 1);
    
    DDLinkedBlock linkedBlockTable = (DDLinkedBlock) hdfFile.retrieveElementByID(new DDID(HDFConstants.DFTAG_LINKED, link_ref));
    int[] blockReferences = linkedBlockTable.getBlockReferences();
    
    int total_length = 0;
    for (int i = 0; i < blockReferences.length; i++) {
      DDID id = new DDID(HDFConstants.DFTAG_LINKED, blockReferences[i]);
      DataDescriptor dataBlock = hdfFile.retrieveElementByID(id);
      total_length += dataBlock.getLength();
    }
    
    int totalBytesRead = 0;
    for (int i = 0; i < blockReferences.length && bufLen > 0; i++) {
      DDID id = new DDID(HDFConstants.DFTAG_LINKED, blockReferences[i]);
      DDLinkedBlock dataBlock = (DDLinkedBlock) hdfFile.retrieveElementByID(id);
      int bytesRead = dataBlock.readData(buf, bufOff, bufLen);
      totalBytesRead += bytesRead;
      bufOff += bytesRead;
      bufLen -= bytesRead;
    }
    return totalBytesRead;
  }
  
  protected int readData(byte[] buf, int bufOff, int bufLen) throws IOException {
    hdfFile.inStream.seek(offset);
    if (!extended) {
      // Read from the input file directly
      hdfFile.inStream.readFully(buf, bufOff, Math.max(this.getLength(), bufLen));
      return bufLen;
    } else {
      // Extended block. Need to retrieve extended data first
      int extensionType = hdfFile.inStream.readUnsignedShort();
      switch (extensionType) {
      case HDFConstants.SPECIAL_COMP:
        // Compressed data
        return readCompressedData(buf, bufOff, bufLen);
      case HDFConstants.SPECIAL_CHUNKED:
        // Chunked data
        return readChunkedData(buf, bufOff, bufLen);
      case HDFConstants.SPECIAL_LINKED:
        // Linked data
        return readLinkedData(buf, bufOff, bufLen);
      default:
        // Not supported
        throw new RuntimeException("Unsupported extension type "+extensionType);
      }
    }
  }
}
