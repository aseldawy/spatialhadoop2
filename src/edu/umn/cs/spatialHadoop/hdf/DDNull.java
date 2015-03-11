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
 * Data descriptor with no data. This tag is used for place holding and to fill
 * empty portions of the data description block. The length and offset fields
 * are always zero.
 * @author Ahmed Eldawy
 *
 */
public class DDNull extends DataDescriptor {

  public DDNull() {
  }

  @Override
  public void readFields(FSDataInputStream in) throws IOException {
    // Nothing to read
  }

}
