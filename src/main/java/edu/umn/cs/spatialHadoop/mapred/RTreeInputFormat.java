/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.RTree;

/**
 * Reads a file stored as a list of RTrees
 * @author eldawy
 *
 */
public class RTreeInputFormat<S extends Shape> extends SpatialInputFormat<CellInfo, RTree<S>> {

  @Override
	public RecordReader<CellInfo, RTree<S>> getRecordReader(InputSplit split,
	    JobConf job, Reporter reporter) throws IOException {
    if (reporter != null)
      reporter.setStatus(split.toString());
    this.rrClass = RTreeRecordReader.class;
    return super.getRecordReader(split, job, reporter);
	}

}