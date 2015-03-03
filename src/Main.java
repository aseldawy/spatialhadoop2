import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot2;


/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which 
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/

/**
 * @author Eldawy
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
      OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
      
      Path[] inPaths = params.getInputPaths();
      Path outPath = params.getOutputPath();
      HDFPlot2.plotWaterMask(inPaths, outPath, params);
   }
}