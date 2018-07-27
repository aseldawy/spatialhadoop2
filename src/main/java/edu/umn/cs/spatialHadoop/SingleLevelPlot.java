/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;
import edu.umn.cs.spatialHadoop.visualization.Canvas;
import edu.umn.cs.spatialHadoop.visualization.Plotter;


/**
 * @author Ahmed Eldawy
 *
 */
public class SingleLevelPlot {
  private static final Log LOG = LogFactory.getLog(SingleLevelPlot.class);
  
  /**Configuration line for input file MBR*/
  private static final String InputMBR = "mbr";

  /**
   * Visualizes a dataset using the existing partitioning of a file.
   * The mapper creates a partial canvas for each partition while the reducer
   * merges the partial canvases together into the final canvas.
   * The final canvas is then written to the output.
   * 
   * @author Ahmed Eldawy
 * @return 
   *
   */


  public static void plotLocal(Path[] inFiles, DataOutputStream output,
      final Class<? extends Plotter> plotterClass,
      final OperationsParams params) throws IOException, InterruptedException {
    OperationsParams mbrParams = new OperationsParams(params);
    mbrParams.setBoolean("background", false);
    final Rectangle inputMBR = params.get(InputMBR) != null ?
        params.getShape("mbr").getMBR() : FileMBR.fileMBR(inFiles, mbrParams);
    if (params.get(InputMBR) == null)
      OperationsParams.setShape(params, InputMBR, inputMBR);

    // Retrieve desired output image size and keep aspect ratio if needed
    int width = params.getInt("width", 1000);
    int height = params.getInt("height", 1000);
    if (params.getBoolean("keepratio", true)) {
      // Adjust width and height to maintain aspect ratio and store the adjusted
      // values back in params in case the caller needs to retrieve them
      if (inputMBR.getWidth() / inputMBR.getHeight() > (double) width / height)
        params.setInt("height", height = (int) (inputMBR.getHeight() * width / inputMBR.getWidth()));
      else
        params.setInt("width", width = (int) (inputMBR.getWidth() * height / inputMBR.getHeight()));
    }
    // Store width and height in final variables to make them accessible in parallel
    final int fwidth = width, fheight = height;

    // Start reading input file
    List<InputSplit> splits = new ArrayList<InputSplit>();
    final SpatialInputFormat3<Rectangle, Shape> inputFormat =
        new SpatialInputFormat3<Rectangle, Shape>();
    for (Path inFile : inFiles) {
      FileSystem inFs = inFile.getFileSystem(params);
      if (!OperationsParams.isWildcard(inFile) && inFs.exists(inFile) && !inFs.isDirectory(inFile)) {
        if (SpatialSite.NonHiddenFileFilter.accept(inFile)) {
          // Use the normal input format splitter to add this non-hidden file
          Job job = Job.getInstance(params);
          SpatialInputFormat3.addInputPath(job, inFile);
          splits.addAll(inputFormat.getSplits(job));
        } else {
          // A hidden file, add it immediately as one split
          // This is useful if the input is a hidden file which is automatically
          // skipped by FileInputFormat. We need to plot a hidden file for the case
          // of plotting partition boundaries of a spatial index
          splits.add(new FileSplit(inFile, 0,
              inFs.getFileStatus(inFile).getLen(), new String[0]));
        }
      } else {
        // Use the normal input format splitter to add this non-hidden file
        Job job = Job.getInstance(params);
        SpatialInputFormat3.addInputPath(job, inFile);
        splits.addAll(inputFormat.getSplits(job));
      }
    }
    
    // Copy splits to a final array to be used in parallel
    final FileSplit[] fsplits = splits.toArray(new FileSplit[splits.size()]);
    int parallelism = params.getInt("parallel",
        Runtime.getRuntime().availableProcessors());
    List<Canvas> partialCanvases = Parallel.forEach(fsplits.length, new RunnableRange<Canvas>() {
      public Canvas run(int i1, int i2) {
        Plotter plotter;
        try {
          plotter = plotterClass.newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException("Error creating rastierizer", e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Error creating rastierizer", e);
        }
        plotter.configure(params);
        // Create the partial layer that will contain the plot of the assigned partitions
        Canvas partialCanvas = plotter.createCanvas(fwidth, fheight, inputMBR);
        
        for (int i = i1; i < i2; i++) {
          try {
            RecordReader<Rectangle, Iterable<Shape>> reader =
                inputFormat.createRecordReader(fsplits[i], null);
            if (reader instanceof SpatialRecordReader3) {
              ((SpatialRecordReader3)reader).initialize(fsplits[i], params);
            } else if (reader instanceof RTreeRecordReader3) {
              ((RTreeRecordReader3)reader).initialize(fsplits[i], params);
            } else if (reader instanceof HDFRecordReader) {
              ((HDFRecordReader)reader).initialize(fsplits[i], params);
            } else {
              throw new RuntimeException("Unknown record reader");
            }

            while (reader.nextKeyValue()) {
              Rectangle partition = reader.getCurrentKey();
              if (!partition.isValid())
                partition.set(inputMBR);

              Iterable<Shape> shapes = reader.getCurrentValue();
              // Run the plot step
              plotter.plot(partialCanvas,
                  plotter.isSmooth() ? plotter.smooth(shapes) : shapes);
            }
            reader.close();
          } catch (IOException e) {
            throw new RuntimeException("Error reading the file ", e);
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupt error ", e);
          }
        }
        return partialCanvas;
      }
    }, parallelism);
    boolean merge = params.getBoolean("merge", true);
    Plotter plotter;
    try {
      plotter = plotterClass.newInstance();
      plotter.configure(params);
    } catch (InstantiationException e) {
      throw new RuntimeException("Error creating plotter", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error creating plotter", e);
    }
    
    // Whether we should vertically flip the final image or not
    boolean vflip = params.getBoolean("vflip", true);
    if (merge) {
      LOG.info("Merging "+partialCanvases.size()+" partial canvases");
      // Create the final canvas that will contain the final image
      Canvas finalCanvas = plotter.createCanvas(fwidth, fheight, inputMBR);
      for (Canvas partialCanvas : partialCanvases)
        plotter.merge(finalCanvas, partialCanvas);
      
      
      // Finally, write the resulting image to the given output path
      LOG.info("Writing final image");
      
      plotter.writeImage(finalCanvas, output, vflip);
    } else {
      // No merge
      LOG.info("Writing partial images");
      for (int i = 0; i < partialCanvases.size(); i++) {
        
        plotter.writeImage(partialCanvases.get(i), output, vflip);
      }
      															//change
    }
  }
  
  /**
   * Plots the given file using the provided plotter
   * @param inFiles
   * @param outFile
   * @param plotterClass
   * @param params
   * @return
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public static void plot(Path[] inFiles, DataOutputStream output,
	      final Class<? extends Plotter> plotterClass,
	      final OperationsParams params)
	          throws IOException, InterruptedException, ClassNotFoundException {
	   
	      plotLocal(inFiles, output, plotterClass, params);
	    
	  }
}
