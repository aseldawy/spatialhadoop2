package edu.umn.cs.spatialHadoop.visualization;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

public class TileMBR {

	private static final String InputMBR = "mbr";

	public static void main(String[] args) throws IOException, InterruptedException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
				Path[] inFiles = params.getInputPaths();
	    //Path outFile = params.getOutputPath();
		OperationsParams mbrParams = new OperationsParams(params);
		
		mbrParams.setBoolean("background", false);
		final Rectangle inputMBR = params.get("mbr") != null ? params.getShape("mbr").getMBR()
		        : FileMBR.fileMBR(inFiles, mbrParams);
		    OperationsParams.setShape(params, InputMBR, inputMBR);
		    
		int tileWidth = params.getInt("tilewidth", 256);
		int tileHeight = params.getInt("tileheight", 256);    
	    String[] strLevels = params.get("levels", "2").split("\\.\\.");
	    int minLevel, maxLevel;
	    if (strLevels.length == 1) {
	      minLevel = 0;
	      maxLevel = Integer.parseInt(strLevels[0]) - 1;
	    } else {
	      minLevel = Integer.parseInt(strLevels[0]);
	      maxLevel = Integer.parseInt(strLevels[1]);
	    }
	    
	   // Rectangle inputMBR = (Rectangle) params.getShape("mbr");
//	    if (inputMBR == null)
//	      inputMBR = FileMBR.fileMBR(inFiles, params);
//	    OperationsParams.setShape(params, InputMBR, inputMBR);
	    //Iterable<? extends Shape> shapes;
	    
	    Vector<InputSplit> splits = new Vector<InputSplit>();
	    final SpatialInputFormat3<Rectangle, Shape> inputFormat = new SpatialInputFormat3<Rectangle, Shape>();
	    
	    
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
	            splits.add(new FileSplit(inFile, 0, inFs.getFileStatus(inFile).getLen(), new String[0]));
	          }
	        } else {
	          Job job = Job.getInstance(params);
	          SpatialInputFormat3.addInputPath(job, inFile);
	          splits.addAll(inputFormat.getSplits(job));
	        }
	      }

	  
	    SubPyramid subPyramid = new SubPyramid(inputMBR, minLevel, maxLevel,
	            0, 0, 1 << maxLevel, 1 << maxLevel);
	    for (InputSplit split : splits) {
	        FileSplit fsplit = (FileSplit) split;
	        RecordReader<Rectangle, Iterable<Shape>> reader = inputFormat.createRecordReader(fsplit, null);
	        if (reader instanceof SpatialRecordReader3) {
	          ((SpatialRecordReader3) reader).initialize(fsplit, params);
	        } else if (reader instanceof RTreeRecordReader3) {
	          ((RTreeRecordReader3) reader).initialize(fsplit, params);
	        } else if (reader instanceof HDFRecordReader) {
	          ((HDFRecordReader) reader).initialize(fsplit, params);
	        } else {
	          throw new RuntimeException("Unknown record reader");
	        }
	        while (reader.nextKeyValue()) {
	            Rectangle partition = reader.getCurrentKey();
	            if (!partition.isValid())
	              partition.set(inputMBR);
	            Iterable<Shape> shapes = reader.getCurrentValue();
	            java.awt.Rectangle overlaps = new java.awt.Rectangle();
	            for (Shape shape : shapes) {
	            	//System.out.println(shape);
	                if (shape == null)
	                  continue;
	                Rectangle mbr = shape.getMBR();
	                if (mbr == null)
	                  continue;
	                 subPyramid.getOverlappingTiles(mbr, overlaps);
	                 for (int z = subPyramid.maximumLevel; z >= subPyramid.minimumLevel; z--) {
	                     for (int x = overlaps.x; x < overlaps.x + overlaps.width; x++) {
	                       for (int y = overlaps.y; y < overlaps.y + overlaps.height; y++) {
	                         long tileID = TileIndex.encode(z, x, y);
	                         Rectangle tileMBR = TileIndex.getMBR(inputMBR, z, x, y);
	                         System.out.println("tile="+z+"-"+x+"-"+y+"\t"+"tileMBR="+tileMBR);
	                       }
	                     }
	                     
	                     int updatedX1 = overlaps.x / 2;
	                     int updatedY1 = overlaps.y / 2;
	                     int updatedX2 = (overlaps.x + overlaps.width - 1) / 2;
	                     int updatedY2 = (overlaps.y + overlaps.height - 1) / 2;
	                     overlaps.x = updatedX1;
	                     overlaps.y = updatedY1;
	                     overlaps.width = updatedX2 - updatedX1 + 1;
	                     overlaps.height = updatedY2 - updatedY1 + 1;
	                 }
	            }
	        }

	    }

	}

}
