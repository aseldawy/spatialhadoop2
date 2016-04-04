/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.nasa.HTTPFileSystem;

/**
 * A bunch of helper functions used with files
 * 
 * @author Ahmed Eldawy
 */
public final class FileUtil {

	public static String copyFile(Configuration job, FileStatus fileStatus)
			throws IOException {
		return FileUtil.copyFileSplit(job, new FileSplit(fileStatus.getPath(),
				0, fileStatus.getLen(), new String[0]));
	}

	/**
	 * Copies a part of a file from a remote file system (e.g., HDFS) to a local
	 * file. Returns a path to a local temporary file.
	 * 
	 * @param conf
	 * @param split
	 * @return
	 * @throws IOException
	 */
	public static String copyFileSplit(Configuration conf, FileSplit split)
			throws IOException {
		FileSystem fs = split.getPath().getFileSystem(conf);
		
		// Special case of a local file. Skip copying the file
		if (fs instanceof LocalFileSystem && split.getStart() == 0)
			return split.getPath().toUri().getPath();
		
		File destFile = File.createTempFile(split.getPath().getName(), "tmp");
		// Special handling for HTTP files for more efficiency
		/*if (fs instanceof HTTPFileSystem && split.getStart() == 0) {
		  URL website = split.getPath().toUri().toURL();
		  ReadableByteChannel rbc = Channels.newChannel(website.openStream());
		  FileOutputStream fos = new FileOutputStream(destFile);
		  fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		  fos.close();
		  return destFile.getAbsolutePath();
		}*/

		// Length of input file. We do not depend on split.length because it is
		// not
		// set by input format for performance reason. Setting it in the input
		// format would cost a lot of time because it runs on the client machine
		// while the record reader runs on slave nodes in parallel
		long length = fs.getFileStatus(split.getPath()).getLen();

		FSDataInputStream in = fs.open(split.getPath());
		in.seek(split.getStart());
		ReadableByteChannel rbc = Channels.newChannel(in);

		// Prepare output file for write
		FileOutputStream out = new FileOutputStream(destFile);
		
		out.getChannel().transferFrom(rbc, 0, length);

		in.close();
		out.close();
		return destFile.getAbsolutePath();
	}

	/**
	 * Copies a file to the local file system given its path.
	 * 
	 * @param conf
	 * @param inFile
	 * @return
	 * @throws IOException
	 */
	public static String copyFile(Configuration conf, Path inFile)
			throws IOException {
		FileSystem fs = inFile.getFileSystem(conf);
		return copyFile(conf, fs.getFileStatus(inFile));
	}

	/**
	 * Writes paths to a HDFS file where each path is a line.
	 * 
	 * @author ibrahimsabek
	 * @param paths
	 */
	
	public static Path writePathsToHDFSFile(OperationsParams params, Path[] paths){
		String tmpFileName = "pathsDictionary.txt";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = params.getPaths()[0].getFileSystem(conf);
			Path hdfsFilePath = new Path(params.getPaths()[0].toString() + "/"
					+ tmpFileName);	
			FSDataOutputStream out = fs.create(hdfsFilePath);
			
			for (int i = 0; i < paths.length; i++) {
				StringBuilder pathStringBuilder = new StringBuilder();
				pathStringBuilder.append(paths[i].toString());
				pathStringBuilder.append("\n");
				
				byte[] bytArr = pathStringBuilder.toString().getBytes();
				out.write(bytArr);
			}
					
			out.close();

			return hdfsFilePath;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	/**
	 * Writes paths to a file where each path is a line.
	 * 
	 * @author ibrahimsabek
	 * @param paths
	 */
	public static Path writePathsToFile(OperationsParams params, Path[] paths) {
		String tmpFileName = "pathsDictionary.txt";
		File tempFile;
		BufferedWriter buffWriter = null;
		try {
			// store the dictionary of paths in a local file
			tempFile = new File(tmpFileName);
			Path localFilePath = new Path(tempFile.getAbsolutePath());
			FileOutputStream outStream = new FileOutputStream(tempFile);
			buffWriter = new BufferedWriter(
					new OutputStreamWriter(outStream));

			for (int i = 0; i < paths.length; i++) {
				buffWriter.write(paths[i].toString());
				buffWriter.newLine();
			}
			// copy the local dictionary into an hdfs file
			Configuration conf = new Configuration();
			FileSystem fs = params.getPaths()[0].getFileSystem(conf);
			Path hdfsFilePath = new Path(params.getPaths()[0].toString() + "/"
					+ tmpFileName);

			copyFromLocal(localFilePath, fs, hdfsFilePath);

			return hdfsFilePath;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			IOUtils.closeQuietly(buffWriter);
		}

	}

	/**
	 * This function to copy a file from local file system to HDFS file stystem
	 * 
	 * @author ibrahimsabek
	 * @param localPath
	 * @param hdfsPath
	 * @throws IOException
	 */
	private static void copyFromLocal(Path localPath, FileSystem hdfsFS,
			Path hdfsPath) throws IOException {
		FSDataOutputStream out = hdfsFS.create(hdfsPath);
		FileInputStream localInputStream = new FileInputStream(new File(
				localPath.toString()));
		int bytesRead;
		byte[] localBuffer = new byte[1024];
		while ((bytesRead = localInputStream.read(localBuffer)) > 0) {
			out.write(localBuffer, 0, bytesRead);
		}

		localInputStream.close();
		out.close();
	}

	/**
	 * function to list files in a certain directory
	 * 
	 * @author ibrahimsabek
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static Path[] getFilesListInPath(Path path) throws IOException{
		FileSystem fileSystem = path.getFileSystem(new Configuration());
		FileStatus[] matchingDirs = fileSystem.listStatus(path);
		Path[] pathsArr = new Path[matchingDirs.length];
		for(int i = 0; i < matchingDirs.length; i++){
			pathsArr[i] = matchingDirs[i].getPath();
		}
		return pathsArr;
	}

	/**
	 * Get the actual size of all data in the given directory. If the input is
	 * a single file, its size is returned immediately. If the input is a
	 * directory, we returns the total size of all data in that directory.
	 * If there is a global index, the size is retrieved from that global index.
	 * Otherwise, we add up all the sizes of single files.
	 * @param fs - the file system that contains the path
	 * @param path - the path that contains the data
	 * @return
	 * @throws IOException 
	 */
  public static long getPathSize(FileSystem fs, Path path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    // 1- Check if the path points to a file
    if (!fileStatus.isDir())
      return fileStatus.getLen();
    // 2- Check if the input is indexed and get the cached size
    GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(fs, path);
    if (gIndex != null) {
      long totalSize = 0;
      for (Partition partition : gIndex)
        totalSize += partition.size;
      return totalSize;
    }
    // 3- Get the total size of all non-hidden files
    long totalSize = 0;
    FileStatus[] allFiles = fs.listStatus(path, SpatialSite.NonHiddenFileFilter);
    for (FileStatus subFile : allFiles) {
      if (!subFile.isDir())
        totalSize += subFile.getLen();
    }
    return totalSize;
  }
  
  /**
   * Used to check whether files are compressed or not to remove their
   * extension.
   */
  private static final CompressionCodecFactory compressionCodecs = 
      new CompressionCodecFactory(new Configuration());
  
  /**
   * Returns the extension of the file after removing any possible suffixes
   * for compression
   * @param path
   * @return
   */
  public static String getExtensionWithoutCompression(Path path) {
    String extension = "";
    String fname = path.getName().toLowerCase();
    if (compressionCodecs.getCodec(path) == null) {
      // File not compressed, get the extension
      int last_dot = fname.lastIndexOf('.');
      if (last_dot >= 0) {
        extension = fname.substring(last_dot + 1);
      }
    } else {
      // File is comrpessed, get the extension before the compression
      int last_dot = fname.lastIndexOf('.');
      if (last_dot > 0) {
        int prev_dot = fname.lastIndexOf('.', last_dot - 1);
        if (prev_dot >= 0) {
          extension = fname.substring(prev_dot + 1, last_dot);
        }
      }
    }
    return extension;
  }

  public static CompressionCodec getCodec(Path file) {
    return compressionCodecs.getCodec(file);
  }

}
