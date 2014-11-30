/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package edu.umn.cs.spatialHadoop.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;

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

		// Length of input file. We do not depend on split.length because it is
		// not
		// set by input format for performance reason. Setting it in the input
		// format would cost a lot of time because it runs on the client machine
		// while the record reader runs on slave nodes in parallel
		long length = fs.getFileStatus(split.getPath()).getLen();

		FSDataInputStream in = fs.open(split.getPath());
		in.seek(split.getStart());

		// Prepare output file for write
		File tempFile = File.createTempFile(split.getPath().getName(), "hdf");
		OutputStream out = new FileOutputStream(tempFile);

		// A buffer used between source and destination
		byte[] buffer = new byte[1024 * 1024];
		while (length > 0) {
			int numBytesRead = in.read(buffer, 0,
					(int) Math.min(length, buffer.length));
			out.write(buffer, 0, numBytesRead);
			length -= numBytesRead;
		}

		in.close();
		out.close();
		return tempFile.getAbsolutePath();
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
		try {
			// store the dictionary of paths in a local file
			tempFile = new File(tmpFileName);
			Path localFilePath = new Path(tempFile.getAbsolutePath());
			FileOutputStream outStream = new FileOutputStream(tempFile);
			BufferedWriter buffWriter = new BufferedWriter(
					new OutputStreamWriter(outStream));

			for (int i = 0; i < paths.length; i++) {
				buffWriter.write(paths[i].toString());
				buffWriter.newLine();
			}
			buffWriter.close();
			outStream.close();

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
		int bytesRead = 0;
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
}
