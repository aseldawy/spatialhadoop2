/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * A bunch of helper functions used with NASA datasets (e.g. MODIS)
 * 
 * @author ibrahimsabek
 */
public final class NASADatasetUtil {

	public static String extractDateStringFromFileStatus(FileStatus fileStatus) {
		return NASADatasetUtil.extractDateStringFromPath(fileStatus.getPath());
	}

	public static String extractDateStringFromPath(Path filePath) {
		String pathString = filePath.toString();
		String dateString = pathString.substring(
				pathString.lastIndexOf("/") + 1, pathString.length());
		return dateString;
	}

	public static String getYearFormat(String dateString) {
		int start = 0;
		int to = dateString.indexOf(".");
		return dateString.substring(start, to);
	}

	public static String getMonthFormat(String dateString) {
		int start = 0;
		int to = dateString.lastIndexOf(".");
		return dateString.substring(start, to);
	}

	public static String getDayFormat(String dateString) {
		return dateString;
	}

	public static int extractDayFromDate(String dateString) {
		int start = dateString.lastIndexOf(".") + 1;
		int to = dateString.length();
		return Integer.parseInt(dateString.substring(start, to));
	}

	public static int extractMonthFromDate(String dateString) {
		int start = dateString.indexOf(".") + 1;
		int to = dateString.lastIndexOf(".");
		return Integer.parseInt(dateString.substring(start, to));
	}

	public static int extractYearFromDate(String dateString) {
		int start = 0;
		int to = dateString.indexOf(".");
		return Integer.parseInt(dateString.substring(start, to));
	}

	public static int getMatchingFilesCountInPath(Path path,
			String inputDateString) throws IOException {
		int count = 0;
		FileSystem fileSystem = path.getFileSystem(new Configuration());
		FileStatus[] matchingDirs = fileSystem.listStatus(path);
		for (FileStatus matchingDir : matchingDirs) {
			String currPathString = matchingDir.getPath().toString();
			int start = currPathString.lastIndexOf("/") + 1;
			int end = currPathString.length();
			String currDateString = currPathString.substring(start, end);
			if (currDateString.contains(inputDateString)) {
				count++;
			}
		}
		return count;
	}

	public static Path[] getMatchingFilesInPath(Path path,
			final String inputDateString) throws IOException {
		FileSystem fileSystem = path.getFileSystem(new Configuration());
		FileStatus[] matchingDirs = fileSystem.listStatus(path,
				new PathFilter() {
					@Override
					public boolean accept(Path p) {
						String dirName = p.getName();
						if (dirName.contains(inputDateString)) {
							return true;
						} else {
							return false;
						}

					}
				});
		Path[] paths = new Path[matchingDirs.length];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = matchingDirs[i].getPath();
		}
		return paths;
	}

	public static Path[] convertFromListToArray(ArrayList<Path> pathsList) {
		Path[] pathsArr = new Path[pathsList.size()];
		for (int i = 0; i < pathsList.size(); i++) {
			pathsArr[i] = pathsList.get(i);
		}
		return pathsArr;
	}

	public static ArrayList<Path[]> getSortedTuplesInPath(Path parentHomePath,
			String dateString) throws IOException {
		ArrayList<Path[]> pairedPaths = new ArrayList<Path[]>();
		Path[] childrenHomePaths = NASADatasetUtil.getMatchingFilesInPath(
				parentHomePath, dateString);

		// list all file in each child path without sorting
		ArrayList<Path[]> nonSortedChildrenFilePaths = new ArrayList<Path[]>();
		FileSystem fileSystem = parentHomePath
				.getFileSystem(new Configuration());
		for (Path childHomePath : childrenHomePaths) {
			FileStatus[] childFileStatuses = fileSystem
					.listStatus(childHomePath);
			Path[] tmpPathsArr = new Path[childFileStatuses.length];
			for (int i = 0; i < childFileStatuses.length; i++) {
				// if (fileSystem.isFile(childFileStatuses[i].getPath())){
				tmpPathsArr[i] = childFileStatuses[i].getPath();
				// }
			}
			nonSortedChildrenFilePaths.add(tmpPathsArr);
		}

		// pair files together
		int filesCountPerPath = nonSortedChildrenFilePaths.get(0).length;
		for (int i = 0; i < filesCountPerPath; i++) {
			Path[] currPairedPaths = new Path[nonSortedChildrenFilePaths.size()];
			for (int j = 0; j < nonSortedChildrenFilePaths.size(); j++) {
				currPairedPaths[j] = nonSortedChildrenFilePaths.get(j)[i];
			}
			pairedPaths.add(currPairedPaths);
		}
		return pairedPaths;
	}

	public static String getHDFfilePattern(String pathString) {
		String extractionOutput = null;
		Pattern hdfPattern = Pattern.compile("h[0-9]*v[0-9]*");
		Matcher matcher = hdfPattern.matcher(pathString);
		if (matcher.find())
			extractionOutput = matcher.group(0);
		return extractionOutput;
	}
}
