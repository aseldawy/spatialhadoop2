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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

/**
 * Temporal index manager that can determine which files need to be
 * indexed/reindexed on the daily, monthly, and yearly levels.
 * 
 * @author ibrahimsabek
 *
 */

public class TemporalIndexManager {

	/** Logger */
	private static final Log LOG = LogFactory
			.getLog(TemporalIndexManager.class);

	private SimpleDateFormat dayFormat;
	private SimpleDateFormat monthFormat;
	private SimpleDateFormat yearFormat;

	private Path datasetPath;
	private Path indexesPath;
	private FileSystem fileSystem;

	private Path dailyIndexesHomePath;
	private Path monthlyIndexesHomePath;
	private Path yearlyIndexesHomePath;

	private HashMap<String, Boolean> existDailyIndexes;
	private HashMap<String, Boolean> existMonthlyIndexes;
	private HashMap<String, Boolean> existYearlyIndexes;

	private Path[] neededDailyIndexes;
	private Path[] neededMonthlyIndexes;
	private Path[] neededYearlyIndexes;

	public TemporalIndexManager(Path datasetPath, Path indexesPath)
			throws ParseException {
		try {
			this.dayFormat = new SimpleDateFormat("yyyy.MM.dd");
			this.monthFormat = new SimpleDateFormat("yyyy.MM");
			this.yearFormat = new SimpleDateFormat("yyyy");

			this.datasetPath = datasetPath;
			this.indexesPath = indexesPath;

			this.fileSystem = this.indexesPath
					.getFileSystem(new Configuration());

			dailyIndexesHomePath = new Path(this.indexesPath.toString()
					+ "/daily");
			monthlyIndexesHomePath = new Path(this.indexesPath.toString()
					+ "/monthly");
			yearlyIndexesHomePath = new Path(this.indexesPath.toString()
					+ "/yearly");

			initializeIndexesHierarchy();

			existDailyIndexes = new HashMap<String, Boolean>();
			existMonthlyIndexes = new HashMap<String, Boolean>();
			existYearlyIndexes = new HashMap<String, Boolean>();

			loadExistIndexesDictionary();
		} catch (IOException e) {
			LOG.error("Failed to initialize TemporalIndexManager: "
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Creates folder hierarchy for indexes if not exist
	 * 
	 * @throws IOException
	 */
	private void initializeIndexesHierarchy() throws IOException {
		// check daily folder
		if (!this.fileSystem.exists(dailyIndexesHomePath)) {
			this.fileSystem.mkdirs(dailyIndexesHomePath);
		}

		// check monthly folder
		if (!this.fileSystem.exists(monthlyIndexesHomePath)) {
			this.fileSystem.mkdirs(monthlyIndexesHomePath);
		}

		// check yearly folder
		if (!this.fileSystem.exists(yearlyIndexesHomePath)) {
			this.fileSystem.mkdirs(yearlyIndexesHomePath);
		}
	}

	/**
	 * Based on a certain time range, this method filters all directories and
   * determines which files need to be indexed on daily, monthly and yearly
   * levels. After calling this method, you need to call the daily, monthly
   * and yearly getters to return paths required to be indexed.
	 * @param timeRange
	 * @throws IOException
	 * @throws ParseException
	 */
	public void prepareNeededIndexes(String timeRange) throws IOException,
			ParseException {
		if (timeRange == null) {
			LOG.error("TimeRange is empty");
			return;
		}

		// Parse start and end dates
		final Date startDate, endDate;
		try {
			startDate = dayFormat.parse(timeRange.split("\\.\\.")[0]);
			endDate = dayFormat.parse(timeRange.split("\\.\\.")[1]);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error("Use the seperator two periods '..' to seperate from and to dates");
			return;
		} catch (ParseException e) {
			LOG.error("Illegal date format in " + timeRange);
			return;
		}

		// Filter all file/folder paths based on the start-end date range
		FileStatus[] matchingDirs = fileSystem.listStatus(datasetPath,
				new PathFilter() {
					@Override
					public boolean accept(Path p) {
						String dirName = p.getName();
						try {
							Date date = dayFormat.parse(dirName);
							return date.compareTo(startDate) >= 0
									&& date.compareTo(endDate) <= 0;
						} catch (ParseException e) {
							LOG.warn("Cannot parse directory name: " + dirName);
							return false;
						}
					}
				});
		if (matchingDirs.length == 0) {
			LOG.warn("No matching directories for the given input");
		}

		// Re-indexing check for each matching
		for (FileStatus matchingDir : matchingDirs) {
			String matchingDirDateString = NASADatasetUtil
					.extractDateStringFromFileStatus(matchingDir);
			if (existYearlyIndexes.containsKey(NASADatasetUtil
					.getYearFormat(matchingDirDateString))) {
				// needs to re-build year, month and year indexes
				existYearlyIndexes.put(
						NASADatasetUtil.getYearFormat(matchingDirDateString),
						true);
				existMonthlyIndexes.put(
						NASADatasetUtil.getMonthFormat(matchingDirDateString),
						true);
				existDailyIndexes.put(
						NASADatasetUtil.getDayFormat(matchingDirDateString),
						true);
			} else if (existMonthlyIndexes.containsKey(NASADatasetUtil
					.getMonthFormat(matchingDirDateString))) {
				// needs to re-build month and day indexes
				existMonthlyIndexes.put(
						NASADatasetUtil.getMonthFormat(matchingDirDateString),
						true);
				existDailyIndexes.put(
						NASADatasetUtil.getDayFormat(matchingDirDateString),
						true);
			} else if (existDailyIndexes.containsKey(NASADatasetUtil
					.getDayFormat(matchingDirDateString))) {
				// needs to re-build day index
				existDailyIndexes.put(
						NASADatasetUtil.getDayFormat(matchingDirDateString),
						true);
			} else {
				// needs to build a new index
				existDailyIndexes.put(
						NASADatasetUtil.getDayFormat(matchingDirDateString),
						true);

				int daysCountInMonth = getMatchesCountFromMap(
						existDailyIndexes,
						NASADatasetUtil.getMonthFormat(matchingDirDateString));

				if (daysCountInMonth >= getNumDaysPerMonth(NASADatasetUtil
						.extractMonthFromDate(matchingDirDateString))) {
					existMonthlyIndexes.put(NASADatasetUtil
							.getMonthFormat(matchingDirDateString), true);

					int monthsCountInYear = getMatchesCountFromMap(
							existMonthlyIndexes,
							NASADatasetUtil
									.getYearFormat(matchingDirDateString));
					if (monthsCountInYear >= getNumMonthsPerYear()) {
						existYearlyIndexes.put(NASADatasetUtil
								.getYearFormat(matchingDirDateString), true);
					}
				}
			}

		}
		convertNeededIndexesListIntoArrays();
	}

	private void convertNeededIndexesListIntoArrays() {
		neededDailyIndexes = convertFromMapToArray(existDailyIndexes,
				dailyIndexesHomePath);
		neededMonthlyIndexes = convertFromMapToArray(existMonthlyIndexes,
				monthlyIndexesHomePath);
		neededYearlyIndexes = convertFromMapToArray(existYearlyIndexes,
				yearlyIndexesHomePath);
	}

	private Path[] convertFromMapToArray(HashMap<String, Boolean> pathsMap,
			Path homePath) {
		ArrayList<Path> pathsArrayList = new ArrayList<Path>();
		int count = 0;
		for (String pathsMapKey : pathsMap.keySet()) {
			boolean pathsMapValue = pathsMap.get(pathsMapKey);
			if (pathsMapValue) {
				pathsArrayList.add(new Path(homePath.toString() + "/"
						+ pathsMapKey));
				count++;
			}
		}

		Path[] pathsArr = new Path[count];
		for (int i = 0; i < count; i++) {
			pathsArr[i] = pathsArrayList.get(i);
		}
		return pathsArr;
	}

	private int getMatchesCountFromMap(HashMap<String, Boolean> pathsMap,
			String matchingString) {
		int matchesCount = 0;
		for (String pathsMapKey : pathsMap.keySet()) {
			if (pathsMapKey.contains(matchingString)) {
				matchesCount++;
			}
		}
		return matchesCount;
	}

	@SuppressWarnings("unused")
	private int getMatchingCountFromNeededIndexes(
			ArrayList<Path> neededIndexesList, String inputDateString) {
		int count = 0;
		for (Path currPath : neededIndexesList) {
			String currPathString = currPath.toString();
			int start = currPathString.lastIndexOf("/") + 1;
			int end = currPathString.length();
			String currDateString = currPathString.substring(start, end);
			if (currDateString.contains(inputDateString))
				count++;
		}
		return count;
	}

	/**
	 * Loads information about exist indexes on all levels: daily, monthly and
	 * yearly
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	private void loadExistIndexesDictionary() throws IOException,
			ParseException {
		// load daily indexes
		FileStatus[] dailyIndexes = fileSystem.listStatus(dailyIndexesHomePath);
		for (FileStatus dailyIndex : dailyIndexes) {
			if (dailyIndex.isDir()) {
				existDailyIndexes.put(NASADatasetUtil
						.extractDateStringFromFileStatus(dailyIndex), false);
			}
		}

		// load monthly indexes
		FileStatus[] monthlyIndexes = fileSystem
				.listStatus(monthlyIndexesHomePath);
		for (FileStatus monthlyIndex : monthlyIndexes) {
			if (monthlyIndex.isDir()) {
				existMonthlyIndexes.put(NASADatasetUtil
						.extractDateStringFromFileStatus(monthlyIndex), false);
			}
		}

		// load yearly indexes
		FileStatus[] yearlyIndexes = fileSystem
				.listStatus(yearlyIndexesHomePath);
		for (FileStatus yearlyIndex : yearlyIndexes) {
			if (yearlyIndex.isDir()) {
				existYearlyIndexes.put(NASADatasetUtil
						.extractDateStringFromFileStatus(yearlyIndex), false);
			}
		}

	}

	@SuppressWarnings("unused")
	private Date getYearDate(String fullDateString) {
		String yearDateString = NASADatasetUtil.getYearFormat(fullDateString);
		try {
			return yearFormat.parse(yearDateString);
		} catch (ParseException e) {
			LOG.error("Date Parsing Error");
			return null;
		}
	}

	@SuppressWarnings("unused")
	private Date getMonthDate(String fullDateString) {
		String monthDateString = NASADatasetUtil.getMonthFormat(fullDateString);
		try {
			return monthFormat.parse(monthDateString);
		} catch (ParseException e) {
			LOG.error("Date Parsing Error");
			return null;
		}
	}

	@SuppressWarnings("unused")
	private Date getDayDate(String fullDateString) {
		String dayDateString = NASADatasetUtil.getDayFormat(fullDateString);
		try {
			return dayFormat.parse(dayDateString);
		} catch (ParseException e) {
			LOG.error("Date Parsing Error");
			return null;
		}
	}

	private int getNumDaysPerMonth(int month) {
		if (month == 1) {
			return 31; 
		} else if (month == 2) {
			return 28;
		} else if (month == 3) {
			return 31;
		} else if (month == 4) {
			return 30;
		} else if (month == 5) {
			return 31;
		} else if (month == 6) {
			return 30;
		} else if (month == 7) {
			return 31;
		} else if (month == 8) {
			return 31;
		} else if (month == 9) {
			return 30;
		} else if (month == 10) {
			return 31;
		} else if (month == 11) {
			return 30;
		} else if (month == 12) {
			return 31;
		} else {
			return 0;
		}
	}

	private int getNumMonthsPerYear() {
		return 12;
	}

	@SuppressWarnings("unused")
	private int getNumDaysPerYear(int year) {
		return 365;
	}

	public Path[] getNeededDailyIndexes() {
		return neededDailyIndexes;
	}

	public Path[] getNeededMonthlyIndexes() {
		return neededMonthlyIndexes;
	}

	public Path[] getNeededYearlyIndexes() {
		return neededYearlyIndexes;
	}

	public Path getDailyIndexesHomePath() {
		return dailyIndexesHomePath;
	}

	public Path getMonthlyIndexesHomePath() {
		return monthlyIndexesHomePath;
	}

	public Path getYearlyIndexesHomePath() {
		return yearlyIndexesHomePath;
	}

	private static void printUsage() {
		System.out
				.println("Performs a temporal indexing for data stored in hadoop");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<dataset path> - (*) Path to input dataset");
		System.out.println("<index path> - (*) Path to index output");
		System.out.println("time:yyyy.mm.dd..yyyy.mm.dd - (*) Time range");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, ParseException {
		// Parse parameters
		OperationsParams params = new OperationsParams(
				new GenericOptionsParser(args));
		final Path[] paths = params.getPaths();
		if (paths.length <= 1 && !params.checkInput()) {
			printUsage();
			System.exit(1);
		}
		if (paths.length >= 2 && paths[1] == null) {
			printUsage();
			System.exit(1);
		}
		if (params.get("time") == null) {
			System.err.println("You must provide a time range");
			printUsage();
			System.exit(1);
		}

		Path datasetPath = paths[0]; // dataset path
		Path indexesPath = paths[1]; // index path
		String timeRange = params.get("time"); // time range

		TemporalIndexManager temporalIndexManager = new TemporalIndexManager(
				datasetPath, indexesPath);
		temporalIndexManager.prepareNeededIndexes(timeRange);

		Path[] dailyIndexes = temporalIndexManager.getNeededDailyIndexes();
		System.out.println("Daily Indexes: ");
		for (Path path : dailyIndexes) {
			System.out.println(path.toString());
		}

		System.out.println("Monthly Indexes: ");
		Path[] monthlyIndexes = temporalIndexManager.getNeededMonthlyIndexes();
		for (Path path : monthlyIndexes) {
			System.out.println(path.toString());
		}

		System.out.println("Yearly Indexes: ");
		Path[] yearlyIndexes = temporalIndexManager.getNeededYearlyIndexes();
		for (Path path : yearlyIndexes) {
			System.out.println(path.toString());
		}

	}

}
