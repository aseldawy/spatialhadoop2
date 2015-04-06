package edu.umn.cs.spatialHadoop.nasa;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class TimeRange implements PathFilter {
  /**Day format of time range*/
  static final SimpleDateFormat DayFormat = new SimpleDateFormat("yyyy.MM.dd");
  /**Month format of time range*/
  static final SimpleDateFormat MonthFormat = new SimpleDateFormat("yyyy.MM");
  /**Year format of time range*/
  static final SimpleDateFormat YearFormat = new SimpleDateFormat("yyyy");

  static final String DayRegex = "^(\\d{4}\\.\\d{2}\\.\\d{2})$";
  static final String MonthRegex = "^(\\d{4}\\.\\d{2})$";
  static final String YearRegex = "^(\\d{4})$";
  /**Regular expression for a time range*/
  static final Pattern TimeRange = Pattern.compile("^(\\d{4}\\.\\d{2}\\.\\d{2})\\.\\.(\\d{4}\\.\\d{2}\\.\\d{2})$");

  /**Start time (inclusive)*/
  public long start;
  /**End time (exclusive)*/
  public long end;
  
  public TimeRange(String str) throws ParseException {
    Matcher matcher = TimeRange.matcher(str);
    if (!matcher.matches())
      throw new RuntimeException("Illegal time range '"+str+"'");
    start = DayFormat.parse(matcher.group(1)).getTime();
    end = DayFormat.parse(matcher.group(2)).getTime();
  }
  
  public TimeRange(long start, long end) {
    this.start = start;
    this.end = end;
  }
  
  @Override
  public String toString() {
    return DayFormat.format(this.start) + " -- "+DayFormat.format(this.end);
  }

  @Override
  public boolean accept(Path pathname) {
    try {
      String filename = pathname.getName();
      // Beginning and end times of this file
      long begin, end;
      if (filename.matches(YearRegex)) {
        // Year
        Date filetime = YearFormat.parse(filename);
        begin = filetime.getTime();
        filetime.setYear(filetime.getYear() + 1);
        end = filetime.getTime();
      } else if (filename.matches(MonthRegex)) {
        // Month
        Date filetime = MonthFormat.parse(filename);
        begin = filetime.getTime();
        filetime.setMonth(filetime.getMonth() + 1);
        end = filetime.getTime();
      } else if (filename.matches(DayRegex)) {
        // Month
        Date filetime = DayFormat.parse(filename);
        begin = filetime.getTime();
        filetime.setDate(filetime.getDate() + 1);
        end = filetime.getTime();
      } else {
        return false;
      }
      // Return true if the file is totally contained in the range
      return (begin >= this.start && end <= this.end);
    } catch (ParseException e) {
      return false;
    }
  }
}