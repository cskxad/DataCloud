package com.xad.hadoop.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

/**
 * Date related Utils
 */
public class DateUtils {

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Returns the Hour of Day from a given Date string, with a specified Data format
     * 
     * @param date Date for which Hour is to be derieved
     * @param dateFormat    Date Format
     *
     * @return  Hour of Day
     */
    public static int getHourOfDayFromDate(String date, String dateFormat) {
        DateTimeParser parser = DateTimeFormat.forPattern(dateFormat).getParser();
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(parser).toFormatter();
        DateTime dateTime = formatter.parseDateTime(date);
        return dateTime.get(DateTimeFieldType.clockhourOfDay());
    }

    /**
     * Returns the Hour of Day from a given Date string, with a specified Data format.
     * Uses the default Date Format
     *
     * @param date Date for which Hour is to be derieved
     *
     * @return  Hour of Day
     */
    public static int getHourOfDayFromDate(String date) {
        return getHourOfDayFromDate(date, DEFAULT_DATE_FORMAT);
    }

    public static void main(String[] args) {
        String date = "2012-03-11 17:00:00";

        System.out.println(DateUtils.getHourOfDayFromDate(date));
    }

    /**
     * Returns Day of week, for a given date with a specified Data Format
     *
     * @param date          Date for
     * @param dateFormat
     * @return
     */
    public static int getDayOfWeek(String date, String dateFormat) {
        DateTimeParser parser = DateTimeFormat.forPattern(dateFormat).getParser();
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(parser).toFormatter();
        DateTime dateTime = formatter.parseDateTime(date);
        return dateTime.getDayOfWeek();
    }
}
