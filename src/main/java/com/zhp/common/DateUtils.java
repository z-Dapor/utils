package com.zhp.common;

import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {
    public static final String DEFAULT_FORMAT_STR = "yyyyMMdd";
    public static final String SECOND_DATE_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";

    public static final DateTimeFormatter YYYYMMDD = DateTimeFormat.forPattern(DEFAULT_FORMAT_STR);
    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormat.forPattern(SECOND_DATE_FORMAT_STR);

    public static String getReportDate(String dateParam) {
        try {
            return getRelativeDate(dateParam, 0);
        } catch (IllegalArgumentException e) {
            int relativeDays = Integer.valueOf(dateParam);
            return getRelativeDate(relativeDays);
        }
    }

    public static String getRelativeDate(int relativeDays) {
        return new DateTime().plusDays(relativeDays).toString(YYYYMMDD);
    }

    public static String getRelativeDate(String strDate, int relativeDays) {
        return parseDefaultDateTime(strDate).plusDays(relativeDays).toString(YYYYMMDD);
    }

    public static String getYestodayReportDate(String strDate) {
        return getRelativeDate(strDate, -1);
    }

    public static String getYestodayReportDate() {
        return getRelativeDate(-1);
    }

    public static List<String> getLastDateList(int backspaceDays) {
        return getLastDateList(new DateTime(), backspaceDays);
    }

    public static List<String> getLastDateList(String startDate, int backspaceDays) {
        return getLastDateList(parseDefaultDateTime(startDate), backspaceDays);
    }

    public static List<String> getDateRangeList(String startDate, String endDate) {
        int days = daysBetween(startDate, endDate);
        return days > 0 ? getLastDateList(endDate, days) : getLastDateList(startDate, -days);
    }

    public static boolean isInDate(String strDate, String startDate, String endDate) {
        long targetTime = parseDefaultDateTime(strDate).getMillis();
        long startTime = parseDefaultDateTime(startDate).getMillis();
        long endTime = parseDefaultDateTime(endDate).getMillis();
        return targetTime >= startTime && targetTime <= endTime;
    }

    public static String getConditionDate(long t) {
        return formatDefaultDateTime(new DateTime(t));
    }

    public static String getReportTime(String strDate) {
        return parseDefaultDateTime(strDate).toString(YYYY_MM_DD_HH_MM_SS);
    }

    public static String getCurrentDateStr() {
        return getRelativeDate(0);
    }

    public static int daysBetween(String startDate, String endDate) {
        DateTime startTime = parseDefaultDateTime(startDate);
        DateTime endTime = parseDefaultDateTime(endDate);
        int days = Days.daysBetween(startTime, endTime).getDays();
        return days < 0 ? days - 1 : days + 1;
    }

    private static List<String> getLastDateList(DateTime t, int backspaceDays) {
        if (backspaceDays < 1) {
            throw new IllegalArgumentException("backspaceDays = " + backspaceDays + " can not < 1");
        }
        List<String> result = new ArrayList<String>();
        for (int i = 0; i < backspaceDays; i++) {
            result.add(formatDefaultDateTime(t));
            t = t.plusDays(-1);
        }
        return result;
    }

    private static DateTime parseDefaultDateTime(String strDate) {
        return YYYYMMDD.parseDateTime(strDate);
    }

    private static String formatDefaultDateTime(DateTime dateTime) {
        return dateTime.toString(YYYYMMDD);
    }

}
