package org.csnowfox.kafkatools.utils;

import com.beust.jcommander.converters.ISO8601DateConverter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {

    public static void main(String[] args) {
        System.out.println(getISO8601Timestamp(convertToGMT(new Date())));
    }

    public static Date convertToGMT(Date date) {
        //Local Time Zone Calendar Instance
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int zoneOffset = calendar.get(Calendar.ZONE_OFFSET);
        int dstOffset = calendar.get(Calendar.DST_OFFSET);
        calendar.add(Calendar.MILLISECOND, -(dstOffset+zoneOffset));
        return calendar.getTime();
    }

    /**
     * 传入Data类型日期，返回字符串类型时间（ISO8601标准时间）
     * @param date
     * @return
     */
    public static String getISO8601Timestamp(Date date){
        TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(tz);
        String nowAsISO = df.format(date);
        return nowAsISO;
    }

    /**
     * 获取Date
     * @param timeStr 时分秒 HHmmss
     * @return
     * @throws ParseException
     */
    public static Date getTime(String timeStr) throws ParseException {
        return getDateClass(timeStr, "HHmmss");
    }

    /**
     * 获取Date
     * @param dateStr 年月日时分秒 yyyyMMddHHmmss
     * @return
     * @throws ParseException
     */
    public static Date getDateClass(String dateStr) throws ParseException {
        return getDateClass(dateStr, "yyyyMMddHHmmss");
    }

    /**
     * 获取Date
     * @param dateStr 时间字符串
     * @param formater 时间格式
     * @return
     */
    public static Long getDate(String dateStr, String formater) {
        try {
            return new SimpleDateFormat(formater).parse(dateStr).getTime();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 转化Date为String
     * @param date 时间
     * @param formater 转化格式
     * @return
     */
    public static String format(Date date, String formater) {
        return new SimpleDateFormat(formater).format(date);
    }

    /**
     * 转化Date为String
     * @param date 时间
     * @param formater 转化格式
     * @return
     */
    public static String format(long date, String formater) {
        return new SimpleDateFormat(formater).format(new Date(date));
    }

    /**
     * 转化Date为String
     * @param date 时间
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String format(long date) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(date));
        } catch (Exception e) {
            return "0";
        }
    }


    /**
     * 获取Date时间
     * @param dateStr 时间
     * @param formater 格式
     * @return
     */
    public static Date getDateClass(String dateStr, String formater) {
        try {
            return new SimpleDateFormat(formater).parse(dateStr);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取Calendar类型的时间
     * @param l timeInMillis
     * @return
     */
    public static Calendar getDate(Long l) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(l);
        return c;
    }

    /**
     * 把前一参数的日期与后一参数的时间组合成一个新时间
     * @param dateTime 日期
     * @param timeTime 时分秒
     * @return
     */
    public static long getCurrentDate(Date dateTime, Long timeTime) {
        Calendar c = Calendar.getInstance();
        c.setTime(dateTime);
        int year = c.get(Calendar.YEAR);
        int month = c.get(Calendar.MONTH);
        int day = c.get(Calendar.DAY_OF_MONTH);
        c.setTimeInMillis(timeTime);
        c.set(year, month, day);
        return c.getTimeInMillis();
    }

    /**
     * 把前一参数的日期与后一参数的时间组合成一个新时间
     * @param dateTime 日期
     * @param timeTime 时分秒
     * @return
     */
    public static long getCurrentDate(Long dateTime, Long timeTime) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(dateTime);
        int year = c.get(Calendar.YEAR);
        int month = c.get(Calendar.MONTH);
        int day = c.get(Calendar.DAY_OF_MONTH);
        c.setTimeInMillis(timeTime);
        c.set(year, month, day);
        return c.getTimeInMillis();
    }

    /**
     * 返回第一个不为Null的Date，如都为空返回当前时间
     * @param params
     * @return
     */
    public static Date getNotNullByParamsSeq(Date... params) {
        for (Date d : params) {
            if (d != null) {
                return d;
            }
        }

        return new Date();
    }

    /**
     * 返回第一个不为Null的Date，如都为空返回当前时间
     * @param params
     * @return
     */
    public static Date getNotNullByParamsSeq(Long... params) {
        for (Long d : params) {
            if (d != null && d != 0) {
                return new Date(d);
            }
        }

        return new Date();
    }

}
