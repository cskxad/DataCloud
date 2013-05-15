package com.xad.hadoop;

import com.xad.hadoop.utils.ApacheAccessLog;
import com.xad.hadoop.utils.DesEncrypter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

/**
 *
 */
public class Utils {

    public static int getHour(String line) {
        String time = line.substring(1, line.indexOf(']'));
        DateTimeParser parser = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss:SSS").getParser();
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(parser).toFormatter();
        DateTime dateTime = formatter.parseDateTime(time);
        return dateTime.get(DateTimeFieldType.clockhourOfDay());
    }

    public static final long getTimeMillis(String line) {
        String time = line.substring(0, line.indexOf('[')).trim();
        DateTimeParser parser = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS").getParser();
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(parser).toFormatter();
        DateTime dateTime = formatter.parseDateTime(time);
        return (dateTime.getMillis() / 1000);
    }

    public static long getMinuteSlice(String line) {
        String time = line.substring(0, line.indexOf('[')).trim();
        DateTimeParser parser = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS").getParser();
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(parser).toFormatter();
        DateTime dateTime = formatter.parseDateTime(time);
        int minOfDay = dateTime.getMinuteOfDay();
//        System.out.println("Min of Day = "+minOfDay + " for Date : "+time);
        String minSlice = "";

        if(minOfDay > 999) {
            minSlice = ""+minOfDay;
        } else if(minOfDay < 1000 && minOfDay > 99) {
            minSlice = "0"+minOfDay;
        } else if(minOfDay < 100 && minOfDay > 9) {
            minSlice = "00"+minOfDay;
        } else if(minOfDay < 10 ) {
            minSlice = "000"+minOfDay;
        }

        long timeInMinutesSinceEpoch = dateTime.getMillis() / (60 * 1000);
        return timeInMinutesSinceEpoch;
    }

    public static int convertToHour(String date) {
        DateTimeParser parser = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS").getParser();
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(parser).toFormatter();
        DateTime dateTime = formatter.parseDateTime(date);
        return dateTime.get(DateTimeFieldType.clockhourOfDay());
    }

    public static void main(String[] args) {
        String line = "10.211.171.76 [29/Nov/2011:09:35:58 -0500] \"GET /rest/banner?v=1.1&k=xpV84otSiHKrLOK2XcY_hF5Aam3TjobXev28q7Fs96rapob9NZQ5Vw..&uid=917141291605688444&appid=Android&devid=Mozilla%2F5" +
                "oc=30096&age=22&gender=f&ip=208.54.35.207 HTTP/1.1\" 200 1530 902 \"-\"";

        String line2 = "208.54.5.129 [15/Oct/2011:00:00:00 -0400] \"GET /rest/xadbanner/layout/pandora/_ui/fonts/TitilliumText22L006-webfont.ttf HTTP/1.1\" 200 22868 534 \"Mozilla/5.0 (Linux; U; Android 2.3.4; en-us; T-Mobile G2 Build/GRJ22) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1\"";

        String line3 = "63.251.207.53 [15/Oct/2011:00:00:00 -0400] \"GET /rest/local?i=wellsfargo+online&i=ashish&lat=39.738083&long=-104.98629&loc=Denver%2CCO&n_ad=4&l=ad&k=glB9Ut_ScRDjZlJpaS9FNe7QoslmHTLJ9ZeYspjAZncVnQVw8ARprg..&uid=72eb51bf-1fc2-47bc-b2af-f9d4b77d5090&v=1.1&auto_imp=1 HTTP/1.0\" 200 171 1775313 \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022)\"";

        ApacheAccessLog accessLogs = new ApacheAccessLog();
        try {
            ApacheAccessLog.AccessLog log = accessLogs.parse(line3);
//            System.out.println(log);
//            System.out.println(log.toCsv());
//            System.out.println(log);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(parseAccesskey("glB9Ut_ScRDjZlJpaS9FNe7QoslmHTLJ9ZeYspjAZncVnQVw8ARprg.."));

    }

    public static String parseAccesskey(String key) {
        String[] keyData = new String[3];
        keyData[0] = "false";
        String STR_DEC_KEY = "V-Enable";
        DesEncrypter mEncrypter;

        mEncrypter = new DesEncrypter(STR_DEC_KEY);

        if (key != null && !(key.equalsIgnoreCase(""))) {
            try {
                String temp = mEncrypter.decrypt(key);
                keyData[0] = "";
                keyData[2] = temp.substring(0, temp.indexOf(":")); // appid
                return keyData[2];
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
}