package com.xad.hadoop.mappers;

import com.xad.hadoop.utils.AdUtils;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 */
public class AdEngineLogFileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final NullWritable nullWritable = NullWritable.get();

    public static final int Search_ID_idx = 0;
    public static final int Timestamp_Idx = 1;
    public static final int App_Id_Idx = 2;
    public static final int Ad_Id_Idx = 3;
    public static final int Is_Internal_Notification = 4;
    public static final int Notify_Type_Idx = 5;
    public static final int Notify_Type_Int_Idx = 6;

    // Raw File Indexes
    public static final int RAW_FILE_TIMESTAMP_IDX = 0;
    public static final int RAW_FILE_REQTYPE_IDX = 1;
    public static final int RAW_FILE_SESSIONID_IDX = 2;
    public static final int RAW_FILE_APPID_IDX = 3;
    public static final int RAW_FILE_PHONENUM_IDX = 4;
    public static final int RAW_FILE_CITY_IDX = 5;
    public static final int RAW_FILE_STATE_IDX = 6;
    public static final int RAW_FILE_ZIP_IDX = 7;
    public static final int RAW_FILE_LAT_IDX = 8;
    public static final int RAW_FILE_LONG_IDX = 9;
    public static final int RAW_FILE_PRIMINPUTS_IDX = 10;
    public static final int RAW_FILE_SECINPUTS_IDX = 11;
    public static final int RAW_FILE_MAPPEDINPUT_IDX = 12;
    public static final int RAW_FILE_SWPITINPUTS_IDX = 13;
    public static final int RAW_FILE_STATUS_IDX = 14;
    public static final int RAW_FILE_SEARCHTYPE_IDX = 15;
    public static final int RAW_FILE_RESULTVENDORS_IDX = 16;
    public static final int RAW_FILE_NUMRESULTS_IDX = 17;
    public static final int RAW_FILE_USERAGENT_IDX = 18;
    public static final int RAW_FILE_CLIENTIP_IDX = 19;
    public static final int RAW_FILE_REQUESTID_IDX = 20;
    public static final int RAW_FILE_PRIORITY_IDX = 21;
    public static final int RAW_FILE_MAXBID_1_IDX = 22;
    public static final int RAW_FILE_MAXBID_2_IDX = 23;
    public static final int RAW_FILE_AVGBID_IDX = 24;
    public static final int RAW_FILE_CORRECTEDCITY_IDX = 25;
    public static final int RAW_FILE_CORRECTEDSTATE_IDX = 26;
    public static final int RAW_FILE_BILLINGRECORDID_IDX = 27;
    public static final int RAW_FILE_GETADSREQUESTID_IDX = 28;
    public static final int RAW_FILE_USERREF_IDX = 29;
    public static final int RAW_FILE_AUTOIMPR_IDX = 30;
    public static final int RAW_FILE_USERNOTIFYINPUT_IDX = 31;
    public static final int RAW_FILE_GENERICINPUT_IDX = 32;
    public static final int RAW_FILE_LOCATIONTYPE_IDX = 33;
    public static final int RAW_FILE_RESPONSETIME_IDX = 34;
    public static final int RAW_FILE_REQUESTTIME_IDX = 35;
    public static final int RAW_FILE_PUBIDS_IDX = 36;
    public static final int RAW_FILE_TRAFFICSRC_IDX = 37;
    public static final int RAW_FILE_REMOVEDPRIORITY_IDX = 38;
    public static final int RAW_FILE_DEVICEID_IDX = 39;
    public static final int RAW_FILE_PROFILING_IDX = 40;
    public static final int RAW_FILE_AGE_IDX = 41;
    public static final int RAW_FILE_GENDER_IDX = 42;
    public static final int RAW_FILE_REMOVEDPRIORITYSLOTS_IDX = 43;

    public static enum NOTIFICATION_COUNTER {
        ERROR_COUNTER,
        NOTIFICATION_COUNT,
        MISSING_SEARCH_ID,
        EMPTY_LINE,
        LESS_COLUMN
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            if(line == null || line.length() < 1) {
                System.out.println("empty line");
                context.getCounter(NOTIFICATION_COUNTER.EMPTY_LINE).increment(1);
                return;
            }

            // parse the CSV file for notification data
            String[] adEngineLogData = AdUtils.parseFileRecord(line);

            // Total columns are 44, but the way split works is a bit diff ;(
            if(adEngineLogData == null || adEngineLogData.length < 44 || adEngineLogData.length > 45) {
                context.getCounter(NOTIFICATION_COUNTER.LESS_COLUMN).increment(1);
                System.err.println("Missing column for line "+adEngineLogData.length);
                System.err.println("Line = "+line);
                return;
            }

            String requestType = adEngineLogData[RAW_FILE_REQTYPE_IDX];

            if ("notify".equalsIgnoreCase(requestType)) {

                String[] notificationData = new String[7];
                notificationData[Search_ID_idx] = adEngineLogData[RAW_FILE_GETADSREQUESTID_IDX];
                notificationData[Timestamp_Idx] = adEngineLogData[RAW_FILE_TIMESTAMP_IDX];
                notificationData[App_Id_Idx] = adEngineLogData[RAW_FILE_APPID_IDX];
                notificationData[Ad_Id_Idx] = adEngineLogData[RAW_FILE_BILLINGRECORDID_IDX];

                // check if multiple notifications are present
                if(adEngineLogData[RAW_FILE_BILLINGRECORDID_IDX] != null && adEngineLogData[RAW_FILE_BILLINGRECORDID_IDX].contains(",")) {
                    // notification for multiple Ads
                    String[] adIds = adEngineLogData[RAW_FILE_BILLINGRECORDID_IDX].split(",");

                    for (int i = 0; i < adIds.length; i++) {
                        // set the Ad ID here
                        notificationData[Ad_Id_Idx] = adIds[i];
                        notificationData[Is_Internal_Notification] = "false";

                        String userNotifyType = adEngineLogData[RAW_FILE_USERNOTIFYINPUT_IDX];
                        if (userNotifyType != null) {
                            notificationData[Notify_Type_Idx] = getNotificationType(Integer.parseInt(userNotifyType));
                            notificationData[Notify_Type_Int_Idx] = userNotifyType;
                            if ("9".equals(userNotifyType)) {
                                notificationData[Is_Internal_Notification] = "true";
                            }
                            if(notificationData[Search_ID_idx] != null || notificationData[Search_ID_idx].length() > 0) {
                                context.getCounter(NOTIFICATION_COUNTER.NOTIFICATION_COUNT).increment(1);
                                context.write(nullWritable, new Text(CsvUtils.arrayAsCsv(notificationData, ",")));
                            } else {
                                context.getCounter(NOTIFICATION_COUNTER.MISSING_SEARCH_ID).increment(1);
                            }
                        }
                    }
                } else {

                    notificationData[Is_Internal_Notification] = "false";

                    String userNotifyType = adEngineLogData[RAW_FILE_USERNOTIFYINPUT_IDX];
                    if (userNotifyType != null) {
                        notificationData[Notify_Type_Idx] = getNotificationType(Integer.parseInt(userNotifyType));
                        notificationData[Notify_Type_Int_Idx] = userNotifyType;
                        if ("9".equals(userNotifyType)) {
                            notificationData[Is_Internal_Notification] = "true";
                        }
                        if(notificationData[Search_ID_idx] != null || notificationData[Search_ID_idx].length() > 0) {
                            context.getCounter(NOTIFICATION_COUNTER.NOTIFICATION_COUNT).increment(1);
                            context.write(nullWritable, new Text(CsvUtils.arrayAsCsv(notificationData, ",")));
                        } else {
                            context.getCounter(NOTIFICATION_COUNTER.MISSING_SEARCH_ID).increment(1);
                        }
                    }
                }
            }
        } catch (Exception e) {
            context.getCounter(NOTIFICATION_COUNTER.ERROR_COUNTER).increment(1);
            System.out.println("Status :: Failed for line "+line);
        }
    }

    protected String getNotificationType(int notifyCode) {
        switch (notifyCode) {
            case 1:
                return "Description";

            case 2:
                return "Call";

            case 3:
                return "SMS";

            case 4:
                return "Map";

            case 5:
                return "Directions";

            case 6:
                return "Review";

            case 7:
                return "No Result";

            case 8:
                return "No Impression";

            case 9:
                return "Impression";

            case 10:
                return "Click";

            case 11:
                return "Call";

            case 12:
                return "Website";

            case 13:
                return "Review Count1";

            case 14:
                return "Review Count2";

            case 15:
                return "Reviews All";

            case 16:
                return "Business Image";

            case 17:
                return "Business Operations Hours";

            case 18:
                return "More Info";

            case 19:
                return "Video";

            case 20:
                return "Email";

            case 21:
                return "Nearby";

            case 22:
                return "Coupon";

            case 23:
                return "Profile";

            case 24:
                return "Offers";

            case 25:
                return "Request 411";

            case 26:
                return "Save To App";

            case 27:
                return "Save To Phone Book";

            case 28:
                return "Arrival";

            case 29:
                return "Checkin";

            case 30:
                return "Banner Call";

            case 31:
                return "Business Name";

            case 32:
                return "Confirn Click";

            case 39:
                return "User Impression";

            default:
                return "Invalid:" + notifyCode;
        }
    }
}
