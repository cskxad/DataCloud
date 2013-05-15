package com.xad.hadoop.csvmappers;

import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 */
public class NotificationDataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final NullWritable nullWritable = NullWritable.get();

    public static final int Search_ID_idx = 0;
    public static final int Timestamp_Idx = 1;
    public static final int App_Id_Idx = 2;
    public static final int Ad_Id_Idx = 3;
    public static final int Is_Internal_Notification = 4;
    public static final int Notify_Type_Idx = 5;
    public static final int Notify_Type_Int_Idx = 6;

    public static enum NOTIFICATION_COUNTER {
        ERROR_COUNTER,
        NOTIFICATION_COUNT,
        MISSING_SEARCH_ID
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            // parse the CSV file for notification data
            String[] adEngineLogData = CsvUtils.csvLineAsArray(line, ",");

            String requestType = adEngineLogData[0];

            if ("notify".equalsIgnoreCase(requestType)) {

                String[] notificationData = new String[7];
                notificationData[Search_ID_idx] = adEngineLogData[27];
                notificationData[Timestamp_Idx] = adEngineLogData[2];
                notificationData[App_Id_Idx] = adEngineLogData[3];
                notificationData[Ad_Id_Idx] = adEngineLogData[29];
                
                // check if multiple notifications are present
                if(adEngineLogData[29] != null && adEngineLogData[29].contains(",")) {
                    // notification for multiple Ads
                    String[] adIds = adEngineLogData[29].split(",");

                    for (int i = 0; i < adIds.length; i++) {
                        // set the Ad ID here
                        notificationData[Ad_Id_Idx] = adIds[i];
                        notificationData[Is_Internal_Notification] = "false";

                        String userNotifyType = adEngineLogData[32];
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

                    String userNotifyType = adEngineLogData[32];
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
