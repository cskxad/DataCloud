package com.xad.hadoop.legacy;

import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 */
public class AdxLegacyImpressionTrackingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    // Index definition
    public static final int DateTime_Idx = 1;
    public static final int Search_Id_Idx = 0;
    public static final int Ad_Id_Idx = 2;
    public static final int Ad_Name_Idx = 3;
    public static final int Ad_Teaser_Idx = 4;
    public static final int Ad_Teaser2_Idx = 5;
    public static final int Ad_Address_Idx = 6;
    public static final int Ad_City_Idx = 7;
    public static final int Ad_State_Idx = 8;
    public static final int Ad_Zip_Code_Idx = 9;
    public static final int Ad_Latitude_Idx = 10;
    public static final int Ad_Longitude_Idx = 11;
    public static final int Ad_Phone_No_Idx = 12;
    public static final int Ad_Rank_Idx = 13;
    public static final int Ad_Search_Input_Idx = 14;
    public static final int Ad_Category_Idx = 15;
    public static final int Ad_Rating_Idx = 16;
    public static final int Ad_Description_Idx = 17;
    public static final int Ad_Business_Hours_Idx = 18;
    public static final int Bid_Rate_Idx = 19;
    public static final int Secondary_Bid_Rate_Idx = 20;
    public static final int Website_URL_Idx = 21;
    public static final int Display_URL_Idx = 22;
    public static final int Profile_URL_Idx = 23;
    public static final int Publisher_Id_Idx = 24;
    public static final int Response_Bid_Rate_Idx = 25;
    public static final int Ad_Vendor_Idx = 26;
    public static final int App_Id_Idx = 27;
    public static final int Pay_Type_Idx = 28;
    public static final int Impression_URL_Idx = 29;
    public static final int Estimated_ECPM_Idx = 30;
    public static final int Estimated_CTR_Idx = 31;
    public static final int Creative_type_Idx = 32;
    public static final int Advertiser_Bid_Idx = 33;
    public static final int Ad_Returned_Idx = 34;
    public static final int Listing_Vendor_Idx = 35;
    public static final int AD_UNIQUE_ID_Idx = 36;

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum IMPRESSION_TRACKING_COUNTER {
        AD_COUNTER,
        ERROR_COUNTER,
        MALFORMED_RECORD
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            /**
             * Need to clean following fields
             * adname
             * adteaser
             * adteaser2
             * adaddress
             * addescription
             * adbushours
             */

            // parse the CSV file for notification data
            String[] impressionTrackingData = CsvUtils.csvLineAsArray(line, ",");

            String[] adsData = new String[37];

            adsData[Search_Id_Idx] = impressionTrackingData[41];
            adsData[DateTime_Idx] = impressionTrackingData[0];
            adsData[Ad_Id_Idx] = impressionTrackingData[19];

            if(!adsData[Ad_Id_Idx].contains("-")) {
                System.err.println("Malformed record");
                context.getCounter(IMPRESSION_TRACKING_COUNTER.MALFORMED_RECORD).increment(1);
                return;
            }

            adsData[Ad_Name_Idx] = impressionTrackingData[3];
            adsData[Ad_Teaser_Idx] = impressionTrackingData[4];
            adsData[Ad_Teaser2_Idx] = impressionTrackingData[5];
            adsData[Ad_Address_Idx] = impressionTrackingData[6];
            adsData[Ad_City_Idx] = impressionTrackingData[7];
            adsData[Ad_State_Idx] = impressionTrackingData[8];
            adsData[Ad_Zip_Code_Idx] = impressionTrackingData[9];
            adsData[Ad_Latitude_Idx] = impressionTrackingData[10];
            adsData[Ad_Longitude_Idx] = impressionTrackingData[11];
            adsData[Ad_Phone_No_Idx] = impressionTrackingData[12];
            adsData[Ad_Rank_Idx] = impressionTrackingData[13];
            adsData[Ad_Search_Input_Idx] = impressionTrackingData[14];
            adsData[Ad_Category_Idx] = impressionTrackingData[15];
            adsData[Ad_Rating_Idx] = impressionTrackingData[16];
            adsData[Ad_Description_Idx] = impressionTrackingData[17];
            adsData[Ad_Business_Hours_Idx] = impressionTrackingData[18];
            adsData[Bid_Rate_Idx] = impressionTrackingData[20];
            adsData[Secondary_Bid_Rate_Idx] = impressionTrackingData[21];
            adsData[Website_URL_Idx] = impressionTrackingData[35];
            adsData[Display_URL_Idx] = impressionTrackingData[36];
            adsData[Profile_URL_Idx] = impressionTrackingData[37];
            adsData[Publisher_Id_Idx] = impressionTrackingData[1];
            adsData[Response_Bid_Rate_Idx] = impressionTrackingData[38];
            adsData[Ad_Vendor_Idx] = impressionTrackingData[39].toLowerCase();
            adsData[App_Id_Idx] = impressionTrackingData[33];
            adsData[Pay_Type_Idx] = impressionTrackingData[40];
            adsData[Impression_URL_Idx] = impressionTrackingData[2];
            adsData[Estimated_ECPM_Idx] = impressionTrackingData[44];
            adsData[Estimated_CTR_Idx] = impressionTrackingData[45];
            adsData[Creative_type_Idx] = impressionTrackingData[46];
            adsData[Advertiser_Bid_Idx] = impressionTrackingData[47];
            adsData[Ad_Returned_Idx] = "true";
            adsData[Listing_Vendor_Idx] = impressionTrackingData[39].toLowerCase();
            adsData[AD_UNIQUE_ID_Idx] = impressionTrackingData[19];

            context.getCounter(IMPRESSION_TRACKING_COUNTER.AD_COUNTER).increment(1);
            context.write(nullWritable, new Text(CsvUtils.arrayAsCsv(adsData, ",")));
        } catch (Exception e) {
            e.printStackTrace();
            context.getCounter(IMPRESSION_TRACKING_COUNTER.ERROR_COUNTER).increment(1);
            System.err.println("Status :: Failed for line = "+line);
        }
    }
}
