package com.xad.hadoop.mappers;

import com.xad.hadoop.utils.AdUtils;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *
 */
public class ImpressionTrackingFileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

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

    //Mukesh (2012-08-01 - Start)
    public static final int Ad_Key_Idx = 37;
    public static final int Quality_Score_Idx = 38;
    public static final int Bus_Location_Id_Idx = 39;
    public static final int Creative_Id_Idx = 40;
    public static final int Neptune_Bkt_Id_Idx = 41;
    public static final int Group_Id_Idx = 42;
    public static final int Campaign_Id_Idx = 43;
    public static final int Ad_Unit_Id_Idx = 44;
    //Mukesh (2012-08-01 - End)

    // Raw Files constants
    public static final int RAW_FILE_TIMESTAMPMILLIS_IDX = 0;
    public static final int RAW_FILE_PUBLISHERID_IDX = 1;
    public static final int RAW_FILE_IMPURL_IDX = 2;
    public static final int RAW_FILE_ADNAME_IDX = 3;
    public static final int RAW_FILE_ADTEASER_IDX = 4;
    public static final int RAW_FILE_ADADDRESS_IDX = 5;
    public static final int RAW_FILE_ADCITY_IDX = 6;
    public static final int RAW_FILE_ADSTATE_IDX = 7;
    public static final int RAW_FILE_ADZIPCODE_IDX = 8;
    public static final int RAW_FILE_ADLATITUDE_IDX = 9;
    public static final int RAW_FILE_ADLONGITUDE_IDX = 10;
    public static final int RAW_FILE_ADPHONENUMBER_IDX = 11;
    public static final int RAW_FILE_ADRANK_IDX = 12;
    public static final int RAW_FILE_ADSEARCHINPUT_IDX = 13;
    public static final int RAW_FILE_ADCATEGORY_IDX = 14;
    public static final int RAW_FILE_ADRATING_IDX = 15;
    public static final int RAW_FILE_ADDESCRIPTION_IDX = 16;
    public static final int RAW_FILE_ADBUSHOURS_IDX = 17;
    public static final int RAW_FILE_RECORDID_IDX = 18;
    public static final int RAW_FILE_BIDRATE_IDX = 19;
    public static final int RAW_FILE_SECBIDRATE_IDX = 20;
    public static final int RAW_FILE_LISTINGTYPE_IDX = 21;
    public static final int RAW_FILE_INPUT_IDX = 22;
    public static final int RAW_FILE_KEYWORD_IDX = 23;
    public static final int RAW_FILE_MAPPEDINPUT_IDX = 24;
    public static final int RAW_FILE_SECONDARYINPUT_IDX = 25;
    public static final int RAW_FILE_CITY_IDX = 26;
    public static final int RAW_FILE_STATE_IDX = 27;
    public static final int RAW_FILE_ZIPCODE_IDX = 28;
    public static final int RAW_FILE_LATITUDE_IDX = 29;
    public static final int RAW_FILE_LONGITUDE_IDX = 30;
    public static final int RAW_FILE_LOCATIONTYPE_IDX = 31;
    public static final int RAW_FILE_APPID_IDX = 32;
    public static final int RAW_FILE_REQUESTID_IDX = 33;
    public static final int RAW_FILE_WEBSITE_IDX = 34;
    public static final int RAW_FILE_PROFILEURL_IDX = 35;
    public static final int RAW_FILE_DISPLAYURL_IDX = 36;
    public static final int RAW_FILE_ADTEASER_2_IDX = 37;
    public static final int RAW_FILE_CLIENTBID_IDX = 38;
    public static final int RAW_FILE_ADVENDOR_IDX = 39;
    public static final int RAW_FILE_PAYTYPE_IDX = 40;
    public static final int RAW_FILE_GETADSREQUESTID_IDX = 41;
    public static final int RAW_FILE_ISMERGE_IDX = 42;
    public static final int RAW_FILE_LISTINGID_IDX = 43;
    public static final int RAW_FILE_ESTECPM_IDX = 44;
    public static final int RAW_FILE_ESTCTR_IDX = 45;
    public static final int RAW_FILE_CREATIVETYPE_IDX = 46;
    public static final int RAW_FILE_ADVERTISERBID_IDX = 47;
    public static final int RAW_FILE_DISTANCE_IDX = 48;
    public static final int RAW_FILE_ADID_IDX = 49;

    //Mukesh (2012-08-01 - Start)
    public static final int RAW_FILE_ADKEY_IDX = 50;
    public static final int RAW_FILE_QUALITYSCORE_IDX = 51;
    public static final int RAW_FILE_BUSLOCATIONID_IDX = 52;
    public static final int RAW_FILE_CREATIVEID_IDX = 53;
    public static final int RAW_FILE_NEPTUNEBKTID_IDX = 54;
    public static final int RAW_FILE_GROUPID_IDX = 55;
    public static final int RAW_FILE_CAMPAIGNID_IDX = 56;
    public static final int RAW_FILE_ADUNITID_IDX = 57;
    //Mukesh (2012-08-01 - End)

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum IMPRESSION_TRACKING_COUNTER {
        AD_COUNTER,
        ERROR_COUNTER,
        MALFORMED_RECORD,
        LESS_COLUMNS,
        EMPTY_LINE
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.out.println( context.getTaskAttemptID() +  " - "+ ((FileSplit)context.getInputSplit()).getPath());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            if(line == null || line.length() < 1) {
                System.out.println("empty line");
                context.getCounter(IMPRESSION_TRACKING_COUNTER.EMPTY_LINE).increment(1);
                return;
            }

            // parse the CSV file for notification data
            String[] impressionTrackingData = AdUtils.parseFileRecord(line);

            //if(impressionTrackingData == null || impressionTrackingData.length < 50 || impressionTrackingData.length > 51) {
            //[Mukesh] It should work in case extra cloumn added in end of the row.
            //if (impressionTrackingData == null || impressionTrackingData.length < 50) {

            //Mukesh (2012-08-01 - Start)
            if (impressionTrackingData == null || impressionTrackingData.length < 58) {
            //Mukesh (2012-08-01 - End)
                System.err.println("Malformed Line = "+line);
                context.getCounter(IMPRESSION_TRACKING_COUNTER.LESS_COLUMNS).increment(1);
                return;
            }

            //String[] adsData = new String[37];

            //Mukesh (2012-08-01 - Start)
            String[] adsData = new String[45];
            //Mukesh (2012-08-01 - End)

            adsData[Search_Id_Idx] = impressionTrackingData[RAW_FILE_GETADSREQUESTID_IDX];
            adsData[DateTime_Idx] = impressionTrackingData[RAW_FILE_TIMESTAMPMILLIS_IDX];
            adsData[Ad_Id_Idx] = impressionTrackingData[RAW_FILE_RECORDID_IDX];

            if(!adsData[Ad_Id_Idx].contains("-")) {
                System.err.println("Malformed record");
                context.getCounter(IMPRESSION_TRACKING_COUNTER.MALFORMED_RECORD).increment(1);
                return;
            }

            adsData[Ad_Name_Idx] = impressionTrackingData[RAW_FILE_ADNAME_IDX];
            adsData[Ad_Teaser_Idx] = impressionTrackingData[RAW_FILE_ADTEASER_IDX];
            adsData[Ad_Teaser2_Idx] = impressionTrackingData[RAW_FILE_ADTEASER_2_IDX];
            adsData[Ad_Address_Idx] = impressionTrackingData[RAW_FILE_ADADDRESS_IDX];
            adsData[Ad_City_Idx] = impressionTrackingData[RAW_FILE_ADCITY_IDX];
            adsData[Ad_State_Idx] = impressionTrackingData[RAW_FILE_ADSTATE_IDX];
            adsData[Ad_Zip_Code_Idx] = impressionTrackingData[RAW_FILE_ADZIPCODE_IDX];
            adsData[Ad_Latitude_Idx] = impressionTrackingData[RAW_FILE_ADLATITUDE_IDX];
            adsData[Ad_Longitude_Idx] = impressionTrackingData[RAW_FILE_ADLONGITUDE_IDX];
            adsData[Ad_Phone_No_Idx] = impressionTrackingData[RAW_FILE_ADPHONENUMBER_IDX];
            adsData[Ad_Rank_Idx] = impressionTrackingData[RAW_FILE_ADRANK_IDX];
            adsData[Ad_Search_Input_Idx] = impressionTrackingData[RAW_FILE_ADSEARCHINPUT_IDX];
            adsData[Ad_Category_Idx] = impressionTrackingData[RAW_FILE_ADCATEGORY_IDX];
            adsData[Ad_Rating_Idx] = impressionTrackingData[RAW_FILE_ADRATING_IDX];
            adsData[Ad_Description_Idx] = impressionTrackingData[RAW_FILE_ADDESCRIPTION_IDX];
            adsData[Ad_Business_Hours_Idx] = impressionTrackingData[RAW_FILE_ADBUSHOURS_IDX];
            adsData[Bid_Rate_Idx] = impressionTrackingData[RAW_FILE_BIDRATE_IDX];
            adsData[Secondary_Bid_Rate_Idx] = impressionTrackingData[RAW_FILE_SECBIDRATE_IDX];
            adsData[Website_URL_Idx] = impressionTrackingData[RAW_FILE_WEBSITE_IDX];
            adsData[Display_URL_Idx] = impressionTrackingData[RAW_FILE_DISPLAYURL_IDX];
            adsData[Profile_URL_Idx] = impressionTrackingData[RAW_FILE_PROFILEURL_IDX];
            adsData[Publisher_Id_Idx] = impressionTrackingData[RAW_FILE_PUBLISHERID_IDX];
            adsData[Response_Bid_Rate_Idx] = impressionTrackingData[RAW_FILE_CLIENTBID_IDX];
            adsData[Ad_Vendor_Idx] = impressionTrackingData[RAW_FILE_ADVENDOR_IDX].toLowerCase();
            adsData[App_Id_Idx] = impressionTrackingData[RAW_FILE_APPID_IDX];
            adsData[Pay_Type_Idx] = impressionTrackingData[RAW_FILE_PAYTYPE_IDX];
            adsData[Impression_URL_Idx] = impressionTrackingData[RAW_FILE_IMPURL_IDX];
            adsData[Estimated_ECPM_Idx] = impressionTrackingData[RAW_FILE_ESTECPM_IDX];
            adsData[Estimated_CTR_Idx] = impressionTrackingData[RAW_FILE_ESTCTR_IDX];
            adsData[Creative_type_Idx] = impressionTrackingData[RAW_FILE_CREATIVETYPE_IDX];
            adsData[Advertiser_Bid_Idx] = impressionTrackingData[RAW_FILE_ADVERTISERBID_IDX];
            adsData[Ad_Returned_Idx] = "true";
            adsData[Listing_Vendor_Idx] = impressionTrackingData[RAW_FILE_ADVENDOR_IDX].toLowerCase();
            adsData[AD_UNIQUE_ID_Idx] = impressionTrackingData[RAW_FILE_ADID_IDX];

            //Mukesh (2012-08-01 - Start)
            adsData[Ad_Key_Idx] = impressionTrackingData[RAW_FILE_ADKEY_IDX];
            adsData[Quality_Score_Idx] = impressionTrackingData[RAW_FILE_QUALITYSCORE_IDX];
            adsData[Bus_Location_Id_Idx] = impressionTrackingData[RAW_FILE_BUSLOCATIONID_IDX];
            adsData[Creative_Id_Idx] = impressionTrackingData[RAW_FILE_CREATIVEID_IDX];
            adsData[Neptune_Bkt_Id_Idx] = impressionTrackingData[RAW_FILE_NEPTUNEBKTID_IDX];
            adsData[Group_Id_Idx] = impressionTrackingData[RAW_FILE_GROUPID_IDX];
            adsData[Campaign_Id_Idx] = impressionTrackingData[RAW_FILE_CAMPAIGNID_IDX];
            adsData[Ad_Unit_Id_Idx] = impressionTrackingData[RAW_FILE_ADUNITID_IDX];
            //Mukesh (2012-08-01 - End)

            context.getCounter(IMPRESSION_TRACKING_COUNTER.AD_COUNTER).increment(1);
            context.write(nullWritable, new Text(CsvUtils.arrayAsCsv(adsData, ",")));
        } catch (Exception e) {
            e.printStackTrace();
            context.getCounter(IMPRESSION_TRACKING_COUNTER.ERROR_COUNTER).increment(1);
            System.err.println("Status :: Failed for line = "+line);
        }
    }
}
