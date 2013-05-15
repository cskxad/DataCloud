package com.xad.hadoop.mappers;

import com.xad.hadoop.utils.AdUtils;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class EdaSessionLogFileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    public static final int SEARCHID_INDEX = 0;
    public static final int TIMESTAMP_INDEX = 1;
    public static final int REQUEST_TYPE_INDEX = 2;
    public static final int REQUEST_TYPESTR_INDEX = 3;
    public static final int INPUT_TYPE_INDEX = 4;
    public static final int INPUT_TYPESTR_INDEX = 5;
    public static final int CSINPUT_TYPE_INDEX = 6;
    public static final int SEARCH_TYPE_INDEX = 7;
    public static final int SEARCH_TYPESTR_INDEX = 8;
    public static final int APPID_INDEX = 9;
    public static final int APP_VERSION_INDEX = 10;
    public static final int SESSIONID_INDEX = 11;
    public static final int UNIQUEID_INDEX = 12;
    public static final int PHONENO_INDEX = 13;
    public static final int DEVICEID_INDEX = 14;
    public static final int RESOURCE_TYPE_INDEX = 15;
    public static final int SEARCH_KEYWORDS_INDEX = 16;
    public static final int CITY_INDEX = 17;
    public static final int STATE_INDEX = 18;
    public static final int ZIPCODE_INDEX = 19;
    public static final int LATITUDE_INDEX = 20;
    public static final int LONGITUDE_INDEX = 21;
    public static final int COUNTRY_INDEX = 22;
    public static final int AGE_INDEX = 23;
    public static final int SEX_INDEX = 24;
    public static final int TRAFFIC_SOURCE_INDEX = 25;
    public static final int CLIENT_IP_INDEX = 26;
    public static final int REQUEST_SERVER_IP_INDEX = 27;
    public static final int LOCATION_TYPE_INDEX = 28;
    public static final int SECONDARY_KEYWORD_INDEX = 29;
    public static final int STATUS_INDEX = 30;
    public static final int ERROR_CODE_INDEX = 31;
    public static final int FILTER_FLAG_INDEX = 32;
    public static final int TOTAL_RESULT_INDEX = 33;
    public static final int RESULT_VENDOR_INDEX = 34;
    public static final int ADVENDOR_INDEX = 35;
    public static final int NUMFEATURED_INDEX = 36;
    public static final int NUM_SWITCHPITCH_INDEX = 37;
    public static final int CAMPAIGNID_INDEX = 38;

    // RAW FILE constants
    public static final int RAW_FILE_TIME_STAMP_IDX = 0;
    public static final int RAW_FILE_REQ_TYPE_IDX = 1;
    public static final int RAW_FILE_INPUT_TYPE_IDX = 2;
    public static final int RAW_FILE_CS_INPUT_TYPE_IDX = 3;
    public static final int RAW_FILE_SEARCH_TYPE_IDX = 4;
    public static final int RAW_FILE_APP_ID_IDX = 5;
    public static final int RAW_FILE_APP_VER_IDX = 6;
    public static final int RAW_FILE_SESSION_ID_IDX = 7;
    public static final int RAW_FILE_PHONE_NO_IDX = 8;
    public static final int RAW_FILE_DEVICE_ID_IDX = 9;
    public static final int RAW_FILE_RESOURCE_TYPE_IDX = 10;
    public static final int RAW_FILE_CARRIER_ID_IDX = 11;
    public static final int RAW_FILE_CITY_IDX = 12;
    public static final int RAW_FILE_STATE_IDX = 13;
    public static final int RAW_FILE_ZIP_CODE_IDX = 14;
    public static final int RAW_FILE_LONGITUDE_IDX = 15;
    public static final int RAW_FILE_LATITUDE_IDX = 16;
    public static final int RAW_FILE_CATEGORY_VER_IDX = 17;
    public static final int RAW_FILE_TIPS_VER_IDX = 18;
    public static final int RAW_FILE_INPUT_IDX = 19;
    public static final int RAW_FILE_FIRST_NAME_IDX = 20;
    public static final int RAW_FILE_LAST_NAME_IDX = 21;
    public static final int RAW_FILE_STREET_NAME_IDX = 22;
    public static final int RAW_FILE_STREET_NUM_IDX = 23;
    public static final int RAW_FILE_FILTERED_FLAG_IDX = 24;
    public static final int RAW_FILE_CATEGORY_URL_IDX = 25;
    public static final int RAW_FILE_SEARCH_INPUT_IDX = 26;
    public static final int RAW_FILE_DESTINATION_IDX = 27;
    public static final int RAW_FILE_VENDOR_IDX = 28;
    public static final int RAW_FILE_F_VENDOR_IDX = 29;
    public static final int RAW_FILE_NUM_FEATURED_IDX = 30;
    public static final int RAW_FILE_TOTAL_RESULT_IDX = 31;
    public static final int RAW_FILE_BILLING_ID_IDX = 32;
    public static final int RAW_FILE_CAMPAIGN_ID_IDX = 33;
    public static final int RAW_FILE_LISTING_ID_IDX = 34;
    public static final int RAW_FILE_BILLING_URL_IDX = 35;
    public static final int RAW_FILE_BUS_PHONE_NO_IDX = 36;
    public static final int RAW_FILE_LISTING_TYPE_IDX = 37;
    public static final int RAW_FILE_MAP_TYPE_IDX = 38;
    public static final int RAW_FILE_IS_MULTI_LOCATION_IDX = 39;
    public static final int RAW_FILE_RESPONCE_TYPE_IDX = 40;
    public static final int RAW_FILE_ADDRESS_CONFIRMED_IDX = 41;
    public static final int RAW_FILE_ROUTE_NUMBER_IDX = 42;
    public static final int RAW_FILE_UNIQUE_ID_IDX = 43;
    public static final int RAW_FILE_REG_AGE_IDX = 44;
    public static final int RAW_FILE_REG_SEX_IDX = 45;
    public static final int RAW_FILE_REG_ZIP_IDX = 46;
    public static final int RAW_FILE_STATUS_IDX = 47;
    public static final int RAW_FILE_LDAPSTATUS_IDX = 48;
    public static final int RAW_FILE_PAY_TYPE_IDX = 49;
    public static final int RAW_FILE_VENDOR_ID_IDX = 50;
    public static final int RAW_FILE_VENDOR_PARAMS_IDX = 51;
    public static final int RAW_FILE_ACCESS_ID_IDX = 52;
    public static final int RAW_FILE_DATA_STATS_IDX = 53;
    public static final int RAW_FILE_USER_AGENT_IDX = 54;
    public static final int RAW_FILE_CELLTOP_HIT_IDX = 55;
    public static final int RAW_FILE_LISTING_SOURCE_IDX = 56;
    public static final int RAW_FILE_NUM_SWITCH_PITCH_IDX = 57;
    public static final int RAW_FILE_SCREEN_ID_IDX = 58;
    public static final int RAW_FILE_SERVER_IP_ADDR_IDX = 59;
    public static final int RAW_FILE_REQUEST_ID_IDX = 60;
    public static final int RAW_FILE_COUNTRY_IDX = 61;
    public static final int RAW_FILE_LOCATION_TYPE_IDX = 62;
    public static final int RAW_FILE_TRAFFIC_SRC_IDX = 63;

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum EDA_SESSION_LOG_COUNTER {
        ERROR_COUNTER,
        SEARCH_COUNT,
        EMPTY_LINE,
        COLUMN_MISMATCH
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            if(line == null || line.length() < 1) {
                System.out.println("empty line");
                context.getCounter(EDA_SESSION_LOG_COUNTER.EMPTY_LINE).increment(1);
                return;
            }

            // parse the CSV file for notification data
            String[] edaSessionLog = AdUtils.parseFileRecord(line);

            // Total columns are 44, but the way split works is a bit diff ;(
            if(edaSessionLog == null || edaSessionLog.length < 65 || edaSessionLog.length > 66) {
                context.getCounter(EDA_SESSION_LOG_COUNTER.COLUMN_MISMATCH).increment(1);
                System.err.println("Line = "+line);
                return;
            }

            String[] searchData = new String [39];
            int searchType = Integer.parseInt(edaSessionLog[RAW_FILE_REQ_TYPE_IDX]);

            if(searchType == 2 || searchType == 3 || searchType == 4 ||
                    searchType == 22 || searchType == 20 || searchType == 24
                    || searchType == 28 || searchType == 29) {
                // its a search request use it

                searchData[TIMESTAMP_INDEX] = edaSessionLog[RAW_FILE_TIME_STAMP_IDX];
                searchData[REQUEST_TYPE_INDEX] = searchType+"";
                searchData[REQUEST_TYPESTR_INDEX] = getRequestTypeFromInt(searchType);

                // Input Type

                String inputType = edaSessionLog[RAW_FILE_INPUT_TYPE_IDX];

                searchData[INPUT_TYPE_INDEX] = inputType;
                searchData[INPUT_TYPESTR_INDEX] = getInputTypeFromInt(Integer.parseInt(inputType));
                searchData[CSINPUT_TYPE_INDEX] = edaSessionLog[RAW_FILE_CS_INPUT_TYPE_IDX];
                searchData[SEARCH_TYPE_INDEX] = edaSessionLog[RAW_FILE_SEARCH_TYPE_IDX];
                searchData[SEARCH_TYPESTR_INDEX] = getSearchTypeFromInt(Integer.parseInt(edaSessionLog[RAW_FILE_SEARCH_TYPE_IDX]));
                searchData[APPID_INDEX] = edaSessionLog[RAW_FILE_APP_ID_IDX];
                searchData[APP_VERSION_INDEX] = edaSessionLog[RAW_FILE_APP_VER_IDX];
                searchData[SESSIONID_INDEX] = edaSessionLog[RAW_FILE_SESSION_ID_IDX];

                searchData[SEARCHID_INDEX] = edaSessionLog[RAW_FILE_REQUEST_ID_IDX];
                searchData[UNIQUEID_INDEX] = edaSessionLog[RAW_FILE_UNIQUE_ID_IDX];
                searchData[PHONENO_INDEX] = edaSessionLog[RAW_FILE_PHONE_NO_IDX];
                searchData[DEVICEID_INDEX] = edaSessionLog[RAW_FILE_DEVICE_ID_IDX];
                searchData[RESOURCE_TYPE_INDEX] = edaSessionLog[RAW_FILE_RESOURCE_TYPE_IDX];
                searchData[SEARCH_KEYWORDS_INDEX] = edaSessionLog[RAW_FILE_INPUT_IDX];
                searchData[CITY_INDEX] = edaSessionLog[RAW_FILE_CITY_IDX];
                searchData[STATE_INDEX] = edaSessionLog[RAW_FILE_STATE_IDX];
                searchData[ZIPCODE_INDEX] = edaSessionLog[RAW_FILE_ZIP_CODE_IDX];
                searchData[LATITUDE_INDEX] = edaSessionLog[RAW_FILE_LATITUDE_IDX];

                searchData[LONGITUDE_INDEX] = edaSessionLog[RAW_FILE_LONGITUDE_IDX];
                searchData[COUNTRY_INDEX] = edaSessionLog[RAW_FILE_COUNTRY_IDX];
                String ageStr = edaSessionLog[RAW_FILE_REG_AGE_IDX];
                if(ageStr == null || ageStr.length() < 1) {
                    searchData[AGE_INDEX] = "-1";
                } else {
                    searchData[AGE_INDEX] = edaSessionLog[RAW_FILE_REG_AGE_IDX];
                }

                searchData[SEX_INDEX] = edaSessionLog[RAW_FILE_REG_SEX_IDX];
                searchData[TRAFFIC_SOURCE_INDEX] = edaSessionLog[RAW_FILE_TRAFFIC_SRC_IDX];

                // get from Data Stats

                // Get the Data Stats column
                String dataStats = edaSessionLog[RAW_FILE_DATA_STATS_IDX];
                Map<String, String> dataStatsData = parseDataStats(dataStats);

                searchData[CLIENT_IP_INDEX] = dataStatsData != null ? dataStatsData.get("CLIENT_IP") : "";

                searchData[REQUEST_SERVER_IP_INDEX] = edaSessionLog[RAW_FILE_SERVER_IP_ADDR_IDX];
                searchData[LOCATION_TYPE_INDEX] = edaSessionLog[RAW_FILE_LOCATION_TYPE_IDX];

                // TBD, get from Data Stats

                String getAdKeyWords = dataStatsData.get("GET_ADS_KWD");

                if(getAdKeyWords == null) {
                    searchData[SECONDARY_KEYWORD_INDEX] = "";
                } else {
                    searchData[SECONDARY_KEYWORD_INDEX] = dataStatsData.get("GET_ADS_KWD").replace(",", "^").replace("[", "").replace("]", "");
                }

                int statusCode = Integer.parseInt(edaSessionLog[RAW_FILE_STATUS_IDX]);

                String statusValue = "Success";

                if(statusCode != 0) {
                    statusValue = "Failure";
                    searchData[ERROR_CODE_INDEX] = edaSessionLog[RAW_FILE_STATUS_IDX];
                } else {
                    searchData[ERROR_CODE_INDEX] = "0";
                }

                searchData[STATUS_INDEX] = statusValue;


                searchData[FILTER_FLAG_INDEX] = edaSessionLog[RAW_FILE_FILTERED_FLAG_IDX];
                searchData[TOTAL_RESULT_INDEX] = edaSessionLog[RAW_FILE_TOTAL_RESULT_IDX];
                searchData[RESULT_VENDOR_INDEX] = edaSessionLog[RAW_FILE_VENDOR_IDX];
                searchData[ADVENDOR_INDEX] = edaSessionLog[RAW_FILE_F_VENDOR_IDX].toLowerCase();
                searchData[NUMFEATURED_INDEX] = edaSessionLog[RAW_FILE_NUM_FEATURED_IDX];
                searchData[NUM_SWITCHPITCH_INDEX] = edaSessionLog[RAW_FILE_NUM_SWITCH_PITCH_IDX];

                // Need to fit this
                searchData[CAMPAIGNID_INDEX] = "TBD";
                String searchDataString = CsvUtils.arrayAsCsv(searchData, ",");
                context.write(nullWritable, new  Text(searchDataString.replace("\n", " ")));
                context.getCounter(EDA_SESSION_LOG_COUNTER.SEARCH_COUNT).increment(1);
            }


        } catch (Exception e) {
            context.getCounter(EDA_SESSION_LOG_COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line "+line);
        }
    }

    protected String getRequestTypeFromInt(int searchType) {

        switch (searchType) {
            case 2:
                return "Business Search";

            case 3:
                return "Category Search";

            case 4:
                return "Resedential Search";

            case 22:
                return "Get Sponsored API";

            case 20:
                return "Single Search API";

            case 24:
                return "Reverse Lookup";

            case 28:
                return "Enhanced Content API";

            case 29:
                return " Most Popular API";

            default:
                System.out.println("unknown search type");
                return "Unknown";
        }
    }

    protected String getInputTypeFromInt(int inputType) {
        switch (inputType) {
            case 1:
                return "Text";

            case 2:
                return "Voice";

            case 3:
                return "Suggestive Search";

            case 4:
                return "Browse Search";

            case 5:
                return "Whats near By";

            case 6:
                return "Recent";

            case 7:
                return "Prefilled";

            case 8:
                return "Operator Input";

            default:
                return "Uknown:"+inputType;
        }
    }

    protected String getSearchTypeFromInt(int searchType) {
        switch (searchType) {
            case 1:
                return "Business Search";

            case 2:
                return "Category Search";

            case 3:
                return "Residential Search";

            case 4:
                return "Operator Search";

            case 5:
                return "Reverse Lookup Search";

            default:
                return "Unkown:"+searchType;

        }
    }

    protected Map<String, String> parseDataStats(String dataStats) {
        if(dataStats == null) {
            return null;
        }

        Map<String, String> keyValue = new HashMap<String, String>(5);

        String[] splitString = dataStats.split("\t");

        for (int i = 0; i < splitString.length; i++) {
            String s = splitString[i];
            String[] keyValuePair = s.split("=");
            if (keyValuePair.length == 2) {
                keyValue.put(keyValuePair[0], keyValuePair[1]);
            } else {
                keyValue.put(keyValuePair[0], "");
            }
        }

        return keyValue;
    }
}
