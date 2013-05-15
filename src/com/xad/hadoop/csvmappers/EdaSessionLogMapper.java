package com.xad.hadoop.csvmappers;

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
public class EdaSessionLogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

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

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum EDA_SESSION_LOG_COUNTER {
        ERROR_COUNTER,
        SEARCH_COUNT
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String line = value.toString();
        try {
            // parse the CSV file for notification data
            String[] edaSessionLog = CsvUtils.csvLineAsArray(line, ",");
            String[] searchData = new String [39];
            int searchType = Integer.parseInt(edaSessionLog[2]);

            if(searchType == 2 || searchType == 3 || searchType == 4 ||
                    searchType == 22 || searchType == 20 || searchType == 24
                    || searchType == 28 || searchType == 29) {
                // its a search request use it

                searchData[TIMESTAMP_INDEX] = edaSessionLog[1];
                searchData[REQUEST_TYPE_INDEX] = searchType+"";
                searchData[REQUEST_TYPESTR_INDEX] = getRequestTypeFromInt(searchType);

                // Input Type

                String inputType = edaSessionLog[3];

                searchData[INPUT_TYPE_INDEX] = inputType;
                searchData[INPUT_TYPESTR_INDEX] = getInputTypeFromInt(Integer.parseInt(inputType));
                searchData[CSINPUT_TYPE_INDEX] = edaSessionLog[4];
                searchData[SEARCH_TYPE_INDEX] = edaSessionLog[5];
                searchData[SEARCH_TYPESTR_INDEX] = getSearchTypeFromInt(Integer.parseInt(edaSessionLog[5]));
                searchData[APPID_INDEX] = edaSessionLog[6];
                searchData[APP_VERSION_INDEX] = edaSessionLog[7];
                searchData[SESSIONID_INDEX] = edaSessionLog[8];

                searchData[SEARCHID_INDEX] = edaSessionLog[61];
                searchData[UNIQUEID_INDEX] = edaSessionLog[44];
                searchData[PHONENO_INDEX] = edaSessionLog[9];
                searchData[DEVICEID_INDEX] = edaSessionLog[10];
                searchData[RESOURCE_TYPE_INDEX] = edaSessionLog[11];
                searchData[SEARCH_KEYWORDS_INDEX] = edaSessionLog[20];
                searchData[CITY_INDEX] = edaSessionLog[13];
                searchData[STATE_INDEX] = edaSessionLog[14];
                searchData[ZIPCODE_INDEX] = edaSessionLog[15];
                searchData[LATITUDE_INDEX] = edaSessionLog[17];

                searchData[LONGITUDE_INDEX] = edaSessionLog[16];
                searchData[COUNTRY_INDEX] = edaSessionLog[62];
                String ageStr = edaSessionLog[45];
                if(ageStr == null || ageStr.length() < 1) {
                    searchData[AGE_INDEX] = "-1";
                } else {
                    searchData[AGE_INDEX] = edaSessionLog[45];
                }

                searchData[SEX_INDEX] = edaSessionLog[46];
                searchData[TRAFFIC_SOURCE_INDEX] = edaSessionLog[64];

                // get from Data Stats
                
                // Get the Data Stats column
                String dataStats = edaSessionLog[54];
                Map<String, String> dataStatsData = parseDataStats(dataStats);
                
                searchData[CLIENT_IP_INDEX] = dataStatsData != null ? dataStatsData.get("CLIENT_IP") : "";

                searchData[REQUEST_SERVER_IP_INDEX] = edaSessionLog[58];
                searchData[LOCATION_TYPE_INDEX] = edaSessionLog[63];

                // TBD, get from Data Stats

                String getAdKeyWords = dataStatsData.get("GET_ADS_KWD");

                if(getAdKeyWords == null) {
                    searchData[SECONDARY_KEYWORD_INDEX] = "";
                } else {
                    searchData[SECONDARY_KEYWORD_INDEX] = dataStatsData.get("GET_ADS_KWD").replace(",", "^").replace("[", "").replace("]", "");
                }

//                searchData[SECONDARY_KEYWORD_INDEX] = dataStatsData != null ? dataStatsData.get("GET_ADS_KWD") : "";


                int statusCode = Integer.parseInt(edaSessionLog[48]);

                String statusValue = "Success";

                if(statusCode != 0) {
                    statusValue = "Failure";
                    searchData[ERROR_CODE_INDEX] = edaSessionLog[48];
                } else {
                    searchData[ERROR_CODE_INDEX] = "0";
                }

                searchData[STATUS_INDEX] = statusValue;


                searchData[FILTER_FLAG_INDEX] = edaSessionLog[25];
                searchData[TOTAL_RESULT_INDEX] = edaSessionLog[32];
                searchData[RESULT_VENDOR_INDEX] = edaSessionLog[29];
                searchData[ADVENDOR_INDEX] = edaSessionLog[30].toLowerCase();
                searchData[NUMFEATURED_INDEX] = edaSessionLog[31];
                searchData[NUM_SWITCHPITCH_INDEX] = edaSessionLog[59];

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
