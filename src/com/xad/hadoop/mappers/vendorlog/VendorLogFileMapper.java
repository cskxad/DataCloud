package com.xad.hadoop.mappers.vendorlog;

import com.xad.hadoop.utils.AdUtils;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper to process Vendor Log file. Instead of DB, the offline file needs to be processed
 *
 * The output shall be vendor Stats
 */
public class VendorLogFileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    public static final int Search_Id_Idx = 0;
    public static final int Keyword_Idx = 1;
    public static final int Timestamp_Idx = 2;
    public static final int Vendor_Idx = 3;
    public static final int City_Idx = 4;
    public static final int State_Idx = 5;
    public static final int Zipcode_Idx = 6;
    public static final int Latitude_Idx = 7;
    public static final int Longitude_Idx = 8;
    public static final int Status_Idx = 9;
    public static final int Number_of_Result_Idx = 10;
    public static final int Response_Time_Idx = 11;
    public static final int Get_Ad_URL_Idx = 12;
    public static final int Connection_Time_Idx = 13;
    public static final int Read_Time_Idx = 14;
    public static final int Queue_Time_Idx = 15;

    // DB export CSV Location count
    public static final int src_timestamp_idx = 0;
    public static final int src_city_idx= 1;
    public static final int src_state_idx = 2;
    public static final int src_zip_idx= 3;
    public static final int src_lat_idx = 4;
    public static final int src_long_idx = 5;
    public static final int src_input_idx = 6;
    public static final int src_keyword_idx = 7;
    public static final int src_vendor_idx = 8;
    public static final int src_numresults_idx = 9;
    public static final int src_status_idx = 10;
    public static final int src_responsetime_idx = 11;
    public static final int src_filteredlistings_idx = 12;
    public static final int src_nonfilteredlistings_idx = 13;
    public static final int src_requestid_idx = 14;
    public static final int src_requrl_idx = 15;
    public static final int src_billingrecordid_idx = 16;
    public static final int src_connectiontime_idx = 17;
    public static final int src_readtime_idx = 18;
    public static final int src_queuetime_idx = 19;
    public static final int src_reqtype_idx = 20;
    public static final int src_keywordtype_idx = 21;
    public static final int src_appid_idx = 22;
    public static final int src_numfilteredlistings_idx = 23;
    public static final int src_numnonfilteredlistings_idx = 24;


    // File based Counter
    public static final int src_new_timestamp_idx = 0;
    public static final int src_new_city_idx= 1;
    public static final int src_new_state_idx = 2;
    public static final int src_new_zip_idx= 3;
    public static final int src_new_lat_idx = 4;
    public static final int src_new_long_idx = 5;
    public static final int src_new_input_idx = 6;
    public static final int src_new_keyword_idx = 7;
    public static final int src_new_vendor_idx = 8;
    public static final int src_new_numresults_idx = 9;
    public static final int src_new_status_idx = 10;
    public static final int src_new_responsetime_idx = 11;
    public static final int src_new_requestid_idx = 12;
    public static final int src_new_requrl_idx = 13;
    public static final int src_new_billingrecordid_idx = 14;
    public static final int src_new_connectiontime_idx = 15;
    public static final int src_new_readtime_idx = 16;
    public static final int src_new_queuetime_idx = 17;
    public static final int src_new_reqtype_idx = 18;
    public static final int src_new_keywordtype_idx = 19;
    public static final int src_new_appid_idx = 20;
    public static final int src_new_numfilteredlistings_idx = 21;
    public static final int src_new_numnonfilteredlistings_idx = 22;

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum VENDOR_LOG_COUNTERS {
        ERROR_COUNTER,
        AD_COUNT,
        VENDOR_STATS_COUNT,
        EMPTY_LINE,
        INVALID_COLUMNS
    }

    MultipleOutputs mo;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mo = new MultipleOutputs(context);
       System.out.println( context.getTaskAttemptID() +  " - "+ ((FileSplit)context.getInputSplit()).getPath());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        try {
            
            if(line == null || line.length() < 1) {
                System.out.println("empty line");
                context.getCounter(VENDOR_LOG_COUNTERS.EMPTY_LINE).increment(1);
                return;
            }

            // parse the CSV file for notification data
            String[] vendorLogData = parserVendorLogRecord(line);
            String[] vendorStatsData = new String [16];

            if(vendorLogData.length == 23) {
                // adx_vendorlog.RequestId
                vendorStatsData[Search_Id_Idx] = vendorLogData[src_new_requestid_idx];
                // adx_vendorlog.Keyword
                vendorStatsData[Keyword_Idx] = vendorLogData[src_new_keyword_idx];
                // adx_vendorlog.timestamp
                vendorStatsData[Timestamp_Idx] = vendorLogData[src_new_timestamp_idx];
                // adx_vendorlog.vendor
                vendorStatsData[Vendor_Idx] = vendorLogData[src_new_vendor_idx].toLowerCase();

                // adx_vendorlog.City
                vendorStatsData[City_Idx] = vendorLogData[src_new_city_idx];
                // adx_vendorlog.state
                vendorStatsData[State_Idx] = vendorLogData[src_new_state_idx];
                // adx_vendorlog.ZipCode
                vendorStatsData[Zipcode_Idx] = vendorLogData[src_new_zip_idx];
                // adx_vendorlog.Latitude
                vendorStatsData[Latitude_Idx] = vendorLogData[src_new_lat_idx];
                // adx_vendorlog.Longitude
                vendorStatsData[Longitude_Idx] = vendorLogData[src_new_long_idx];
                // adx_vendorlog.Status
                vendorStatsData[Status_Idx] = vendorLogData[src_new_status_idx];
                // adx_vendorlog.numResults
                vendorStatsData[Number_of_Result_Idx] = vendorLogData[src_new_numresults_idx];
                // adx_vendorlog.ResponseTime
                vendorStatsData[Response_Time_Idx] = vendorLogData[src_new_responsetime_idx];
                // adx_vendorlog.url
                vendorStatsData[Get_Ad_URL_Idx] = vendorLogData[src_new_requrl_idx];
                // adx_vendorlog.connectiontime
                vendorStatsData[Connection_Time_Idx] = vendorLogData[src_new_connectiontime_idx];
                // adx_vendorlog.readtime
                vendorStatsData[Read_Time_Idx] = vendorLogData[src_new_readtime_idx];
                // adx_vendorlog.queuetime
                vendorStatsData[Queue_Time_Idx] = vendorLogData[src_new_queuetime_idx];
                String outputStr = CsvUtils.arrayAsCsv(vendorStatsData, ",");

                context.write(nullWritable, new Text(outputStr));
                context.getCounter(VENDOR_LOG_COUNTERS.VENDOR_STATS_COUNT).increment(1);
            } else if (vendorLogData.length == 25) {
                // adx_vendorlog.RequestId
                vendorStatsData[Search_Id_Idx] = vendorLogData[src_requestid_idx];
                // adx_vendorlog.Keyword
                vendorStatsData[Keyword_Idx] = vendorLogData[src_keyword_idx];
                // adx_vendorlog.timestamp
                vendorStatsData[Timestamp_Idx] = vendorLogData[src_timestamp_idx];
                // adx_vendorlog.vendor
                vendorStatsData[Vendor_Idx] = vendorLogData[src_vendor_idx].toLowerCase();

                // adx_vendorlog.City
                vendorStatsData[City_Idx] = vendorLogData[src_city_idx];
                // adx_vendorlog.state
                vendorStatsData[State_Idx] = vendorLogData[src_state_idx];
                // adx_vendorlog.ZipCode
                vendorStatsData[Zipcode_Idx] = vendorLogData[src_zip_idx];
                // adx_vendorlog.Latitude
                vendorStatsData[Latitude_Idx] = vendorLogData[src_lat_idx];
                // adx_vendorlog.Longitude
                vendorStatsData[Longitude_Idx] = vendorLogData[src_long_idx];
                // adx_vendorlog.Status
                vendorStatsData[Status_Idx] = vendorLogData[src_status_idx];
                // adx_vendorlog.numResults
                vendorStatsData[Number_of_Result_Idx] = vendorLogData[src_numresults_idx];
                // adx_vendorlog.ResponseTime
                vendorStatsData[Response_Time_Idx] = vendorLogData[src_responsetime_idx];
                // adx_vendorlog.url
                vendorStatsData[Get_Ad_URL_Idx] = vendorLogData[src_requrl_idx];
                // adx_vendorlog.connectiontime
                vendorStatsData[Connection_Time_Idx] = vendorLogData[src_connectiontime_idx];
                // adx_vendorlog.readtime
                vendorStatsData[Read_Time_Idx] = vendorLogData[src_readtime_idx];
                // adx_vendorlog.queuetime
                vendorStatsData[Queue_Time_Idx] = vendorLogData[src_queuetime_idx];
                String outputStr = CsvUtils.arrayAsCsv(vendorStatsData, ",");

                mo.write("vendorstats", nullWritable, new Text(outputStr));
                context.write(nullWritable, new Text(outputStr));
                context.getCounter(VENDOR_LOG_COUNTERS.VENDOR_STATS_COUNT).increment(1);

                // parse ads
                String filteredListing = vendorLogData[src_filteredlistings_idx];
                String nonFilteredListings = vendorLogData[src_nonfilteredlistings_idx];

                String[] ads = AdUtils.parseAdInfo(filteredListing, nonFilteredListings, vendorLogData[src_appid_idx]);
//                context.setStatus("Processed Records = "+context.getCounter(VENDOR_LOG_COUNTERS.VENDOR_STATS_COUNT));

                if(ads == null || ads.length < 1) {
                    return;
                }

                for (int i = 0; i < ads.length; i++) {
                    String ad = ads[i];
                    mo.write("ads", nullWritable, new Text(ad));
                    context.getCounter(VENDOR_LOG_COUNTERS.AD_COUNT).increment(1);
                }
            } else {
                context.getCounter(VENDOR_LOG_COUNTERS.INVALID_COLUMNS).increment(1);
            }

            // parse ads
//            String filteredListing = vendorLogData[src_filteredlistings_idx];
//            String nonFilteredListings = vendorLogData[src_nonfilteredlistings_idx];
//
//            String[] ads = AdUtils.parseAdInfo(filteredListing, nonFilteredListings, vendorLogData[src_appid_idx]);
////            context.getCounter(Task.Counter.MAP_OUTPUT_RECORDS).increment(1);
////            context.progress();
//
//            context.setStatus("Processed Records = "+context.getCounter(VENDOR_LOG_COUNTERS.VENDOR_STATS_COUNT));
//
//            if(ads == null || ads.length < 1) {
//                return;
//            }
//
//            for (int i = 0; i < ads.length; i++) {
//                String ad = ads[i];
//                mo.write("ads", nullWritable, new Text(ad));
//                context.getCounter(VENDOR_LOG_COUNTERS.AD_COUNT).increment(1);
//            }
         } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Status :: Failed for line"+line);
            context.getCounter(VENDOR_LOG_COUNTERS.ERROR_COUNTER).increment(1);
//            System.exit(-1);
        }
    }

    public static final int ADX_VENDOR_LOG_COLUMN_COUNT = 23;

    public static String[] parserVendorLogRecord(String record) throws RuntimeException {
        if(record == null) {
            return null;
        }

        String[] recordSplits = record.split("\\[\\{\\^\\}\\]");

//        if(recordSplits == null || recordSplits.length != ADX_VENDOR_LOG_COLUMN_COUNT) {
//            throw new RuntimeException("Invalid Row. Either null or columns are missing");
//        }

        return recordSplits;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mo.close();
    }
}
