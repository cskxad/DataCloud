package com.xad.hadoop.csvmappers;

import com.xad.hadoop.utils.AdUtils;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Mapper for parsing adx_vendorlog table and generating ad vendor stats
 */
public class AdxVendorLogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

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

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum VENDOR_LOG_COUNTERS {
        ERROR_COUNTER,
        AD_COUNT,
        VENDOR_STATS_COUNT
    }

    MultipleOutputs mo;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mo = new MultipleOutputs(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String line = value.toString();
        try {

            line = line.replace("\n", " ");

            // parse the CSV file for notification data
            String[] vendorLogData = CsvUtils.csvLineAsArray(line, ",");
            String[] vendorStatsData = new String [16];
            // adx_vendorlog.RequestId
            vendorStatsData[Search_Id_Idx] = vendorLogData[14];
            // adx_vendorlog.Keyword
            vendorStatsData[Keyword_Idx] = vendorLogData[6];
            // adx_vendorlog.timestamp
            vendorStatsData[Timestamp_Idx] = vendorLogData[0];
            // adx_vendorlog.vendor
            vendorStatsData[Vendor_Idx] = vendorLogData[7].toLowerCase();

            // adx_vendorlog.City
            vendorStatsData[City_Idx] = vendorLogData[1];
            // adx_vendorlog.state
            vendorStatsData[State_Idx] = vendorLogData[2];
            // adx_vendorlog.ZipCode
            vendorStatsData[Zipcode_Idx] = vendorLogData[3];
            // adx_vendorlog.Latitude
            vendorStatsData[Latitude_Idx] = vendorLogData[4];
            // adx_vendorlog.Longitude
            vendorStatsData[Longitude_Idx] = vendorLogData[5];
            // adx_vendorlog.Status
            vendorStatsData[Status_Idx] = vendorLogData[9];
            // adx_vendorlog.numResults
            vendorStatsData[Number_of_Result_Idx] = vendorLogData[8];
            // adx_vendorlog.ResponseTime
            vendorStatsData[Response_Time_Idx] = vendorLogData[10];
            // adx_vendorlog.url
            vendorStatsData[Get_Ad_URL_Idx] = vendorLogData[15];
            // adx_vendorlog.connectiontime
            vendorStatsData[Connection_Time_Idx] = vendorLogData[17];
            // adx_vendorlog.readtime
            vendorStatsData[Read_Time_Idx] = vendorLogData[18];
            // adx_vendorlog.queuetime
            vendorStatsData[Queue_Time_Idx] = vendorLogData[19];
            String outputStr = CsvUtils.arrayAsCsv(vendorStatsData, ",");

            mo.write("vendorstats", nullWritable, new  Text(outputStr));
            context.getCounter(VENDOR_LOG_COUNTERS.VENDOR_STATS_COUNT).increment(1);

            // parse ads
            String filteredListing = vendorLogData[11];
            String nonFilteredListings = vendorLogData[12];

            String[] ads = AdUtils.parseAdInfo(filteredListing, nonFilteredListings, vendorLogData[22]);

            if(ads == null || ads.length < 1) {
                return;
            }

            for (int i = 0; i < ads.length; i++) {
                String ad = ads[i];
                mo.write("ads", nullWritable, new Text(ad));
                context.getCounter(VENDOR_LOG_COUNTERS.AD_COUNT).increment(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Status :: Failed for line"+line);
            context.getCounter(VENDOR_LOG_COUNTERS.ERROR_COUNTER).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mo.close();
    }
}