package com.xad.hadoop.reports.advendor;

import com.xad.hadoop.core.VendorStatsConstants;
import com.xad.hadoop.core.VendorStatsCounterWritable;
import com.xad.hadoop.utils.CsvUtils;
import com.xad.hadoop.utils.DateUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Vendor Fill Rate Mapper
 */
public class VendorFillRateMapper extends Mapper<LongWritable, Text, Text, VendorStatsCounterWritable> {

    public static enum VendorFillRateMapper_COUNTER {
        ERROR_COUNTER,
        EMPTY_LINE,
        CONNTIME_ERROR,
        RESPTIME_ERROR
    }

    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            if(line == null || line.length() < 1) {
                context.getCounter(VendorFillRateMapper_COUNTER.EMPTY_LINE).increment(1);
                return;
            }
            String[] vendorStats = CsvUtils.csvLineAsArray(line, ",");

            String adVendor = vendorStats[VendorStatsConstants.VENDOR_IDX];
            String numResult = vendorStats[VendorStatsConstants.NUMBER_OF_RESULT_IDX];
            String timeStamp = vendorStats[VendorStatsConstants.TIMESTAMP_IDX];
            String connectionTime = vendorStats[VendorStatsConstants.CONNECTION_TIME_IDX];
            String responseTime = vendorStats[VendorStatsConstants.RESPONSE_TIME_IDX];

            int hourOfDay = DateUtils.getHourOfDayFromDate(timeStamp);

            int numberOfResult = 0;

            try {
                numberOfResult = Integer.parseInt(numResult);
            } catch (Exception ex) {
                // noop
            }

            int connTime = 0;

            try {
                connTime = Integer.parseInt(connectionTime);
            } catch (Exception ex) {
               context.getCounter(VendorFillRateMapper_COUNTER.CONNTIME_ERROR).increment(1);
            }

            int respTime = 0;

            try {
                connTime = Integer.parseInt(responseTime);
            } catch (Exception ex) {
                context.getCounter(VendorFillRateMapper_COUNTER.RESPTIME_ERROR).increment(1);
            }
            context.write(new Text(adVendor+"_"+hourOfDay), new VendorStatsCounterWritable(1, numberOfResult, respTime, connTime));
        } catch (Exception e) {
            context.getCounter(VendorFillRateMapper_COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line "+line);
        }
    }
}
