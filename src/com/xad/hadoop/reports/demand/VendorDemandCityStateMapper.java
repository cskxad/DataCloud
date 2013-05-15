package com.xad.hadoop.reports.demand;

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
 * Vendor Demand mapper for a City State
 */
public class VendorDemandCityStateMapper extends Mapper<LongWritable, Text, Text, VendorStatsCounterWritable> {

    public static enum DMRequestMapper_COUNTER {
        ERROR_COUNTER,
        EMPTY_LINE,
        NULL_CITY_STATE,
        CITY_STATE_COUNT,
        PARSE_ERROR
    }

    public static final String DELIMITER = "[{^}]";

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            if (line == null || line.length() < 1) {
                context.getCounter(DMRequestMapper_COUNTER.EMPTY_LINE).increment(1);
                return;
            }
            String[] vendorStats = CsvUtils.csvLineAsArray(line, ",");

            String adVendor = vendorStats[VendorStatsConstants.VENDOR_IDX];
            String keyword = vendorStats[VendorStatsConstants.KEYWORD_IDX];
            String city = vendorStats[VendorStatsConstants.CITY_IDX];
            String state = vendorStats[VendorStatsConstants.STATE_IDX];
            if (city == null || state == null || city.length() < 1 || state.length() < 1) {
                context.getCounter(DMRequestMapper_COUNTER.NULL_CITY_STATE).increment(1);
                return;
            }
            String numResult = vendorStats[VendorStatsConstants.NUMBER_OF_RESULT_IDX];

            int numberOfResult = 0;

            try {
                numberOfResult = Integer.parseInt(numResult);
            } catch (Exception ex) {
                // noop
                context.getCounter(DMRequestMapper_COUNTER.PARSE_ERROR).increment(1);
            }

            int numberOfAds = Integer.parseInt(numResult);
            context.write(new Text(adVendor + DELIMITER + city + DELIMITER + state),
                    new VendorStatsCounterWritable(1, numberOfAds));
            context.write(new Text(state), new VendorStatsCounterWritable(1, numberOfAds));
            context.getCounter(DMRequestMapper_COUNTER.CITY_STATE_COUNT).increment(1);
        } catch (Exception e) {
            context.getCounter(DMRequestMapper_COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line " + line);
        }
    }
}