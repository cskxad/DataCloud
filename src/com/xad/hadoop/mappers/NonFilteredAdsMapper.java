package com.xad.hadoop.mappers;

import com.xad.hadoop.utils.AdUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: ashish
 * Date: 3/4/12
 * Time: 2:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class NonFilteredAdsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    public static final int REQUEST_ID_IDX = 0;
    public static final int VENDOR_IDX = 1;
    public static final int LISTING_VENDOR_IDX = 2;
    public static final int APP_ID_IDX = 3;
    public static final int AD_ID_IDX = 4;
    public static final int AD_INFO_IDX = 5;

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum COUNTERS {
        ERROR_COUNTER,
        NONFILTERED_AD_COUNT,
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            if(line == null || line.length() < 1) {
                System.out.println("empty line");
                return;
            }

            // parse the CSV file for notification data
            String[] filteredListing = parserRecord(line);

            String[] ads = AdUtils.parseNonFilteredAdInfo(filteredListing[AD_INFO_IDX], filteredListing[APP_ID_IDX]);

            for (int i = 0; i < ads.length; i++) {
                context.write(nullWritable, new Text(ads[i]));
                context.getCounter(COUNTERS.NONFILTERED_AD_COUNT).increment(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Status :: Failed for line"+line);
            context.getCounter(COUNTERS.ERROR_COUNTER).increment(1);
        }
    }

    public static String[] parserRecord(String record) throws RuntimeException {
        if(record == null) {
            return null;
        }

        String[] recordSplits = record.split("\\[\\{\\^\\}\\]");

        if(recordSplits == null || recordSplits.length != 6) {
            throw new RuntimeException("Invalid Row. Either null or columns are missing");
        }

        return recordSplits;
    }
}
