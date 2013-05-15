package com.xad.hadoop.reports.advendor;

import com.xad.hadoop.core.VendorStatsCounterWritable;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Output format is
 *
 * keyword, Ad vendor, city, state, number of request, number of times ad returned, total number of Ads returned
 */
public class VendorDemandReducer extends Reducer<Text, VendorStatsCounterWritable, NullWritable, Text> {

    public static enum COUNTER {
        LESS_THAN_500_SEARCHES
    }

    @Override
    protected void reduce(Text key, Iterable<VendorStatsCounterWritable> values, Context context) throws IOException, InterruptedException {
        int requestCount = 0;
        int numberOfTimesAdReturned = 0;
        int totalAdsReturned = 0;
        for (VendorStatsCounterWritable value : values) {
            requestCount++;

            if(value.getNumberOfResult() > 0) {
                numberOfTimesAdReturned++;
            }
            totalAdsReturned += value.getNumberOfResult();
        }

        if(requestCount > 100) {
            
            String[] keyBreak = key.toString().split("\\[\\{\\^\\}\\]");
            
            String[] data = new String[7];
            data[0] = keyBreak[0];
            data[1] = keyBreak[1];
            data[2] = keyBreak[2];
            data[3] = keyBreak[3];
//            data[4] = keyBreak[4];
            data[4] = "" + requestCount;
            data[5] = "" + numberOfTimesAdReturned;
            data[6] = "" + totalAdsReturned;
            
            context.write(NullWritable.get(), new Text(CsvUtils.arrayAsCsv(data, ",")));
        } else {
            context.getCounter(COUNTER.LESS_THAN_500_SEARCHES).increment(1);
        }
    }
}
