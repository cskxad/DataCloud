package com.xad.hadoop.reports.demand;

import com.xad.hadoop.core.VendorStatsCounterWritable;
import com.xad.hadoop.utils.CsvUtils;
import com.xad.hadoop.utils.USCityStateHelper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Output format is
 * <p/>
 * City, state, Ad vendor, number of request, number of times ad returned, total number of Ads returned
 */
public class VendorDemandCityStateReducer extends Reducer<Text, VendorStatsCounterWritable, NullWritable, Text> {

    public static enum COUNTER {
        LESS_THAN_500_SEARCHES,
        ERROR_KEY
    }

    @Override
    protected void reduce(Text key, Iterable<VendorStatsCounterWritable> values, Context context) throws IOException, InterruptedException {
        int requestCount = 0;
        int numberOfTimesAdReturned = 0;
        int totalAdsReturned = 0;
        for (VendorStatsCounterWritable value : values) {
            requestCount++;

            if (value.getNumberOfResult() > 0) {
                numberOfTimesAdReturned++;
            }
            totalAdsReturned += value.getNumberOfResult();
        }

        String[] keyBreak = key.toString().split("\\[\\{\\^\\}\\]");
        if (keyBreak != null && keyBreak.length == 3) {
            String[] data = new String[6];
            data[0] = keyBreak[1];
            data[1] = keyBreak[2];
            data[2] = keyBreak[0];
            data[3] = "" + requestCount;
            data[4] = "" + numberOfTimesAdReturned;
            data[5] = "" + totalAdsReturned;

            context.write(NullWritable.get(), new Text(CsvUtils.arrayAsCsv(data, ",")));
        } else {
            context.getCounter(COUNTER.ERROR_KEY).increment(1);
            System.err.println("Error for key "+key.toString());
        }
    }
}
