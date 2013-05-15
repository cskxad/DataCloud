package com.xad.hadoop.reports.advendor;

import com.xad.hadoop.core.VendorStatsCounterWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class VendorFillRateReducer extends Reducer<Text, VendorStatsCounterWritable, NullWritable, Text> {

    public static enum COUNTER {
        OUTPUT_COUNT
    }

    @Override
    protected void reduce(Text key, Iterable<VendorStatsCounterWritable> values, Context context) throws IOException, InterruptedException {
        int requestCount = 0;
        int minResponseTime = 0;
        int maxResponseTime = 0;
        int minConnectionTime = 0;
        int maxConnectionTime = 0;
        long totalConnectionTime = 0L;
        long totalResponseTime = 0L;
        int numberOfTimesAdReturned = 0;
        for (VendorStatsCounterWritable value : values) {
            if (value.getResponseTime() < minResponseTime) {
                minResponseTime = value.getResponseTime();
            }

            if(value.getResponseTime() > maxResponseTime) {
                maxResponseTime = value.getResponseTime();
            }

            if (value.getConnectionTime() < minConnectionTime) {
                minConnectionTime = value.getConnectionTime();
            }

            if(value.getConnectionTime() > maxConnectionTime) {
                maxConnectionTime = value.getConnectionTime();
            }

            requestCount++;
            
            if(value.getNumberOfResult() > 0) {
                numberOfTimesAdReturned++;    
            }
            totalConnectionTime += value.getConnectionTime();
            totalResponseTime += value.getResponseTime();
        }
        
        StringBuilder stringBuilder = new StringBuilder(128);
        
        String keyStr = key.toString();
        
        int underscoreIndex = keyStr.indexOf("_");
        
        String vendor = keyStr.substring(0, underscoreIndex);
        String hourOfDay = keyStr.substring(underscoreIndex + 1, keyStr.length());
        
        stringBuilder.append('"').append(vendor).append("\",");
        stringBuilder.append('"').append(hourOfDay).append("\",");
        stringBuilder.append('"').append(requestCount).append("\",");
        stringBuilder.append('"').append(numberOfTimesAdReturned).append("\",");

        stringBuilder.append('"').append(minConnectionTime).append("\",");
        stringBuilder.append('"').append(maxConnectionTime).append("\",");
        stringBuilder.append('"').append(totalConnectionTime/requestCount).append("\",");

        stringBuilder.append('"').append(minResponseTime).append("\",");
        stringBuilder.append('"').append(maxResponseTime).append("\",");
        stringBuilder.append('"').append(totalResponseTime/requestCount).append('"');

        context.getCounter(COUNTER.OUTPUT_COUNT).increment(1);
        context.write(NullWritable.get(), new Text(stringBuilder.toString()));
    }
}
