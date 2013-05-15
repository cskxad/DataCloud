package com.xad.hadoop.reports.aggregations;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class ToDRequestReducer extends Reducer<IntWritable, IntWritable, Text, NullWritable> {

    public static enum COUNTER {
        ERROR_COUNTER,
    }

    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int requestCount = 0;
        for (IntWritable value : values) {
            requestCount += value.get();
        }

        try {

            String hour = "";

            if(key.get() < 10) {
                hour = "0"+key.toString();
            } else {
                hour = key.toString();
            }

            String keyOut = "\"" + hour + "\",\"" + requestCount + "\"";
            context.write(new Text(keyOut), NullWritable.get());
        } catch (Exception e) {
            context.getCounter(COUNTER.ERROR_COUNTER).increment(1);
        }
    }
}
