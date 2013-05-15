package com.xad.hadoop.accesslogs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class AccessLogAgeReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    public static enum COUNTER {
        ERROR_COUNT
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for(IntWritable value: values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }

}
