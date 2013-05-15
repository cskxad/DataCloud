package com.xad.hadoop.reports.publisher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 */
public class DMRequestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int requestCount = 0;
        for (IntWritable value : values) {
            requestCount += value.get();
        }
        context.write(key, new IntWritable(requestCount));
    }
}
