package com.xad.hadoop.reports.keyword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class KeywordReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    public static enum COUNTER {
        ERROR_COUNTER,
    }

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int requestCount = 0;
        for (IntWritable value : values) {
            requestCount += value.get();
        }

        try {
//            String keyStr = key.toString();
//            int index = keyStr.indexOf("_");
//            String keyword = keyStr.substring(0, index);
//            String zipcode = keyStr.substring(index + 1, keyStr.length());
//            String keyOut = "\"" + keyword + "\",\"" + zipcode + "\",\"" + requestCount + "\"";
            String keyOut = "\"" + key.toString() + "\",\"" + requestCount + "\"";
            context.write(new Text(keyOut), NullWritable.get());
        } catch (Exception e) {
            context.getCounter(COUNTER.ERROR_COUNTER).increment(1);
        }
    }
}