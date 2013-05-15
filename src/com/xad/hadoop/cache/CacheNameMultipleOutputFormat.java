package com.xad.hadoop.cache;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class CacheNameMultipleOutputFormat extends MultipleTextOutputFormat<Text, Text> {

    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        String val = key.toString();
        int index = val.lastIndexOf('.');

        if(index == -1) {
            return val;
        }
        return val.substring(index + 1);
    }
}