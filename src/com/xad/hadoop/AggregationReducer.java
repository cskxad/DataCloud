package com.xad.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class AggregationReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, Text> outputCollector,
                       Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }

        // try something new
        String cacheName;
        String val = key.toString();
        int index = val.indexOf('.');

        if(index == -1) {
            cacheName = val;
        }
        cacheName = val.substring(0, index);

        String time = key.toString().substring(key.toString().lastIndexOf(".") + 1, key.toString().length());

        long iTime = Integer.parseInt(time);

        // prepare string
        String value = (iTime * 60) + " " + sum + " cacheName="+cacheName;

        outputCollector.collect(new Text("cache.getStats."+cacheName), new Text(value));

//        outputCollector.collect(key, new IntWritable(sum));
    }
}
