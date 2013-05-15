package com.xad.hadoop.cache;

import com.xad.hadoop.AggregationReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class CacheLatencyDriver {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: CacheLatencyDriver <input path> <output path>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(CacheLatencyDriver.class);
        conf.setJobName("Cache Aggregator");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(CacheLatencyMapper.class);
        conf.setReducerClass(AggregationReducer.class);
        conf.set("mapred.textoutputformat.separator", " ");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setOutputFormat(CacheNameMultipleOutputFormat.class);
        JobClient.runJob(conf);
    }

}
