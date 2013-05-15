package com.xad.hadoop.accesslogs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 */
public class AccessLogDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AccessLogDriver <input path> <output path> [jars]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: AccessLogDriver <in> <out> [jars]");
            System.exit(2);
        }

        Job job = new Job(conf, "AccessLogDriver");

        for (int i = 2; i < otherArgs.length; i++) {
            ((JobConf)job.getConfiguration()).setJar(args[i]);
        }

        job.setMapperClass(AccessLogMapper.class);
        job.setNumReduceTasks(6);
        job.setReducerClass(AccessLogAMReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setCompressOutput(job, true);
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        String prefixPath = "input/access_logs/mar/";
        String sufficPath = "032012";
        String date = "";
        for (int i = 1; i <= 31; i++) {
            if(i < 10) {
                date = "0"+i;
            } else {
                date = ""+i;
            }

            FileInputFormat.addInputPath(job, new Path(prefixPath + date + sufficPath));
        }

//        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
