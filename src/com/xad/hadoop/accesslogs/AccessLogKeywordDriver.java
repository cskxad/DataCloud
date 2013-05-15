package com.xad.hadoop.accesslogs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

/**
 *
 */
public class AccessLogKeywordDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AccessLogKeywordDriver <input path> <output path> [jars]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: AccessLogKeywordDriver <in> <out> [jars]");
            System.exit(2);
        }

        Job job = new Job(conf, "AccessLogKeywordDriver");

        for (int i = 2; i < otherArgs.length; i++) {
            ((JobConf)job.getConfiguration()).setJar(args[i]);
        }

        job.setMapperClass(AccessLogKeywordMapper.class);
        job.setNumReduceTasks(6);
        job.setReducerClass(AccessLogAMReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setCompressOutput(job, true);
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        DistributedCache.addCacheFile(new URI("input/cache/TravelCategoryKeywords.txt#TravelCategoryKeywords.txt"), job.getConfiguration());


        String prefixPath = "input/access_logs/feb/";
        String sufficPath = "022012";
        String date = "";
        for (int i = 1; i <= 29; i++) {
            if(i < 10) {
                date = "0"+i;
            } else {
                date = ""+i;
            }

            FileInputFormat.addInputPath(job, new Path(prefixPath + date + sufficPath));
        }

//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
