package com.xad.hadoop.legacy.drivers;

import com.xad.hadoop.inputformat.CsvInputFormatNoSplit;
import com.xad.hadoop.legacy.AdInfoLegacyReducer;
import com.xad.hadoop.mappers.AdInfoMapper;
import com.xad.hadoop.reducers.AdInfoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Driver to get Legacy data
 */
    public class AdDataLegacyDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AdDataDriver <input path> <input path 2> <output path> [jars]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: AdDataDriver <input path> <input path 2> <output path> [jars]");
            System.exit(2);
        }

        Job job = new Job(conf, "Ad Data");

        for (int i = 3; i < otherArgs.length; i++) {
            ((JobConf)job.getConfiguration()).setJar(args[i]);
        }

        job.setMapperClass(AdInfoMapper.class);
        job.setReducerClass(AdInfoLegacyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CsvInputFormatNoSplit.class);
        job.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
