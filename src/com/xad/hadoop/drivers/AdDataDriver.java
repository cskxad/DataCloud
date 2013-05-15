package com.xad.hadoop.drivers;

import com.xad.hadoop.inputformat.CsvInputFormatNoSplit;
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
 * Driver to merge the Ad Data
 */
public class AdDataDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: AdDataDriver <input path> <input path 2> <input path 3> <input path 4> <output path> [jars]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: AdDataDriver <input path> <input path 2> <input path 3> <input path 4> <output path> [jars]");
            System.exit(2);
        }

        Job job = new Job(conf, "Ad Data");

        for (int i = 5; i < otherArgs.length; i++) {
            ((JobConf)job.getConfiguration()).setJar(args[i]);
        }

        job.setMapperClass(AdInfoMapper.class);
        job.setReducerClass(AdInfoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CsvInputFormatNoSplit.class);
        job.setNumReduceTasks(12);
//        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
