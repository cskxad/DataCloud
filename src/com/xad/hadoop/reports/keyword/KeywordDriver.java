package com.xad.hadoop.reports.keyword;

import com.xad.hadoop.inputformat.CsvInputFormatNoSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 */
public class KeywordDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: KeywordDriver <input path> <output path> [jars]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: KeywordDriver <in> <out> [jars]");
            System.exit(2);
        }

        Job job = new Job(conf, "KeywordDriver");

        for (int i = 2; i < otherArgs.length; i++) {
            ((JobConf)job.getConfiguration()).setJar(args[i]);
        }

        job.setMapperClass(KeywordZipcodeMapper.class);
        job.setNumReduceTasks(12);
        job.setReducerClass(KeywordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CsvInputFormatNoSplit.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        String prefixPath = "input/2012-01-";
        String sufficPath = "/Search";
        String date = "";
        for (int i = 1; i <= 31; i++) {
            if(i < 10) {
                date = "0"+i;
            } else {
                date = ""+i;
            }

            FileInputFormat.addInputPath(job, new Path(prefixPath + date + sufficPath));
        }
//        FileInputFormat.addInputPath(job, new Path("input/2012-01-01/Search"));
//        FileInputFormat.addInputPath(job, new Path("input/2012-01-02/Search"));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
