package com.xad.hadoop.drivers;

import com.xad.hadoop.csvmappers.AdxVendorLogMapper;
import com.xad.hadoop.inputformat.CsvInputFormatNoSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 */
public class VendorLogDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: VendorLogDriver <input path> <output path> [jars]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.err.println("Usage: VendorLogDriver <in> <out> [jars]");
          System.exit(2);
        }

        Job job = new Job(conf, "VendorLog");

        for (int i = 2; i < otherArgs.length; i++) {
            ((JobConf)job.getConfiguration()).setJar(args[i]);
        }

        job.setMapperClass(AdxVendorLogMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CsvInputFormatNoSplit.class);
        MultipleOutputs.addNamedOutput(job, "vendorstats", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "ads", TextOutputFormat.class, NullWritable.class, Text.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
