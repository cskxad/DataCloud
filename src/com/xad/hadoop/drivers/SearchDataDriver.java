package com.xad.hadoop.drivers;

import com.xad.hadoop.csvmappers.EdaSessionLogMapper;
import com.xad.hadoop.inputformat.CsvInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 */
public class SearchDataDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: com.xad.hadoop.drivers.SearchDataDriver <input path> <output path> [jars]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.err.println("Usage: SearchDataDriver <in> <out> [jars]");
          System.exit(2);
        }

        Job job = new Job(conf, "EdaSessionLog");

        for (int i = 2; i < otherArgs.length; i++) {
            ((JobConf)job.getConfiguration()).setJar(args[i]);

        }
        job.setMapperClass(EdaSessionLogMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(CsvInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        boolean jobStatus = job.waitForCompletion(true);
        Counter errorCounter = job.getCounters().findCounter(EdaSessionLogMapper.EDA_SESSION_LOG_COUNTER.ERROR_COUNTER);
        System.out.println("Error Counter = "+errorCounter.getValue());
        System.exit(jobStatus ? 0 : 1);
    }
}