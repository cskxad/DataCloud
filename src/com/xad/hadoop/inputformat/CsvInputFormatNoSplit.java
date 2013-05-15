package com.xad.hadoop.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

/**
 *
 */
public class CsvInputFormatNoSplit extends CsvInputFormat {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
