package com.xad.hadoop.reports.aggregations;

import com.xad.hadoop.core.RequestConstants;
import com.xad.hadoop.utils.CsvUtils;
import com.xad.hadoop.utils.DateUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * The Mapper task maps the Searches for a time of day.
 * This mapper used in conjugation with its reducer shall produce a report to collect
 * stats to show distribution of Requests on Time of Day
 */
public class ToDRequestMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    public static enum COUNTER {
        ERROR_COUNTER,
        SEARCH_COUNT,
        EMPTY_LINE
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.err.println(context.getTaskAttemptID() +  " - "+ ((FileSplit)context.getInputSplit()).getPath());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            if(line == null || line.length() < 1) {
                context.getCounter(COUNTER.EMPTY_LINE).increment(1);
                return;
            }
            String[] searchData = CsvUtils.csvLineAsArray(line, ",");

            int requestType = Integer.parseInt(searchData[RequestConstants.REQUEST_TYPE_INDEX]);

            if(requestType == 2 || requestType == 3 || requestType == 22) {
                int hourOfDay = DateUtils.getHourOfDayFromDate(searchData[RequestConstants.TIMESTAMP_INDEX]);
                context.write(new IntWritable(hourOfDay), one);
                context.getCounter(COUNTER.SEARCH_COUNT).increment(1);
            }
        } catch (Exception e) {
            context.getCounter(COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line "+line);
        }
    }
}