package com.xad.hadoop.reports.aggregations;

import com.xad.hadoop.core.RequestConstants;
import com.xad.hadoop.utils.CsvUtils;
import com.xad.hadoop.utils.DateUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper used to aggregate data on basis of City State. Following aggregations would be needed
 * 1. Daily aggregation
 * 2. Hourly break-up to analyse the trend
 */
public class TopCityStateRequestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    public static enum COUNTER {
        ERROR_COUNTER,
        SEARCH_COUNT,
        EMPTY_LINE
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

            int hourOfDay = DateUtils.getHourOfDayFromDate(searchData[RequestConstants.TIMESTAMP_INDEX]);
            context.write(new Text(""+hourOfDay), one);
            context.getCounter(COUNTER.SEARCH_COUNT).increment(1);
        } catch (Exception e) {
            context.getCounter(COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line "+line);
        }
    }
}