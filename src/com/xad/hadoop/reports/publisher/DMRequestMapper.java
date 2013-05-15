package com.xad.hadoop.reports.publisher;

import com.xad.hadoop.core.RequestConstants;
import com.xad.hadoop.utils.CsvUtils;
import com.xad.hadoop.utils.DateUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper to collate Reporting data
 *
 * Publisher ID == Src
 * App ID = Publisher
 *
 */
public class DMRequestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum DMRequestMapper_COUNTER {
        ERROR_COUNTER,
        SEARCH_COUNT,
        EMPTY_LINE
    }

    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            if(line == null || line.length() < 1) {
                context.getCounter(DMRequestMapper_COUNTER.EMPTY_LINE).increment(1);
                return;
            }
            String[] searchData = CsvUtils.csvLineAsArray(line, ",");
            
            String appId = searchData[RequestConstants.APPID_INDEX];
            String requestType = searchData[RequestConstants.REQUEST_TYPE_INDEX];
            int hourOfDay = DateUtils.getHourOfDayFromDate(searchData[RequestConstants.TIMESTAMP_INDEX]);
//            context.write(new Text(appId + "_" + requestType+ "_"+hourOfDay), one);
            context.write(new Text(appId + "\t" + requestType+ "\t"+hourOfDay), one);
            context.getCounter(DMRequestMapper_COUNTER.SEARCH_COUNT).increment(1);
        } catch (Exception e) {
            context.getCounter(DMRequestMapper_COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line "+line);
        }
    }
}
