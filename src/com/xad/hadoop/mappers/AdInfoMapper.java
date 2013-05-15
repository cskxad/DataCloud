package com.xad.hadoop.mappers;

import com.xad.hadoop.csvmappers.AdxImpressionTrackingMapper;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper task to collate Ad Info
 *
 * Mapper task just emits ads per search request id
 */
public class AdInfoMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static enum AD_INFO_COUNTERS {
        AD_COUNT,
        ERROR_COUNT,
        MISSING_SEARCH_ID
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.out.println( context.getTaskAttemptID() +  " - "+ ((FileSplit)context.getInputSplit()).getPath());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

//        String[] adInfo = CsvUtils.csvLineAsArray(line, ",");

        if(line == null || line.length() < 1) {
            System.out.println("empty line");
            return;
        }

        try {

            String[] adInfo = CsvUtils.csvLineAsArray(line, ",");
            
            // lets parse the Request ID
//            int nextIndex = line.indexOf('"', 2);
            
//            String searchId = line.substring(1, nextIndex);
            String searchId = adInfo[0];

            if(searchId == null || searchId.length() < 1) {
                context.getCounter(AD_INFO_COUNTERS.MISSING_SEARCH_ID).increment(1);
                System.err.println("Search Id is missing for line = "+line);
                return;
            }
            context.getCounter(AD_INFO_COUNTERS.AD_COUNT).increment(1);
            context.write(new Text(searchId), value);
            context.progress();
            context.setStatus("Processed Ad for Search ID "+searchId);
        } catch (Exception e) {
            context.getCounter(AD_INFO_COUNTERS.ERROR_COUNT).increment(1);
            e.printStackTrace();
            System.err.println("Status :: Failed "+e.getMessage() + "--- "+line);
        }
    }
}