package com.xad.hadoop.reports.keyword;

import com.xad.hadoop.core.RequestConstants;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 */
public class KeywordZipcodeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static enum COUNTER {
        ERROR_COUNTER,
        AD_COUNT,
        EMPTY_LINE,
        KEYWORD_NO_ZIP,
        KEYWORD_ZIP_COUNT
    }

    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            if (line == null || line.length() < 1) {
                context.getCounter(COUNTER.EMPTY_LINE).increment(1);
                return;
            }
            String[] searchData = CsvUtils.csvLineAsArray(line, ",");

            String keyword = searchData[RequestConstants.SEARCH_KEYWORDS_INDEX];
            if (keyword != null) {
                if (keyword.toLowerCase().startsWith("papa john")) {
                    String zipCode = searchData[RequestConstants.ZIPCODE_INDEX];
                    if (zipCode != null && zipCode.length() > 1) {
//                        context.write(new Text(keyword + "_" + zipCode), one);
                        context.write(new Text(zipCode), one);
                        context.getCounter(COUNTER.KEYWORD_ZIP_COUNT).increment(1);
                    } else {
                        context.getCounter(COUNTER.KEYWORD_NO_ZIP).increment(1);
                    }
                }
            }
        } catch (Exception e) {
            context.getCounter(COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line " + line);
        }
    }
}