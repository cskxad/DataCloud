package com.xad.hadoop.reports.hyperlocal;

import com.xad.hadoop.csvmappers.AdxImpressionTrackingMapper;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 *
 */
public class HyperLocalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum HyperLocalMapper_COUNTER {
        ERROR_COUNTER,
        AD_COUNT,
        EMPTY_LINE,
        HYPERLOCAL_COUNT,
        HYPERLOCAL_NATIONAL_COUNT
    }

    MultipleOutputs mo;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mo = new MultipleOutputs(context);
    }


    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            if (line == null || line.length() < 1) {
                context.getCounter(HyperLocalMapper_COUNTER.EMPTY_LINE).increment(1);
                return;
            }
            String[] adData = CsvUtils.csvLineAsArray(line, ",");

            String publisherId = adData[1];
            if (publisherId != null && publisherId.equalsIgnoreCase("ve-hyperlocal")) {
                String adName = adData[AdxImpressionTrackingMapper.Ad_Name_Idx];
                mo.write("vehyperlocal", nullWritable, new Text(adName));
//                context.write(new Text("ve-hyperlocal_"+adName), one);
                context.getCounter(HyperLocalMapper_COUNTER.HYPERLOCAL_COUNT).increment(1);
            } else if (publisherId != null && publisherId.equalsIgnoreCase("ve-hyperlocal-national")) {
                String adName = adData[AdxImpressionTrackingMapper.Ad_Name_Idx];
                mo.write("vehyperlocalnational", nullWritable, new Text(adName));
//                context.write(new Text("ve-hyperlocal-national_"+adName), one);
                context.getCounter(HyperLocalMapper_COUNTER.HYPERLOCAL_NATIONAL_COUNT).increment(1);
            }

            context.getCounter(HyperLocalMapper_COUNTER.AD_COUNT).increment(1);
        } catch (Exception e) {
            context.getCounter(HyperLocalMapper_COUNTER.ERROR_COUNTER).increment(1);
            e.printStackTrace();
            System.out.println("Status :: Failed for line " + line);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mo.close();
    }
}
