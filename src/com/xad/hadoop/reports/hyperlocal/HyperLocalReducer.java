package com.xad.hadoop.reports.hyperlocal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 *
 */
public class HyperLocalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    MultipleOutputs mo;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mo = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int requestCount = 0;
        for (IntWritable value : values) {
            requestCount += value.get();
        }


        String keyStr = key.toString();

        String publisher =keyStr.substring(0, keyStr.indexOf("_"));
        String adName = keyStr.substring(keyStr.indexOf("_") + 1, keyStr.length());
        if("ve-hyperlocal".equals(publisher)) {
            mo.write("vehyperlocal", new Text(adName), requestCount);    
        } else {
            mo.write("vehyperlocalnational", new Text(adName), requestCount);
        }
        context.write(key, new IntWritable(requestCount));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mo.close();
    }

}
