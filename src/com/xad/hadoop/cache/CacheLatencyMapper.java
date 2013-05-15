package com.xad.hadoop.cache;

import com.xad.hadoop.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CacheLatencyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    static final IntWritable one = new IntWritable(1);

    public void map(LongWritable longWritable, Text text,
                    OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
       String line = text.toString();
        try {
            int index = line.indexOf("Took [");
            int getIndex = line.indexOf("to get");

            if (index != -1  && getIndex != -1) {
                // parse the cache stats

                String remString = line.substring(index + "Took [".length());

                int latency = Integer.parseInt(remString.substring(0, remString.indexOf("]")));

                String cacheString = remString.substring(remString.indexOf("from cache:[") + "from cache:[".length());
                String cacheName = cacheString.substring(0, cacheString.indexOf(']'));

                String result = cacheString.substring(cacheString.lastIndexOf("[") + 1, cacheString.lastIndexOf("]"));

//                if(latency <= 5) {
//                    outputCollector.collect(new Text(cacheName +"_5_"+ Utils.getMinuteSlice(line)), one);
//                } else if (latency > 5 && latency <= 10) {
//                    outputCollector.collect(new Text(cacheName +"_10_"+ Utils.getMinuteSlice(line)), one);
//                } else if (latency > 10 && latency <= 20) {
//                    outputCollector.collect(new Text(cacheName +"_20_"+ Utils.getMinuteSlice(line)), one);
//                } else if (latency > 20) {
//                    outputCollector.collect(new Text(cacheName +"_20+_"+ Utils.getMinuteSlice(line)), one);
//                }

                  outputCollector.collect(new Text(cacheName +"."+ Utils.getMinuteSlice(line)), one);

//                if("true".equals(result)) {
//                    outputCollector.collect(new Text(cacheName +"_Hits"), one);
//                } else {
//                    outputCollector.collect(new Text(cacheName +"_Miss"), one);
//                }

            }
        } catch (Exception e) {
            System.err.println("Error processing line " + line);
            e.printStackTrace();
        }
    }
}