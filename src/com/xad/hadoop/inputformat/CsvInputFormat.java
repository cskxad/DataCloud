package com.xad.hadoop.inputformat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 *
 */
public class CsvInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
       try {
            CsvRecordReader reader = new CsvRecordReader(split, context);
            return reader;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}