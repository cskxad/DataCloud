package com.xad.hadoop.inputformat.offlinelogfile;

import com.xad.hadoop.inputformat.CsvLineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *
 */
public class OfflineFileRecordReader extends RecordReader<LongWritable, Text> {
    public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private CsvLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private Counter inputByteCounter;
    private CompressionCodec codec;
    private Decompressor decompressor;

    public OfflineFileRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        initialize(genericSplit, context);
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        FileSplit split = (FileSplit) genericSplit;
//        this.inputByteCounter = context.getCounter("FileInputFormatCounters", "BYTES_READ");

        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
        this.start = split.getStart();
        this.end = (this.start + split.getLength());
        Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        this.codec = this.compressionCodecs.getCodec(file);

        FileSystem fs = file.getFileSystem(job);
        this.fileIn = fs.open(file);
        if (isCompressedInput()) {
            this.decompressor = CodecPool.getDecompressor(this.codec);
            if ((this.codec instanceof SplittableCompressionCodec)) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec) this.codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);

                this.in = new CsvLineReader(cIn, job);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new CsvLineReader(this.codec.createInputStream(this.fileIn, this.decompressor), job);
                this.filePosition = this.fileIn;
            }
        } else {
            this.fileIn.seek(this.start);
            this.in = new CsvLineReader(this.fileIn, job);
            this.filePosition = this.fileIn;
        }

        if (this.start != 0L) {
            this.start += this.in.readLine(new Text(), 0, maxBytesToConsume(this.start));
        }
        this.pos = this.start;
    }

    private boolean isCompressedInput() {
        return this.codec != null;
    }

    private int maxBytesToConsume(long pos) {
        return isCompressedInput() ? 2147483647 : (int) Math.min(2147483647L, this.end - pos);
    }

    private long getFilePosition()
            throws IOException {
        long retVal;
        if ((isCompressedInput()) && (null != this.filePosition))
            retVal = this.filePosition.getPos();
        else {
            retVal = this.pos;
        }
        return retVal;
    }

    public boolean nextKeyValue() throws IOException {
        if (this.key == null) {
            this.key = new LongWritable();
        }
        this.key.set(this.pos);
        if (this.value == null) {
            this.value = new Text();
        }
        int newSize = 0;

        while (getFilePosition() <= this.end) {
            newSize = this.in.readLine(this.value, this.maxLineLength, Math.max(maxBytesToConsume(this.pos), this.maxLineLength));

            if (newSize == 0) {
                break;
            }
            this.pos += newSize;
//            this.inputByteCounter.increment(newSize);
            if (newSize < this.maxLineLength) {
                break;
            }
        }

        if (newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        }
        return true;
    }

    public LongWritable getCurrentKey() {
        return this.key;
    }

    public Text getCurrentValue() {
        return this.value;
    }

    public float getProgress()
            throws IOException {
        if (this.start == this.end) {
            return 0.0F;
        }
        return Math.min(1.0F, (float) (getFilePosition() - this.start) / (float) (this.end - this.start));
    }

    public synchronized void close() throws IOException {
        try {
            if (this.in != null)
                this.in.close();
        } finally {
            if (this.decompressor != null)
                CodecPool.returnDecompressor(this.decompressor);
        }
    }
}
