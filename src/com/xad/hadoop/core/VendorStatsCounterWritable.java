package com.xad.hadoop.core;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds counters related to Vendor Stats
 */
public class VendorStatsCounterWritable implements Writable {

    int requestCount = -1;
    int numberOfResult = -1;
    int responseTime = -1;
    int connectionTime = -1;

    public VendorStatsCounterWritable() {
    }

    public VendorStatsCounterWritable(int requestCount, int numberOfResult, int responseTime, int connectionTime) {
        this.requestCount = requestCount;
        this.numberOfResult = numberOfResult;
        this.responseTime = responseTime;
        this.connectionTime = connectionTime;
    }

    public VendorStatsCounterWritable(int requestCount, int numberOfResult) {
        this.requestCount = requestCount;
        this.numberOfResult = numberOfResult;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(requestCount);
        dataOutput.writeInt(numberOfResult);
        dataOutput.writeInt(responseTime);
        dataOutput.writeInt(connectionTime);
    }

    public void readFields(DataInput dataInput) throws IOException {
        requestCount = dataInput.readInt();
        numberOfResult = dataInput.readInt();
        responseTime = dataInput.readInt();
        connectionTime = dataInput.readInt();
    }

    public int getRequestCount() {
        return requestCount;
    }

    public int getNumberOfResult() {
        return numberOfResult;
    }

    public int getResponseTime() {
        return responseTime;
    }

    public int getConnectionTime() {
        return connectionTime;
    }
}
