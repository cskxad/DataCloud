package com.xad.hadoop.inputformat.offlinelogfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class OfflineFileReader {

    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private InputStream in;
    private byte[] buffer;
    private int bufferLength = 0;
    private int bufferPosn = 0;

    enum PARSE_STATE { STATE_NA, STATE_1, STATE_2, STATE_3, STATE_4, STATE_5 }

    /**
     * Create a csv line reader that reads from the given stream using the
     * default buffer-size (64k).
     *
     * @param in The input stream
     * @throws java.io.IOException
     */
    public OfflineFileReader(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a csv line reader that reads from the given stream using the
     * given buffer-size.
     *
     * @param in         The input stream
     * @param bufferSize Size of the read buffer
     * @throws java.io.IOException
     */
    public OfflineFileReader(InputStream in, int bufferSize) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>.
     *
     * @param in   input stream
     * @param conf configuration
     * @throws java.io.IOException
     */
    public OfflineFileReader(InputStream in, Configuration conf)
            throws IOException {
        this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    }

    /**
     * Fill the buffer with more data.
     *
     * @return was there more data?
     * @throws IOException
     */
    boolean backfill()
            throws IOException {
        bufferPosn = 0;
        bufferLength = in.read(buffer);
        return bufferLength > 0;
    }

    /**
     * Close the underlying stream.
     *
     * @throws IOException
     */
    public void close()
            throws IOException {
        in.close();
    }

    /**
     * Read from the InputStream into the given Text.
     *
     * @param txt               the object to store the given line
     * @param maxLineLength     the maximum number of bytes to store into txt.
     * @param maxBytesToConsume the maximum number of bytes to consume in this
     *                          call.
     * @return the number of bytes read including the newline
     * @throws IOException if the underlying stream throws
     */
    public int readLine(Text txt, int maxLineLength, int maxBytesToConsume)
            throws IOException {
        txt.clear();
        boolean hadFinalNewline = false;
        boolean hadFinalReturn = false;
        boolean hitEndOfFile = false;
        int startPosn = bufferPosn;
        long bytesConsumed = 0;
        boolean inQuote = false;
        boolean isLastCharEscapeChar = false;
        PARSE_STATE currentState = PARSE_STATE.STATE_NA;

        outerLoop:
        while (true) {
            if (bufferPosn >= bufferLength) {
                if (!backfill()) {
                    hitEndOfFile = true;
                    break;
                }
            }

            startPosn = bufferPosn;

            // [{^}]

            for (; bufferPosn < bufferLength; ++bufferPosn) {

//                char ch = Character.valueOf((char)buffer[bufferPosn]);
                switch (buffer[bufferPosn]) {

                    case '[':
                        if(currentState == PARSE_STATE.STATE_NA) {
                            currentState = PARSE_STATE.STATE_1;
                        }

                        break;

                    case '{':

                        if(currentState == PARSE_STATE.STATE_1) {
                            currentState = PARSE_STATE.STATE_2;
                        }

                        break;

                    case '^':
                        if(currentState == PARSE_STATE.STATE_2) {
                            currentState = PARSE_STATE.STATE_3;
                        }

                        break;

                    case '}':
                        if(currentState == PARSE_STATE.STATE_3) {
                            currentState = PARSE_STATE.STATE_4;
                        }
                        break;

                    case ']':
                        if(currentState == PARSE_STATE.STATE_4) {
                            currentState = PARSE_STATE.STATE_5;
                        }
                        break;

                    case '\n':
                        // NOOP
                        break;

                    case '\r':
                        // NOOP
                        break;

                   default:
                        isLastCharEscapeChar = false;
                        if (!inQuote && hadFinalReturn) {
                            break outerLoop;
                        }
                }
            }
            bytesConsumed += bufferPosn - startPosn;
            int length = bufferPosn - startPosn - (hadFinalReturn ? 1 : 0);
            length = (int) Math.min(length, maxLineLength - txt.getLength());

            if (length >= 0)
                txt.append(buffer, startPosn, length);

            if (bytesConsumed >= maxBytesToConsume)
                return (int) Math.min(bytesConsumed, (long) Integer.MAX_VALUE);
        }

        int newlineLength = (hadFinalNewline ? 1 : 0) + (hadFinalReturn ? 1 : 0);

        if (!hitEndOfFile) {
            bytesConsumed += bufferPosn - startPosn;
            int length = bufferPosn - startPosn - newlineLength;
            length = (int) Math.min(length, maxLineLength - txt.getLength());

            if (length > 0)
                txt.append(buffer, startPosn, length);
        }
        return (int) Math.min(bytesConsumed, (long) Integer.MAX_VALUE);
    }

    /**
     * Read from the InputStream into the given Text.
     *
     * @param txt           the object to store the given line
     * @param maxLineLength the maximum number of bytes to store into txt.
     * @return the number of bytes read including the newline
     * @throws IOException if the underlying txteam throws
     */
    public int readLine(Text txt, int maxLineLength)
            throws IOException {
        return readLine(txt, maxLineLength, Integer.MAX_VALUE);
    }

    /**
     * Read from the InputStream into the given Text.
     *
     * @param txt the object to store the given line
     * @return the number of bytes read including the newline
     * @throws IOException if the underlying stream throws
     */
    public int readLine(Text txt)
            throws IOException {
        return readLine(txt, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

}
