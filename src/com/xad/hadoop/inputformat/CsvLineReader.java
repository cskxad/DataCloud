package com.xad.hadoop.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class CsvLineReader {

    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private InputStream in;
    private byte[] buffer;
    private int bufferLength = 0;
    private int bufferPosn = 0;

    /**
     * Create a csv line reader that reads from the given stream using the
     * default buffer-size (64k).
     *
     * @param in The input stream
     * @throws IOException
     */
    public CsvLineReader(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a csv line reader that reads from the given stream using the
     * given buffer-size.
     *
     * @param in         The input stream
     * @param bufferSize Size of the read buffer
     * @throws IOException
     */
    public CsvLineReader(InputStream in, int bufferSize) {
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
     * @throws IOException
     */
    public CsvLineReader(InputStream in, Configuration conf)
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
//        System.out.println("readLine: entry");
        txt.clear();
        boolean hadFinalNewline = false;
        boolean hadFinalReturn = false;
        boolean hitEndOfFile = false;
        int startPosn = bufferPosn;
        long bytesConsumed = 0;
        boolean inQuote = false;
        boolean isLastCharEscapeChar = false;

        outerLoop:
        while (true) {
            if (bufferPosn >= bufferLength) {
                if (!backfill()) {
                    hitEndOfFile = true;
                    break;
                }
            }

            startPosn = bufferPosn;

//            System.out.println("Entering loop");
            for (; bufferPosn < bufferLength; ++bufferPosn) {

                char ch = Character.valueOf((char)buffer[bufferPosn]);
                switch (buffer[bufferPosn]) {

                    case '\\':
//                        System.out.println("Escape character");
                        if(isLastCharEscapeChar) {
                            isLastCharEscapeChar = false;
                        } else {
                            isLastCharEscapeChar = true;
                        }
                        break;

                    case '"':
//                        System.out.println("double quote char");
                        if (!inQuote && hadFinalReturn) {
                            break outerLoop;
                        }

                        if (!isLastCharEscapeChar) {
                                inQuote = !inQuote;
//                            System.out.println("inQuote = !inQuote;");
                        } else {
//                            System.out.println("string is escaped, so skip");
                        }
                        isLastCharEscapeChar = false;
//                        System.out.println("1-> Quote = "+inQuote);
                        break;

                    case '\n':
                        isLastCharEscapeChar = false;
//                        System.out.println("-->2 Quote = "+inQuote);
                        if (!inQuote) {
                            hadFinalNewline = true;
                            bufferPosn += 1;
//                            System.out.println("!inQuote - Breaking outer loop");
                            break outerLoop;
                        }
                        break;

                    case '\r':
                        isLastCharEscapeChar = false;
//                        System.out.println("--> 3 Quote = "+inQuote);
                        if (!inQuote) {
                            if (hadFinalReturn) {
                                // leave this \r in the stream, so we'll get it next time
                                break outerLoop;
                            }
                            hadFinalReturn = true;
                        }
                        break;

                    default:
                        isLastCharEscapeChar = false;
//                        System.out.println("-->4 ch = "+ch);
                        if (!inQuote && hadFinalReturn) {
//                            System.out.println("default case");
                            break outerLoop;
                        }
                }
            }
//            System.out.println("Out of Loop & isLastCharEscapeChar = "+isLastCharEscapeChar);

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
//        System.out.println("Returing txt = "+txt.toString());
//        System.out.println("readLine: exit");
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