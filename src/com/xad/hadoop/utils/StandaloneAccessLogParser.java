package com.xad.hadoop.utils;

import java.io.*;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

/**
 *
 * Standalone program to parse access logs
 *
 */
public class StandaloneAccessLogParser {

    protected ApacheAccessLog accessLogs = new ApacheAccessLog();

    /**
     * Reads all the files from the directory and parse the access logs
     *
     * @param directory Base directory from where logs are to be parsed
     * @throws Exception
     */
    public void parseFiles(String directory, String outputDirectory) throws Exception {

        File baseDirectory = new File(directory);

        if(!baseDirectory.isDirectory()) {
            System.err.println(String.format("%s is not a Directory. Please specify the base directory", directory));
            System.exit(-1);
        }

        // We are assuming that there shall be no nested directories
        File[] files = baseDirectory.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            long t1 = System.currentTimeMillis();
            processAccessLog(file, outputDirectory);
            long t2 = System.currentTimeMillis();
            System.out.println("Processed "+file + " in "+ (t2 - t1) + "ms");
        }
    }

    /**
     * Process an Access Log file and write the output to the file
     *
     * @param accessLogFile     Access Log file to be parsed
     * @throws IOException      If an error occurs while reading the file
     */
    public void processAccessLog(File accessLogFile, String outputDir) throws Exception {
        if(accessLogFile == null || accessLogFile.isHidden() || !accessLogFile.canRead()) {
            System.err.println("Could not read access log file : "+accessLogFile);
            return;
        }

        File outputDirectory = new File(outputDir);
        if(!outputDirectory.exists()) {
            System.err.println(String.format("Output directory %s doesn't exist, creating one", outputDir));
            outputDirectory.mkdirs();
        }

        File errorFile = new File(outputDir + "/" + accessLogFile.getName() +"_error.log");
        BufferedWriter errorLog = new BufferedWriter(new FileWriter(errorFile));
        System.out.println("File Name = "+accessLogFile.getName());
        StringTokenizer tokenizer = new StringTokenizer(accessLogFile.getName(), "_");
        tokenizer.nextToken();
        String virtualHost = tokenizer.nextToken();
        String hostName = tokenizer.nextToken();
        String instanceID = tokenizer.nextToken();

        File outfile = new File(outputDir + "/" + accessLogFile.getName() + "_out.csv");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outfile));

        File notificationfile = new File(outputDir + "/" + accessLogFile.getName() + "_notification.csv");
        BufferedWriter notificationWriter = new BufferedWriter(new FileWriter(notificationfile));

        BufferedReader reader = null;

        if(accessLogFile.getName().endsWith(".gz")) {
            // gosh its a gzipped file :(
            FileInputStream fin = new FileInputStream(accessLogFile);
            GZIPInputStream gzipStream = new GZIPInputStream(fin);
            InputStreamReader inputStreamReader = new InputStreamReader(gzipStream);
            reader = new BufferedReader(inputStreamReader);
        } else {
            reader = new BufferedReader(new FileReader(accessLogFile));
        }

        String readLine = null;

        while((readLine = reader.readLine()) != null) {
            try {
                ApacheAccessLog.AccessLog log = accessLogs.parse(readLine);
                if(log != null) {
                    log.setInstanceID(instanceID);
                    log.setServerIP(hostName);
                    log.setVirtualHost(virtualHost);
                    if(log.isNotificationRequest()) {
                        notificationWriter.write(log.toCsv());
                    } else {
                        bufferedWriter.write(log.toCsv());
                    }
                }
            } catch (Exception ex) {
//                System.err.println("Error for line : "+readLine);
                errorLog.write("Error for line : "+readLine);
                errorLog.newLine();
            }
        }

        bufferedWriter.close();
        notificationWriter.close();
        errorLog.close();
    }

    public static void main(String[] args) {
        StandaloneAccessLogParser parser = new StandaloneAccessLogParser();
        try {
            long t1 = System.currentTimeMillis();
            parser.parseFiles(args[0], args[1]);
            long t2 = System.currentTimeMillis();
            System.out.println("Total Time taken = "+(t2 - t1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}