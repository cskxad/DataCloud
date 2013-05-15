package com.xad.hadoop.reports.aggregations;

import com.xad.hadoop.utils.CsvUtils;
import com.xad.hadoop.utils.DateUtils;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 *
 */
public class WoDReportGenerator {

    public void mergeData(String monthFolder) throws Exception {
        // read files
        // process files

        File outputFile = new File(monthFolder+"WoD.csv");
        System.out.println("Writing output to File : "+outputFile.getAbsolutePath());
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        File monthFolderBase = new File(monthFolder);
        if (!monthFolderBase.isDirectory()) {
            System.err.println("Base Folder is not a Directory. Exiting..");
            System.exit(-1);
        }

        long[] dayOfWeekAggregation = new long[7];

        File[] files = monthFolderBase.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file.isFile()) {
                FileInputStream fin = new FileInputStream(file);
                GZIPInputStream gzipStream = new GZIPInputStream(fin);
                InputStreamReader inputStreamReader = new InputStreamReader(gzipStream);
                BufferedReader reader = new BufferedReader(inputStreamReader);
                String[] counts = new String[25];

                String line = null;
                
                int dayTotal = 0;

                while((line = reader.readLine()) != null) {
                    String[] data = CsvUtils.csvLineAsArray(line, ",");
                    int hour = Integer.parseInt(data[0]);
                    try {
                        int searchCount = Integer.parseInt(data[1]);
                        dayTotal += searchCount;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                // Add Date to start
                String date = file.getName().substring(file.getName().indexOf("_") + 1, file.getName().indexOf(".csv.gz"));

                int dayOfWeek = DateUtils.getDayOfWeek(date, "yyyy-MM-dd");

                dayOfWeekAggregation[dayOfWeek - 1] += dayTotal;
                System.out.println("Date :"+date + ":"+dayOfWeek+":"+dayOfWeekAggregation[dayOfWeek - 1]);
                // dump the content

            } else {
                System.err.println("Argg... Should have been a file.");
            }
        }
        for (int j = 0; j < dayOfWeekAggregation.length; j++) {
            writer.write(""+dayOfWeekAggregation[j]);
            writer.newLine();
        }
        writer.close();
    }


    public static void main(String[] args) throws Exception {
        WoDReportGenerator merger = new WoDReportGenerator();
        merger.mergeData(args[0]);
    }

}
