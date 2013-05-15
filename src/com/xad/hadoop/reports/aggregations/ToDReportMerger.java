package com.xad.hadoop.reports.aggregations;

import com.xad.hadoop.utils.CsvUtils;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Merges Daily ToD reports to generate Monthly reports
 */
public class ToDReportMerger {

    public void mergeData(String monthFolder) throws Exception {
        // read files
        // process files
        
        File outputFile = new File(monthFolder+"output.csv");
        System.out.println("Writing output to File : "+outputFile.getAbsolutePath());
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        File monthFolderBase = new File(monthFolder);
        if (!monthFolderBase.isDirectory()) {
            System.err.println("Base Folder is not a Directory. Exiting..");
            System.exit(-1);
        }

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
                
                while((line = reader.readLine()) != null) {
                    String[] data = CsvUtils.csvLineAsArray(line, ",");
                    int hour = Integer.parseInt(data[0]);
//                    long searches = Integer.parseInt(data[1]);
                    counts[hour] = data[1];
                }
                // Add Date to start
                String date = file.getName().substring(file.getName().indexOf("_") + 1, file.getName().indexOf(".csv.gz"));
                counts[0] = date;

                // dump the content
                writer.write(CsvUtils.arrayAsCsv(counts, ","));
                System.out.println(CsvUtils.arrayAsCsv(counts, ","));
                writer.newLine();
            } else {
                System.err.println("Argg... Should have been a file.");
            }
        }
        writer.close();
    }


    public static void main(String[] args) throws Exception {
        ToDReportMerger merger = new ToDReportMerger();
        merger.mergeData(args[0]);
    }

}
