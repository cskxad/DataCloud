package com.xad.hadoop.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * Utility class to find issues in Raw offline Files
 */
public class RawFileVerifier {

    public static void main(String[] args) throws Exception {
        FileInputStream fin = new FileInputStream(args[0]);
        GZIPInputStream gzipStream = new GZIPInputStream(fin);
        InputStreamReader inputStreamReader = new InputStreamReader(gzipStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);

        String readLine = null;
        long count = 0;

        while((readLine = reader.readLine()) != null) {
            count++;

            String[] data = AdUtils.parseFileRecord(readLine);

            if(data.length != 50) {
                System.out.println("Less Columns = "+readLine);
            }
            
//            if(count > 48690 && count < 48750 ) {
//                System.out.println(readLine);
//            }
        }

        System.out.println("Total Lines = "+count);
    }

}
