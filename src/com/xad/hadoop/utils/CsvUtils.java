package com.xad.hadoop.utils;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;

/**
 * CSV Utilities
 */
public class CsvUtils {

    /**
     * Converts an array of String into a CSV format string
     *
     * @param values
     * @param separator
     * @return
     */
    public static String arrayAsCsv(String[] values, String separator) {
        if(values == null || values.length < 1) {
            return null;
        }

        StringWriter writer = new StringWriter();
        CSVWriter csvWriter = new CSVWriter(writer, ',', '"', '\\');
        csvWriter.writeNext(values);
        return writer.toString().trim();
    }

    /**
     * Returns a CSV line as an array of Strings
     * @param csvLine
     * @param separator
     * @return
     */
    public static String[] csvLineAsArray(String csvLine, String separator) {
        if(csvLine == null) {
            return null;
        }
        CSVParser parser = new CSVParser(',', '"', '\\', false);
        try {
            return parser.parseLine(csvLine);
        } catch (IOException e) {
            System.out.println(e.getMessage()+ " for "+csvLine);
            return null;
        }
    }

    public static void main(String[] args) {
//        String line = "\"lVx7jt7Y\",\"2012-01-24 00:00:00.221\",\"lVx7jt7Y-1\",\"Ball & McGraw PC\",\"The \"\"on the ball\"\" accountants.\",\"\",\"351 W Hatcher Rd\",\"Phoenix\",\"AZ\",\"85021\",\"33.5712975\",\"-112.0793725\",\"8663297803\",\"-1\",\"accountants\",\"\",\"4.0\",\"\",\"monday 8:00 a.m. 5:00 p.m., tuesday 8:00 a.m. 5:00 p.m., wednesday 8:00 a.m. 5:00 p.m., thursday 8:00 a.m. 5:00 p.m., friday 8:00 a.m. 5:00 p.m., saturday closed, sunday closed\",\"0.65\",\"0.0\",\"http://www.ball-mcgraw.com\",\"aHR0cDovL3d3dy5kZXhrbm93cy5jb20vcmQvaW5kZXguYXNwP2RraWQ9NTU0NjI5JmFjdD0yJnBkdD1jcGEmcGFydG5lcj14YWRkaXNwJmRrY2F0PWM2MWRhMzcwLTBkNjQtNDA0NC1hYWUxLWY5N2U5NTI2ZDU4ZCZka2dlbz1jLXBob2VuaXgtYXomYWRjYXRpZD1DNjFEQTM3MC0wRDY0LTQwNDQtQUFFMS1GOTdFOTUyNkQ1OEQmYWRnZW9pZD1jLXBob2VuaXgtYXomZGtxPWFjY291bnRhbnRzJmRrbG9jPXBob2VuaXgrYXomYWJzcj0xJmRrZHQ9MjAxMjAxMjMyMjAwMDAmZGt6b25lPXBydC5tdy53ZWIuMSZwZ3Q9MiZta3Q9eGFkZGlzcCZta3c9JnJlbHI9MSZpdGVtaWQ9YjU1NDYyOS5jQzYxREEzNzAtMEQ2NC00MDQ0LUFBRTEtRjk3RTk1MjZENThELmdjLXBob2VuaXgtYXouY3BhJmRraXRlbT1iNTU0NjI5LmNDNjFEQTM3MC0wRDY0LTQwNDQtQUFFMS1GOTdFOTUyNkQ1OEQuZ2MtcGhvZW5peC1hei5jcGEmZGtiYW10PTAuNjUmZGtzPWRlMjExYWIyLTAxODgtNGRkZS05Zjg0LThlNTRkYmJiMWU5YjIwMTIwMTIzMjIwMDAwX2FscGhhJmRrdT1odHRwJTNBJTJGJTJGd3d3LmJhbGwtbWNncmF3LmNvbQ==\",\"aHR0cDovL3d3dy5kZXhrbm93cy5jb20vYnVzaW5lc3NfcHJvZmlsZXMvYmFsbF9hbmRfbWNncmF3X3BjLWI1NTQ2Mjk=\",\"xaddisp\",\"\",\"Dex\",\"pandoraiphone411ws\",\"PPC\",\"\",\"\",\"\",\"Text\",\"\",\"false\",\"dexppc\"";
//        System.out.println(Arrays.asList(CsvUtils.csvLineAsArray(line, ",")));
        
        String[] input = new String[2];
        input[0] = "lVx7jt7Y";
        input[1] = "dental offices\\";
        System.out.println(CsvUtils.arrayAsCsv(input, ","));
    }
}