package com.xad.hadoop.utils;

import com.xad.hadoop.mappers.vendorlog.VendorLogFileMapper;

/**
 * Parsers
 */
public class Parsers {

    public static String[] parseAdInfo(String filteredListings, String nonFilteredListings, String appId) {
        return AdUtils.parseAdInfo(filteredListings, nonFilteredListings, "dummy");
    }
    
    public static void parseVendorLog(String vendorLogRecord) {
        if(vendorLogRecord == null || vendorLogRecord.length() < 1) {
            System.out.println("empty line");
            return;
        }

        // parse the CSV file for notification data
        String[] vendorLogData = AdUtils.parserVendorLogRecord(vendorLogRecord);
        String[] vendorStatsData = new String [16];
        // adx_vendorlog.RequestId
        vendorStatsData[VendorLogFileMapper.Search_Id_Idx] = vendorLogData[VendorLogFileMapper.src_requestid_idx];
        // adx_vendorlog.Keyword
        vendorStatsData[VendorLogFileMapper.Keyword_Idx] = vendorLogData[VendorLogFileMapper.src_keyword_idx];
        // adx_vendorlog.timestamp
        vendorStatsData[VendorLogFileMapper.Timestamp_Idx] = vendorLogData[VendorLogFileMapper.src_timestamp_idx];
        // adx_vendorlog.vendor
        vendorStatsData[VendorLogFileMapper.Vendor_Idx] = vendorLogData[VendorLogFileMapper.src_vendor_idx].toLowerCase();

        // adx_vendorlog.City
        vendorStatsData[VendorLogFileMapper.City_Idx] = vendorLogData[VendorLogFileMapper.src_city_idx];
        // adx_vendorlog.state
        vendorStatsData[VendorLogFileMapper.State_Idx] = vendorLogData[VendorLogFileMapper.src_state_idx];
        // adx_vendorlog.ZipCode
        vendorStatsData[VendorLogFileMapper.Zipcode_Idx] = vendorLogData[VendorLogFileMapper.src_zip_idx];
        // adx_vendorlog.Latitude
        vendorStatsData[VendorLogFileMapper.Latitude_Idx] = vendorLogData[VendorLogFileMapper.src_lat_idx];
        // adx_vendorlog.Longitude
        vendorStatsData[VendorLogFileMapper.Longitude_Idx] = vendorLogData[VendorLogFileMapper.src_long_idx];
        // adx_vendorlog.Status
        vendorStatsData[VendorLogFileMapper.Status_Idx] = vendorLogData[VendorLogFileMapper.src_status_idx];
        // adx_vendorlog.numResults
        vendorStatsData[VendorLogFileMapper.Number_of_Result_Idx] = vendorLogData[VendorLogFileMapper.src_numresults_idx];
        // adx_vendorlog.ResponseTime
        vendorStatsData[VendorLogFileMapper.Response_Time_Idx] = vendorLogData[VendorLogFileMapper.src_responsetime_idx];
        // adx_vendorlog.url
        vendorStatsData[VendorLogFileMapper.Get_Ad_URL_Idx] = vendorLogData[VendorLogFileMapper.src_requrl_idx];
        // adx_vendorlog.connectiontime
        vendorStatsData[VendorLogFileMapper.Connection_Time_Idx] = vendorLogData[VendorLogFileMapper.src_connectiontime_idx];
        // adx_vendorlog.readtime
        vendorStatsData[VendorLogFileMapper.Read_Time_Idx] = vendorLogData[VendorLogFileMapper.src_readtime_idx];
        // adx_vendorlog.queuetime
        vendorStatsData[VendorLogFileMapper.Queue_Time_Idx] = vendorLogData[VendorLogFileMapper.src_queuetime_idx];
        String outputStr = CsvUtils.arrayAsCsv(vendorStatsData, ",");
        System.out.println(outputStr);

        // parse ads
        String filteredListing = vendorLogData[VendorLogFileMapper.src_filteredlistings_idx];
        String nonFilteredListings = vendorLogData[VendorLogFileMapper.src_nonfilteredlistings_idx];

        String[] ads = AdUtils.parseAdInfo(filteredListing, nonFilteredListings, vendorLogData[VendorLogFileMapper.src_appid_idx]);

        for (int i = 0; i < ads.length; i++) {
            String ad = ads[i];
            System.out.println(ad);
        }
    }

    public static void main(String[] args) {
        
        String line = ",,\"\",,,,,,,,,,,\"\",,\"\",,,,,\"0.0\",,,,,\"\",,\"pandoraiphone411ws\",,,\"\",\"\",,\"\",\"filtered\",,";
        
        if(line.startsWith(",,")) {
            System.out.println(true);
        }
        
        Parsers.parseVendorLog("2012-02-17 21:59:17[{^}]lanham[{^}]md[{^}][{^}]0.0[{^}]0.0[{^}][{^}]drug charges attorneys[{^}]superpages[{^}]0[{^}]0[{^}]49[{^}][{^}][{^}]9xdLLJK3[{^}]http://apigslb.superpages.com/xml/search?XSL=off&search=Find+It&Results=PPC&SRC=ve-hyperlocal&STYPE=S&L=lanham+md&C=drug+charges+attorneys&PI=1&PS=2&IP=198.64.136.68[{^}][{^}]0[{^}]49[{^}]0[{^}]getads[{^}]3[{^}]pandoraiphone411ws[{^}]0[{^}]0[{^}]");
    }

}
