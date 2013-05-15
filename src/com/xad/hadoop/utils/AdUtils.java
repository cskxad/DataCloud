package com.xad.hadoop.utils;

import com.xad.hadoop.csvmappers.AdxImpressionTrackingMapper;

import java.util.*;

/**
 * Decode Ads
 */
public class AdUtils {

    public static final String NON_FILTERED_RECORD_SEPARATOR = "####";
    public static final String NON_FILTERED_KEYVALUE_SEPARATOR = ":::";
    public static final String NON_FILTERED_ROW_SEPARATOR = "\t";
    public static final String FILTERED_RECORD_SEPARATOR = NON_FILTERED_RECORD_SEPARATOR;
    public static final String FILTERED_KEYVALUE_SEPARATOR = NON_FILTERED_KEYVALUE_SEPARATOR;
    public static final String FILTERED_ROW_SEPARATOR = NON_FILTERED_ROW_SEPARATOR;

    public static List<HashMap<String, String>> deFormatAllRecords(String records, String recordSeparator,
                                                                   String rowSeparator, String keyValueSeparator) {
        List<HashMap<String, String>> lList = new ArrayList<HashMap<String, String>>();
        String rows[] = records.split(recordSeparator);
        for (int i = 0; i < rows.length; i++) {
            lList.add(deFormatRecord(rows[i], rowSeparator, keyValueSeparator));
        }
        return lList;
    }

    public static HashMap<String, String> deFormatRecord(String row, String rowSeparator, String keyValueSeparator) {
        String[] key = null;
        String[] val = null;
        HashMap<String, String> record = null;
        if (null != row && !row.isEmpty()) {
            key = row.split(rowSeparator);

            record = new HashMap<String, String>();
            for (int j = 0; j < key.length; j++) {
                val = key[j].split(keyValueSeparator);
                record.put(val[0].trim(), val.length > 1 ? val[1] : "");
            }
        }
        return record;
    }

    public static List<HashMap<String, String>> getAdInfoFromNonFilteredListings(String nonFilteredRecords) {
        if(nonFilteredRecords == null) {
            return null;
        }
        return deFormatAllRecords(nonFilteredRecords, NON_FILTERED_RECORD_SEPARATOR, NON_FILTERED_ROW_SEPARATOR, NON_FILTERED_KEYVALUE_SEPARATOR);
    }

    public static List<HashMap<String, String>> getAdInfoFromFilteredListings(String filteredRecords) {

        if(filteredRecords == null) {
            return null;
        }

        // cleanup the strings for start nd end
        filteredRecords = filteredRecords.replace("[{", "");
        filteredRecords = filteredRecords.replace("}]", "");
        filteredRecords = filteredRecords.replace("}, {", NON_FILTERED_RECORD_SEPARATOR);

        return deFormatAllRecords(filteredRecords, FILTERED_RECORD_SEPARATOR, FILTERED_ROW_SEPARATOR, FILTERED_KEYVALUE_SEPARATOR);
    }

    public static String[] parseAdInfo(String filteredListings, String nonFilteredListings, String appId) {

        List<String> filteredAds = new ArrayList<String>();

        if (filteredListings != null && filteredListings.trim().length() > 1) {
            List<HashMap<String, String>> parsedFilteredListings = AdUtils.getAdInfoFromFilteredListings(filteredListings);

            if (parsedFilteredListings != null) {
                for (Iterator<HashMap<String, String>> iterator = parsedFilteredListings.iterator(); iterator.hasNext(); ) {
                    HashMap<String, String> next = iterator.next();
                    filteredAds.add(getAdFromRecord(next, "filtered", appId));
                }
            }
        }

        if (nonFilteredListings != null && nonFilteredListings.trim().length() > 1) {
            List<HashMap<String, String>> parsedNonFilteredListings = AdUtils.getAdInfoFromNonFilteredListings(nonFilteredListings);

            List<String> nonFilteredAds = new ArrayList<String>(parsedNonFilteredListings.size());

            if (parsedNonFilteredListings != null) {
                for (Iterator<HashMap<String, String>> iterator = parsedNonFilteredListings.iterator(); iterator.hasNext(); ) {
                    HashMap<String, String> next = iterator.next();
                    nonFilteredAds.add(getAdFromRecord(next, "non-filtered", appId));
                }
            }

            filteredAds.addAll(nonFilteredAds);
        }

        return filteredAds.toArray(new String[0]);
    }

    public static String getAdFromRecord(final Map<String, String> adInfo, String sourceType, String appId) {
        //String[] adsData = new String[37];

        //Mukesh (2012-08-01 - Start)
        String[] adsData = new String[45];
        //Mukesh (2012-08-01 - End)

        if(adInfo == null) {
            System.out.println("Ad Info is null");
            return null;
        }

        adsData[AdxImpressionTrackingMapper.DateTime_Idx] = adInfo.get("csttimeformat");
        adsData[AdxImpressionTrackingMapper.Search_Id_Idx] = adInfo.get("searchrequestid");
        adsData[AdxImpressionTrackingMapper.Ad_Id_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Ad_Name_Idx] = adInfo.get("name");
        adsData[AdxImpressionTrackingMapper.Ad_Teaser_Idx] = adInfo.get("caption");
        adsData[AdxImpressionTrackingMapper.Ad_Teaser2_Idx] = adInfo.get("caption2");
        adsData[AdxImpressionTrackingMapper.Ad_Address_Idx] = adInfo.get("street");
        adsData[AdxImpressionTrackingMapper.Ad_City_Idx] = adInfo.get("city");
        adsData[AdxImpressionTrackingMapper.Ad_State_Idx] = adInfo.get("state");
        adsData[AdxImpressionTrackingMapper.Ad_Zip_Code_Idx] = adInfo.get("zip");
        adsData[AdxImpressionTrackingMapper.Ad_Latitude_Idx] = adInfo.get("latitude");
        adsData[AdxImpressionTrackingMapper.Ad_Longitude_Idx] = adInfo.get("longitude");
        adsData[AdxImpressionTrackingMapper.Ad_Phone_No_Idx] = adInfo.get("phone");
        adsData[AdxImpressionTrackingMapper.Ad_Rank_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Ad_Search_Input_Idx] = adInfo.get("searchinput");
        // TBD
        adsData[AdxImpressionTrackingMapper.Ad_Category_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Ad_Rating_Idx] = adInfo.get("rating");
        adsData[AdxImpressionTrackingMapper.Ad_Description_Idx] = adInfo.get("");
        adsData[AdxImpressionTrackingMapper.Ad_Business_Hours_Idx] = adInfo.get("bushours");
        adsData[AdxImpressionTrackingMapper.Bid_Rate_Idx] = adInfo.get("bidrate");
        adsData[AdxImpressionTrackingMapper.Secondary_Bid_Rate_Idx] = "0.0";
        adsData[AdxImpressionTrackingMapper.Website_URL_Idx] = adInfo.get("website");
        adsData[AdxImpressionTrackingMapper.Display_URL_Idx] = adInfo.get("displayurl");
        adsData[AdxImpressionTrackingMapper.Profile_URL_Idx] = adInfo.get("profileurl");
        adsData[AdxImpressionTrackingMapper.Publisher_Id_Idx] = adInfo.get("pubid");
        adsData[AdxImpressionTrackingMapper.Response_Bid_Rate_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Ad_Vendor_Idx] = adInfo.get("vendor");
        adsData[AdxImpressionTrackingMapper.App_Id_Idx] = appId;
        adsData[AdxImpressionTrackingMapper.Pay_Type_Idx] = adInfo.get("paytype");
        adsData[AdxImpressionTrackingMapper.Impression_URL_Idx] = adInfo.get("");
        adsData[AdxImpressionTrackingMapper.Estimated_ECPM_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Estimated_CTR_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Creative_type_Idx] = adInfo.get("creativetype");
        adsData[AdxImpressionTrackingMapper.Advertiser_Bid_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Ad_Returned_Idx] = sourceType;
        adsData[AdxImpressionTrackingMapper.Listing_Vendor_Idx] = adInfo.get("listingvendor");
        adsData[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx] = adInfo.get("adid");

        //Mukesh (2012-08-01 - Start)
        adsData[AdxImpressionTrackingMapper.Ad_Key_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Quality_Score_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Bus_Location_Id_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Creative_Id_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Neptune_Bkt_Id_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Group_Id_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Campaign_Id_Idx] = "";
        adsData[AdxImpressionTrackingMapper.Ad_Unit_Id_Idx] = "";
        //Mukesh (2012-08-01 - End)

        String ad = CsvUtils.arrayAsCsv(adsData, ",");
        //System.out.println(ad);
        return ad;
    }

    public static void main(String[] args) {

        String filteredAdKey = "2012-02-17 21:59:21[{^}]memphis[{^}]tn[{^}][{^}]0.0[{^}]0.0[{^}][{^}]criminal law attorneys[{^}]superpages[{^}]0[{^}]5009[{^}]10[{^}][{^}][{^}]FPlK8dK[{^}]http://apigslb.superpages.com/xml/search?XSL=off&search=Find+It&Results=PPC&SRC=ve-hyperlocal&STYPE=S&L=memphis+tn&C=criminal+law+attorneys&PI=1&PS=2&IP=198.64.136.68[{^}][{^}]0[{^}]0[{^}]0[{^}]getads[{^}]3[{^}]handmarkbanner411ws[{^}]0[{^}]0[{^}]";
        String requestLine = "\"BUxis9L\",\"2012-02-19 22:00:00\",\"handmarkbanner411ws\",\"BUxis9L-0\",\"true\",\"Impression\",\"9\"";
        int nextIndex = requestLine.indexOf('"', 2);

        String searchId = requestLine.substring(1, nextIndex);
        System.out.println(searchId);
        
        String adengineLogRecord = "2012-03-20 21:00:47.248[{^}]ve-hyperlocal-national[{^}]<img src=\"http://impr.superpages.com/ct/clickThrough?&SRC=ve-hyperlocal-national&campaignId=12617517&PGID=dalms107.8085.1332291647021.131226242948&bidType=IMPRCLIK&LOC=http://img.superpages.com/impr/impr.gif\"height=\"0\" width=\"0\" border=\"0\" alt=\"\" >[{^}]K S & B Cabinet Mart Inc.[{^}]Contact us for kitchen and bathroom cabinets and kitchen remodeling.[{^}][{^}][{^}][{^}][{^}][{^}][{^}]4046961453[{^}]0[{^}]kitchen cabinet remodeling & repair[{^}][{^}][{^}][{^}][{^}]83k0eJK3-0[{^}]0.29[{^}]0[{^}]3[{^}][{^}]kitchen cabinet remodeling & repair[{^}][{^}][{^}]buford[{^}]ga[{^}][{^}][{^}][{^}][{^}]handmarkbanner411ws[{^}]wQsj03LX[{^}]http://www.RealPagesSites.com[{^}][{^}]aHR0cDovL2NsaWNrcy5zdXBlcnBhZ2VzLmNvbS9jdC9jbGlja1Rocm91Z2g/U1JDPXZlLWh5cGVybG9jYWwtbmF0aW9uYWwmdGFyZ2V0PVNQJlBOPTEmVD1CdWZvcmQmUz1HQSZDPWtpdGNoZW4rY2FiaW5ldCtyZW1vZGVsaW5nKyUyNityZXBhaXImUEdJRD1kYWxtczEwNy44MDg1LjEzMzIyOTE2NDcwMjEuMTMxMjI2MjQyOTQ4JkFMRz0xMTEmVFM9YnVsbHMmQUNUSU9OPWxvZyxyZWQmVFI9MSZiaWRUeXBlPUNMSUsmcmVsYXRpdmVQb3NpdGlvbj0wJnBvc2l0aW9uPTAmUEdTTj1FMCZNVD1EJkRJRD1Sb1F0dElHb2Viem1OTFdtYiUyQlBvcFElM0QlM0QmYWNpZD1teEVRZTklMkIlMkY3Z0d2TCUyQm9iVjloWG5nJTNEJTNEJmNpZD0xMjYxNzUxNyZiaWQ9MCZFTT0wJk1ESUQ9SzJ1dWhXZCUyQkIlMkIwJTNEJlJTPTAuMDYzODA2NjUmRkw9dXJsJlRMPW9mZiZMT0M9aHR0cDovL21scy5tYXJjaGV4LmNvbS9jP2NpZD0yMiZhaWQ9MjA3MjAmY2FtcGFpZ25faWQ9MTk3MTAzNiZ0ZWNoPXNwZyZ1cmw9WGhCeDV5RVZSRFlOWENwdTB4cnVDZGhBZzNDbE1TWjNKTUJ1SFl2akpzeHdRR2Z4TEx6MGJ5YnZDY2JPRVg=[{^}][{^}]0.13[{^}]superpagessp[{^}]PPC[{^}]83k0eJK3[{^}]0[{^}][{^}][{^}][{^}]Text[{^}]0.29[{^}][{^}]hTLwKBhM5[{^}]";
        String tmpRec = "0[{^}][{^}][{^}]1[{^}]";
        String[] adRecs = AdUtils.parseFileRecord(adengineLogRecord);
        System.out.println("Count = "+adRecs.length);
        for (int i = 0; i < adRecs.length; i++) {
            String adRec = adRecs[i];
            System.out.println(i + "= " + adRec);
        }

        // Use Guava
//        Iterator<String> itr = Splitter.on("[{^}]").split(adengineLogRecord).iterator();
//        int count = 0;
//        while (itr.hasNext()) {
//            System.out.println(count++ + "="+ itr.next());
//        }
    }
    
    public static String[] parseFilteredAdInfo(String filteredListings, String appId) {

        List<String> filteredAds = new ArrayList<String>();
        
        if (filteredListings != null && filteredListings.trim().length() > 1) {
            List<HashMap<String, String>> parsedFilteredListings = AdUtils.getAdInfoFromFilteredListings(filteredListings);

            if (parsedFilteredListings != null) {
                for (Iterator<HashMap<String, String>> iterator = parsedFilteredListings.iterator(); iterator.hasNext(); ) {
                    HashMap<String, String> next = iterator.next();
                    filteredAds.add(getAdFromRecord(next, "filtered", appId));
                }
            }
        }
        
        return filteredAds.toArray(new String[0]);
    }

    public static String[] parseNonFilteredAdInfo(String nonFilteredListings, String appId) {

        List<String> nonFilteredAds = null;

        if (nonFilteredListings != null && nonFilteredListings.trim().length() > 1) {
            List<HashMap<String, String>> parsedNonFilteredListings = AdUtils.getAdInfoFromNonFilteredListings(nonFilteredListings);

            nonFilteredAds = new ArrayList<String>(parsedNonFilteredListings.size());

            if (parsedNonFilteredListings != null) {
                for (Iterator<HashMap<String, String>> iterator = parsedNonFilteredListings.iterator(); iterator.hasNext(); ) {
                    HashMap<String, String> next = iterator.next();
                    nonFilteredAds.add(getAdFromRecord(next, "non-filtered", appId));
                }
            }
        }

        return nonFilteredAds.toArray(new String[0]);
    }

    
    public static final int ADX_VENDOR_LOG_COLUMN_COUNT = 25;
    
    public static String[] parserVendorLogRecord(String record) throws RuntimeException {
        if(record == null) {
            return null;   
        }

        String[] recordSplits = record.split("\\[\\{\\^\\}\\]");

        if(recordSplits == null || recordSplits.length != ADX_VENDOR_LOG_COLUMN_COUNT) {
            throw new RuntimeException("Invalid Row. Either null or columns are missing");
        }

        return recordSplits;
    }

    /**
     * Parses the file record
     *
     * @param record
     * @return
     */
    public static String[] parseFileRecord(String record) {
        if(record == null) {
            return null;
        }

        String[] recordSplits = record.split("\\[\\{\\^\\}\\]", -1);
        return recordSplits;
    }

}