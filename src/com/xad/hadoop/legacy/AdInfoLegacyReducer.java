package com.xad.hadoop.legacy;

import com.xad.hadoop.csvmappers.AdxImpressionTrackingMapper;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class AdInfoLegacyReducer extends Reducer<Text, Text, NullWritable, Text> {

    private static final NullWritable NULL_KEY = NullWritable.get();

    public static enum AD_INFO_REDUCER_COUNTERS {
        RETURNED_AD_COUNT,
        FILTERED_AD_COUNT,
        NON_FILTERED_AD_COUNT,
        IP_FILTERED_AD_COUNT,
        IP_NON_FILTERED_AD_COUNT,
        DUP_AD_COUNT,
        ERROR_COUNT,
        AD_ERROR
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        List<String[]> returnedAds = new ArrayList<String[]>();
        List<String[]> filteredAds = new ArrayList<String[]>();
        List<String[]> nonFilteredAds = new ArrayList<String[]>();
        int maxAdIndex = 0;

        String keyPrefix = key.toString() + "-";

        //debug
//        System.out.println("Processing Ads for Search ID =  "+key.toString());
        
        for (Text value : values) {
//            System.out.println("Ad Values = "+value.toString());
            String[] adDetails = CsvUtils.csvLineAsArray(value.toString(), ",");
            if("true".equals(adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx])) {
                try {
                    returnedAds.add(adDetails);
                    String adId = adDetails[AdxImpressionTrackingMapper.Ad_Id_Idx];
                    String[] adIdParts = adId.split("-");
                    int adNumber = Integer.parseInt(adIdParts[1]);
                    if (adNumber > maxAdIndex) {
                        maxAdIndex = adNumber;
                    }

                    // write the ad
                    adDetails[AdxImpressionTrackingMapper.Ad_Rank_Idx] = String.valueOf(adNumber);
                    context.getCounter(AD_INFO_REDUCER_COUNTERS.RETURNED_AD_COUNT).increment(1);
                    context.write(NULL_KEY, new Text(value.toString().replace("\n", " ")));
                } catch (Exception e) {
                    System.err.println("Ad IS is = " + adDetails[AdxImpressionTrackingMapper.Ad_Id_Idx]);
                    System.err.println("Ad ID error error for " + value.toString());
                    e.printStackTrace();
                    context.getCounter(AD_INFO_REDUCER_COUNTERS.AD_ERROR).increment(1);
                }
            } else if("filtered".equals(adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx])) {
                filteredAds.add(adDetails);
                context.getCounter(AD_INFO_REDUCER_COUNTERS.IP_FILTERED_AD_COUNT).increment(1);
            } else if ("non-filtered".equals(adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx])) {
                nonFilteredAds.add(adDetails);
                context.getCounter(AD_INFO_REDUCER_COUNTERS.IP_NON_FILTERED_AD_COUNT).increment(1);
            } else {
                // invalid ad type
                context.getCounter(AD_INFO_REDUCER_COUNTERS.ERROR_COUNT).increment(1);
                System.err.println("Ads details "+adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx]);
            }
        }

        // time to go through pain and filter the duplicate Ads
        /**
         * This step is to be executed only if we have ads in Non-Filtered case, which is a least possible scenario, if
         * Ads are present for a search
         *
         * An Ad is duplicate if following are same
         * Name
         * City
         * State
         * Zip
         * Phone
         * Vendor
         *
         * For filtering out we shall just compare Non-Filtered Listing with Returned Ads.
         * Filtered listing was dropped at initial phase
         */
        for (Iterator<String[]> iterator = returnedAds.iterator(); iterator.hasNext(); ) {
            String[] returnedAd =  iterator.next();
            for (Iterator<String[]> iterator1 = nonFilteredAds.iterator(); iterator1.hasNext(); ) {
                String[] nonFilteredAd =  iterator1.next();
                if(returnedAd[AdxImpressionTrackingMapper.Ad_Name_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.Ad_Name_Idx].equals(nonFilteredAd[AdxImpressionTrackingMapper.Ad_Name_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }

                // Lets check for City
                if(returnedAd[AdxImpressionTrackingMapper.Ad_City_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.Ad_City_Idx].equals(nonFilteredAd[AdxImpressionTrackingMapper.Ad_City_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }

                // Lets check State
                if(returnedAd[AdxImpressionTrackingMapper.Ad_State_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.Ad_State_Idx].equals(nonFilteredAd[AdxImpressionTrackingMapper.Ad_State_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }

                // Lets check Zip
                if(returnedAd[AdxImpressionTrackingMapper.Ad_Zip_Code_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.Ad_Zip_Code_Idx].equals(nonFilteredAd[AdxImpressionTrackingMapper.Ad_Zip_Code_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }

                // Lets check Phone
                if(returnedAd[AdxImpressionTrackingMapper.Ad_Phone_No_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.Ad_Phone_No_Idx].equals(nonFilteredAd[AdxImpressionTrackingMapper.Ad_Phone_No_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }

                // Lets check Vendor

                // check listing vendor
                if(returnedAd[AdxImpressionTrackingMapper.Listing_Vendor_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.Listing_Vendor_Idx].equalsIgnoreCase(nonFilteredAd[AdxImpressionTrackingMapper.Listing_Vendor_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }

                if(returnedAd[AdxImpressionTrackingMapper.Ad_Search_Input_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.Ad_Search_Input_Idx].equalsIgnoreCase(nonFilteredAd[AdxImpressionTrackingMapper.Ad_Search_Input_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }
                context.getCounter(AD_INFO_REDUCER_COUNTERS.DUP_AD_COUNT).increment(1);
                iterator1.remove();
            }
        }

        try {
            // Lets write the non-filtered data
            if(nonFilteredAds.size() > 0) {
                for (Iterator<String[]> iterator = nonFilteredAds.iterator(); iterator.hasNext(); ) {
                    String[] ad =  iterator.next();
                    // update the data
                    ad[AdxImpressionTrackingMapper.Ad_Returned_Idx] = "false";
                    ad[AdxImpressionTrackingMapper.Ad_Rank_Idx] = "-1";
                    ad[AdxImpressionTrackingMapper.Ad_Id_Idx] = keyPrefix + ++maxAdIndex;
                    context.getCounter(AD_INFO_REDUCER_COUNTERS.NON_FILTERED_AD_COUNT).increment(1);
                    context.write(NULL_KEY, new Text(CsvUtils.arrayAsCsv(ad, ",").replace("\n", " ")));
                }
            }

            if(filteredAds.size() > 0) {
                for (Iterator<String[]> iterator = filteredAds.iterator(); iterator.hasNext(); ) {
                    String[] ad =  iterator.next();
                    ad[AdxImpressionTrackingMapper.Ad_Returned_Idx] = "false";
                    ad[AdxImpressionTrackingMapper.Ad_Id_Idx] = keyPrefix + ++maxAdIndex;
                    ad[AdxImpressionTrackingMapper.Ad_Rank_Idx] = "-1";
                    context.getCounter(AD_INFO_REDUCER_COUNTERS.FILTERED_AD_COUNT).increment(1);
                    context.write(NULL_KEY, new Text(CsvUtils.arrayAsCsv(ad, ",").replace("\n", " ")));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            context.getCounter(AD_INFO_REDUCER_COUNTERS.ERROR_COUNT).increment(1);
        }
    }
}
