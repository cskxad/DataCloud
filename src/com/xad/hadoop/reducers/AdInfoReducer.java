package com.xad.hadoop.reducers;

import com.xad.hadoop.csvmappers.AdxImpressionTrackingMapper;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class AdInfoReducer extends Reducer<Text, Text, NullWritable, Text> {

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
        
        Map<String, String> rankMap = new HashMap<String, String>();

        for (Text value : values) {
            try {
                String[] adDetails = CsvUtils.csvLineAsArray(value.toString(), ",");

                if ("true".equals(adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx])) {
                    try {
                        returnedAds.add(adDetails);
                        String adId = adDetails[AdxImpressionTrackingMapper.Ad_Id_Idx];
                        String[] adIdParts = adId.split("-");
                        int adNumber = Integer.parseInt(adIdParts[1]);
                        if (adNumber > maxAdIndex) {
                            maxAdIndex = adNumber;
                        }
                        context.getCounter(AD_INFO_REDUCER_COUNTERS.RETURNED_AD_COUNT).increment(1);
                        context.write(NULL_KEY, new Text(value));
                    } catch (Exception e) {
                        System.err.println("Ad IS is = " + adDetails[AdxImpressionTrackingMapper.Ad_Id_Idx]);
                        System.err.println("Ad ID error error for " + value.toString());
                        e.printStackTrace();
                        context.getCounter(AD_INFO_REDUCER_COUNTERS.AD_ERROR).increment(1);
                    }
                } else if ("filtered".equals(adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx])) {
                    filteredAds.add(adDetails);
                    context.getCounter(AD_INFO_REDUCER_COUNTERS.IP_FILTERED_AD_COUNT).increment(1);
                } else if ("non-filtered".equals(adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx])) {
                    nonFilteredAds.add(adDetails);
                    context.getCounter(AD_INFO_REDUCER_COUNTERS.IP_NON_FILTERED_AD_COUNT).increment(1);
                } else if ("AdRank".equals(adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx])) {
                    rankMap.put(adDetails[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx], adDetails[AdxImpressionTrackingMapper.Ad_Rank_Idx]);
                } else {
                    // invalid ad type
                    context.getCounter(AD_INFO_REDUCER_COUNTERS.ERROR_COUNT).increment(1);
                    System.err.println("Ads details " + adDetails[AdxImpressionTrackingMapper.Ad_Returned_Idx]);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("Exception for Ad " + value.toString());
                context.getCounter(AD_INFO_REDUCER_COUNTERS.ERROR_COUNT).increment(1);
            }
        }

        for (Iterator<String[]> iterator = returnedAds.iterator(); iterator.hasNext(); ) {
            String[] returnedAd =  iterator.next();
            for (Iterator<String[]> iterator1 = nonFilteredAds.iterator(); iterator1.hasNext(); ) {
                String[] nonFilteredAd =  iterator1.next();
                if(returnedAd[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx] != null) {
                    if(!returnedAd[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx].equals(nonFilteredAd[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx])) {
                        // Value is different so ads are different, break nd move to next
                        continue;
                    }
                }
                context.getCounter(AD_INFO_REDUCER_COUNTERS.DUP_AD_COUNT).increment(1);
                iterator1.remove();
            }
        }

        // Lets write the non-filtered data
        if(nonFilteredAds.size() > 0) {
            for (Iterator<String[]> iterator = nonFilteredAds.iterator(); iterator.hasNext(); ) {
                String[] ad =  iterator.next();
                // update the data
                ad[AdxImpressionTrackingMapper.Ad_Returned_Idx] = "false";
                ad[AdxImpressionTrackingMapper.Ad_Rank_Idx] = rankMap.get(ad[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx]) != null ? rankMap.get(ad[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx]) : "-1";
                ad[AdxImpressionTrackingMapper.Ad_Id_Idx] = keyPrefix + ++maxAdIndex;
                context.getCounter(AD_INFO_REDUCER_COUNTERS.NON_FILTERED_AD_COUNT).increment(1);
                context.write(NULL_KEY, new Text(CsvUtils.arrayAsCsv(ad, ",")));
            }
        }

        if(filteredAds.size() > 0) {
            for (Iterator<String[]> iterator = filteredAds.iterator(); iterator.hasNext(); ) {
                String[] ad =  iterator.next();
                ad[AdxImpressionTrackingMapper.Ad_Returned_Idx] = "false";
                ad[AdxImpressionTrackingMapper.Ad_Id_Idx] = keyPrefix + ++maxAdIndex;
                ad[AdxImpressionTrackingMapper.Ad_Rank_Idx] = "-1";
                context.getCounter(AD_INFO_REDUCER_COUNTERS.FILTERED_AD_COUNT).increment(1);
                context.write(NULL_KEY, new Text(CsvUtils.arrayAsCsv(ad, ",")));
            }
        }
    }
}
