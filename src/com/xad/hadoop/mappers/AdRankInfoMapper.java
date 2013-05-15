package com.xad.hadoop.mappers;

import com.xad.hadoop.csvmappers.AdxImpressionTrackingMapper;
import com.xad.hadoop.utils.AdUtils;
import com.xad.hadoop.utils.CsvUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *
 */
public class AdRankInfoMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final NullWritable nullWritable = NullWritable.get();

    public static enum ADRANK_COUNTER {
        AD_COUNTER,
        ERROR_COUNTER,
        EMPTY_LINE
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.out.println(((FileSplit)context.getInputSplit()).getPath());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {

            if(line == null || line.length() < 1) {
                System.out.println("empty line");
                context.getCounter(ADRANK_COUNTER.EMPTY_LINE).increment(1);
                return;
            }

            String[] adRankData = AdUtils.parseFileRecord(line);

            //String[] adsData = new String[37];

            //Mukesh (2012-08-01 - Start)
            String[] adsData = new String[45];
            //Mukesh (2012-08-01 - End)

            adsData[AdxImpressionTrackingMapper.Search_Id_Idx] = adRankData[1];
            adsData[AdxImpressionTrackingMapper.DateTime_Idx] = adRankData[0];
            adsData[AdxImpressionTrackingMapper.Ad_Id_Idx] = adRankData[2];
            adsData[AdxImpressionTrackingMapper.Ad_Name_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Teaser_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Teaser2_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Address_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_City_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_State_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Zip_Code_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Latitude_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Longitude_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Phone_No_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Rank_Idx] = adRankData[3];
            adsData[AdxImpressionTrackingMapper.Ad_Search_Input_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Category_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Rating_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Description_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Business_Hours_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Bid_Rate_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Secondary_Bid_Rate_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Website_URL_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Display_URL_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Profile_URL_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Publisher_Id_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Response_Bid_Rate_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Vendor_Idx] = "";
            adsData[AdxImpressionTrackingMapper.App_Id_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Pay_Type_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Impression_URL_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Estimated_ECPM_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Estimated_CTR_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Creative_type_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Advertiser_Bid_Idx] = "";
            adsData[AdxImpressionTrackingMapper.Ad_Returned_Idx] = "AdRank";
            adsData[AdxImpressionTrackingMapper.Listing_Vendor_Idx] = "";
            adsData[AdxImpressionTrackingMapper.AD_UNIQUE_ID_Idx] = adRankData[2];

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

            context.getCounter(ADRANK_COUNTER.AD_COUNTER).increment(1);
            context.write(nullWritable, new Text(CsvUtils.arrayAsCsv(adsData, ",")));
        } catch (Exception e) {
            context.getCounter(ADRANK_COUNTER.ERROR_COUNTER).increment(1);
            System.err.println("Status :: Failed for line = "+line);
        }
    }
}
