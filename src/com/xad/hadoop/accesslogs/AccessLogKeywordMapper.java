package com.xad.hadoop.accesslogs;

import com.xad.hadoop.mappers.AbstractCacheMapper;
import com.xad.hadoop.utils.ApacheAcessLogParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

/**
 * 
 */
public class AccessLogKeywordMapper extends AbstractCacheMapper<LongWritable, Text, Text, IntWritable>  {
    @Override
    public String[] parseRecord(String record) {
        return new String[] { record.trim(),""};
    }

    protected ApacheAcessLogParser accessLogs = new ApacheAcessLogParser();

    public static final int MAX_AGE = 99;

    public static enum COUNTERS {
        REQUEST_COUNT,
        FAILED_REQUEST_COUNT,
        ERROR_COUNT,
        AGE_FOUND,
        AGE_NOT_FOUND,
        GENDER_FOUND,
        GENDER_NOT_FOUND,
        NULL_REQUEST_PARAM,
        CATEGORY_MATCH,
        CATEGORY_NOT_MATCHED
    }

    public static final IntWritable one = new IntWritable(1);
    public static final Text UNKNOWN_GENDER = new Text("UNKNOWN_GENDER");
    public static final Text UNKNOWN_AGE = new Text("UNKNOWN_AGE");
    public static final Text MALE = new Text("Male");
    public static final Text FEMALE = new Text("Female");

    public static final Text AGE_GROUP_1317 = new Text("13-17");
    public static final Text AGE_GROUP_1824 = new Text("18-24");
    public static final Text AGE_GROUP_2534 = new Text("25-34");
    public static final Text AGE_GROUP_3544 = new Text("35-44");
    public static final Text AGE_GROUP_4554 = new Text("45-54");
    public static final Text AGE_GROUP_5564 = new Text("55-64");
    public static final Text AGE_GROUP_65 = new Text("65+");
    public static final Text AGE_GROUP_INVALID = new Text("Invalid-Age-group");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            Map log = accessLogs.parse(line);

            // what to do here
            if (log != null) {
                Map<String, String[]> requestParams = (Map<String, String[]>) log.get(ApacheAcessLogParser.REQUEST_PARAMETER);
                if (requestParams != null) {
                    
                    String[] keywords = requestParams.get("i");

                    boolean isCategoryMatch = false;

                    if(keywords != null) {
                        for (int i = 0; i < keywords.length; i++) {
                            String keyword = keywords[i];
                            if (keyword != null && cache.containsKey(keyword)) {
                                isCategoryMatch = true;
                                context.getCounter(COUNTERS.CATEGORY_MATCH).increment(1);
                                break;
                            }
                        }
                    }

                    if(!isCategoryMatch) {
                        context.getCounter(COUNTERS.CATEGORY_NOT_MATCHED).increment(1);
                        return;
                    }
                    
                    
                    String gender = getRequestParameter(requestParams, "gender");

                    if (gender != null && gender.trim().length() > 0) {
                        if (gender.equalsIgnoreCase("f") || gender.equalsIgnoreCase("female")) {
                            context.write(FEMALE, one);
                            context.getCounter(COUNTERS.GENDER_FOUND).increment(1);
                        } else if (gender.equalsIgnoreCase("m") || gender.equalsIgnoreCase("male")) {
                            context.write(MALE, one);
                            context.getCounter(COUNTERS.GENDER_FOUND).increment(1);
                        }
                    } else {
                        context.write(UNKNOWN_GENDER, one);
                        context.getCounter(COUNTERS.GENDER_NOT_FOUND).increment(1);
                    }

                    // Age

                    String age = getRequestParameter(requestParams, "age");

                    if (age != null && age.trim().length() > 0) {

                        try {
                            int ageInt = Integer.parseInt(age);

                            if (ageInt >= 13 && ageInt <= MAX_AGE) {
                                if (ageInt <= 17) {
                                    context.write(AGE_GROUP_1317, one);
                                }
                                else if (ageInt > 17 && ageInt <= 24) {
                                    context.write(AGE_GROUP_1824, one);
                                }
                                else if (ageInt > 24 && ageInt <= 34) {
                                    context.write(AGE_GROUP_2534, one);
                                }
                                else if (ageInt > 34 && ageInt <= 44) {
                                    context.write(AGE_GROUP_3544, one);
                                }
                                else if (ageInt > 44 && ageInt <= 54) {
                                    context.write(AGE_GROUP_4554, one);
                                }
                                else if (ageInt > 54 && ageInt <= 64) {
                                    context.write(AGE_GROUP_5564, one);
                                } else {
                                    context.write(AGE_GROUP_65, one);
                                }
                            }
                            else {
                                context.write(AGE_GROUP_INVALID, one);
                            }

                            context.getCounter(COUNTERS.AGE_FOUND).increment(1);
                        } catch (NumberFormatException numEx) {
                            // NOOP
                        }
                    } else {
                        context.write(UNKNOWN_AGE, one);
                        context.getCounter(COUNTERS.AGE_NOT_FOUND).increment(1);
                    }
                } else {
                    context.getCounter(COUNTERS.NULL_REQUEST_PARAM).increment(1);
                }
            }
        } catch (Exception e) {
            context.getCounter(COUNTERS.ERROR_COUNT).increment(1);
        }
    }

    public String getRequestParameter(Map<String, String[]> requestParameter, String key) {
        String[] values = requestParameter.get(key);
        if (values != null) {
            return values[0];
        }
        return null;
    }
}
