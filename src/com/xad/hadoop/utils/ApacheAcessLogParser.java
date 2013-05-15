package com.xad.hadoop.utils;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class ApacheAcessLogParser {

//    private static final String regEx = "^([\\d.]+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"GET /rest/(.+?)\" (\\d{3}) (\\d+) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
    private static Pattern pattern = Pattern.compile("^([\\d.]+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"GET /rest/(.+?)\" (\\d{3}) (\\d+) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
//            .compile("^([\\d\\.]+)\\s+\\[(.*)\\]\\s+\"([^\"]*)\"\\s+([\\d\\-]+)\\s+([\\d\\-]+)\\s+([\\d\\-]+)\\s+\"([^\"]*)\"\\s*\"*([^\"]*)\"*");
    private int _ip = 1;
    private int _uri = 3;
    private int _status = 4;
    private int _agent = 7;
    private int _size = 5;
    private int _time = 6;
    private int _date = 2;
    private int _referrer = 8;

    // Constants
    public static final String REMOTE_IP = "REMOTE_IP";
    public static final String URI = "URI";
    public static final String STATUS = "STATUS";
    public static final String USER_AGENT = "USER_AGENT";
    public static final String SIZE = "SIZE";
    public static final String TIME ="TIME";
    public static final String DATE = "DATE";
    public static final String REQUEST_PARAMETER = "REQUEST_PARAMETERS";
    public static final String REFERRER = "REFERRER";

//    Pattern pattern;

    public ApacheAcessLogParser() {
//        try {
//            pattern = Pattern.compile(regEx);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
    }

    public Map parse(String line) throws Exception {
        Matcher matcher = pattern.matcher(line);
        Map paramMap = new HashMap();
        if (matcher.matches()) {
            paramMap.put(REMOTE_IP, matcher.group(_ip));
            paramMap.put(URI, matcher.group(_uri));
            paramMap.put(STATUS, matcher.group(_status));
            paramMap.put(USER_AGENT, matcher.group(_agent));
            paramMap.put(SIZE, matcher.group(_size));
            paramMap.put(TIME, matcher.group(_time));
            paramMap.put(DATE, matcher.group(_date));
            paramMap.put(REFERRER, matcher.group(_referrer));
            paramMap.put(REQUEST_PARAMETER, parseRequestParameters(matcher.group(_uri)));
        } else {
            throw new Exception("Paring failed");
        }
        return paramMap;
    }
    
    public Map parseRequestParameters(String uri) throws UnsupportedEncodingException {
        return HttpUtils.parserAccessLogURLWithoutRequestType(uri);
    }
}