package com.xad.hadoop.utils;

import org.apache.catalina.util.RequestUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class HttpUtils {

    static final String GET_KEY = "GET ";

    public static Map<String, String[]> parseURL(String requestURI) throws UnsupportedEncodingException {
        Map<String, String[]> requestParams = new HashMap<String, String[]>();
//        requestURI = URLDecoder.decode(requestURI, "UTF-8");
        RequestUtil.parseParameters(requestParams, requestURI, "UTF-8");
        return requestParams;
    }

    public static Map<String, String[]> parserAccessLogURL(String accessLogLine) throws UnsupportedEncodingException {
        int idx = accessLogLine.indexOf(GET_KEY);
        if (idx != -1) {
            // get call.. lets parse it further
            String remainingLine = accessLogLine.substring(idx);

            int queryIdx = remainingLine.indexOf('?');

            if (queryIdx != -1) {
                String comQueryString = remainingLine.substring(queryIdx + 1, remainingLine.lastIndexOf(" "));
                return parseURL(comQueryString);
            }
        }
        return null;
    }

    public static Map<String, String[]> parserAccessLogURLWithoutRequestType(String accessLogLine) throws UnsupportedEncodingException {

        int queryIdx = accessLogLine.indexOf('?');

        if (queryIdx != -1) {
            String urlString = accessLogLine.substring(0, queryIdx);
            String comQueryString = accessLogLine.substring(queryIdx + 1, accessLogLine.lastIndexOf(" "));
            Map<String, String[]> params = parseURL(comQueryString);
            params.put("xad-request", new String[] {urlString});
            return params;
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        String line = "10.209.195.112 [28/Mar/2012:02:26:18 -0400] \"GET /rest/notify?t=imp&tid=0&k=E29K8SV_gEu7dXFIKxvIiTXt3o4-B4zlAvheo3H25QI.&v=1.1&uid=0d19384c09102f010957afbecba53126b217514e&l_id=7xkce5sZ%7E1xkce5sZ-0%7E1%7E1%7E1%7E3%7END%7Eus%7E1xkce5sZ%7E1%7E0%7E0%7E%7E0%7E1&type=banner HTTP/1.1\" 200 43 191 \"Mozilla/5.0 (iPhone; CPU iPhone OS 5_0_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Mobile/9A405\" \"http://display.xad.com/rest/banner?k=E29K8SV_gEu7dXFIKxvIiTXt3o4-B4zlAvheo3H25QI.&v=1.1&uid=0d19384c09102f010957afbecba53126b217514e&o_fmt=Html5&size=320x50&gender=f&age=24&loc=58601&devid=Mozilla%2F5.0%20%28iPhone%3B%20CPU%20iPhone%20OS%205_0_1%20like%20Mac%20OS%20X%29%20AppleWebKit%2F534.46%20%28KHTML%2C%20like%20Gecko%29%20Mobile%2F9A405&ip=72.20.80.111&appid=TFV-Voicemail\"";
        ApacheAcessLogParser parser = new ApacheAcessLogParser();
        Map request = parser.parse(line);
        System.out.println("Request Map = "+request);
    }

}
