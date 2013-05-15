package com.xad.hadoop.utils;

import au.com.bytecode.opencsv.CSVWriter;
import au.com.bytecode.opencsv.bean.ColumnPositionMappingStrategy;
import com.xad.hadoop.Utils;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents Apache Access Logs
 *
 * @todo - Unify Date format, Search Type field to be populated
 */
public class ApacheAccessLog {

    private static final String regEx = "^([\\d.]+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"GET /rest/(.+?)\" (\\d{3}) (\\d+) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
//    private static final String regEx = "([0-9\\.]*) \\[(([ 0-9]{1,2})/([0-9a-zA-Z]{1,3})/([0-9]{2,4})[^ ]*([0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})) [^ ]*\\] \"[^ ]*GET (http://[^/]*)?(\\/[^ ]*) [^ ]*\" ([0-9]*) ([0-9\\-][0-9]*) ([0-9]*) \"([^\"]*)\"$";

    private int _ip = 1;
    private int _uri = 3;
    private int _status = 4;
    private int _agent = 7;
    private int _size = 5;
    private int _time = 6;
    private int _date = 2;
    private int _referrer = 8;

    Pattern pattern;

    public ApacheAccessLog() {
        try {
            pattern = Pattern.compile(regEx);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public AccessLog parse(String line) throws Exception {
        Matcher matcher = pattern.matcher(line);

        if (matcher.matches()) {
            return new AccessLog(matcher.group(_ip), matcher.group(_uri), matcher.group(_status), matcher.group(_agent),
                                    matcher.group(_size), matcher.group(_time), matcher.group(_date), matcher.group(_referrer));
        } else {
//            System.err.println("match failed for line : "+line);
        }
        return null;
    }

    public class Location {
        private String city;
        private String state;
        private int zipcode;
        private int airportCode;
        boolean isCityPresent;
        boolean isStatePresent;
        boolean isZipCodePresent;
        boolean isAirportCodePresent;
        private String locParameter;

        public Location(String loc) {
            locParameter = loc;
        }

        public void parse() {
            if(locParameter != null && locParameter.trim().length() > 0) {

            }
        }
    }

    public class AccessLog {

        private String[] header = {"remoteIP", "date", "Server IP", "Instance ID", "size", "time", "virtual host",
                                    "Status", "userAgent", "Search", "Request Type", "API Version", "Publisher key", "Application Id",
                                    "User Id", "Request Id", "Device Id", "Search keywords", "Latitude",
                                    "Longitude", "Location", "City", "State", "Zipcode", "Country", "Airport Code", "Number of Ads",
                                    "Output Format", "Callback function", "Referal", "Display IP", "Auto Impression",
                                    "Search Category", "Start Index", "Desired Number of Listing", "Sort Type",
                                    "First Name", "Last Name", "Result Type", "Neighbour Address", "Search Radius",
                                    "Data Source", "Result Set", "Display Place Id", "Banner Size", "Age",
                                    "Gender", "Language", "Interest"};

        {
            ColumnPositionMappingStrategy strat = new ColumnPositionMappingStrategy();
            strat.setType(AccessLog.class);
            strat.setColumnMapping(header);
        }

        private String remoteIP;
        private String uri;
        private String status;
        private String userAgent;
        private String size;
        private String time;
        private String date;
        private Map<String, String[]> requestParameter;
        private String virtualHost;
        private String serverIP;
        private String instanceID;
        private String referrer;

        public String getReferrer() {
            return referrer;
        }

        public void setReferrer(String referrer) {
            this.referrer = referrer;
        }

        public String getVirtualHost() {
            return virtualHost;
        }

        public void setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
        }

        public String getServerIP() {
            return serverIP;
        }

        public void setServerIP(String serverIP) {
            this.serverIP = serverIP;
        }

        public String getInstanceID() {
            return instanceID;
        }

        public void setInstanceID(String instanceID) {
            this.instanceID = instanceID;
        }

        public boolean isNotificationRequest() {
            if("notify".equalsIgnoreCase(getRequestParameter("xad-request"))) {
//                System.out.println("Notification request "+getRequestParameter("xad-request"));
                return true;
            }
            return false;
        }

        AccessLog(String remoteIP, String uri, String status, String userAgent, String size, String time, String date,
                    String referrer) {
            this.remoteIP = remoteIP;
            this.uri = uri;
            this.status = status;
            this.userAgent = userAgent;
            this.size = size;
            this.time = time;
            this.date = date;
            this.referrer = referrer;
            requestParameter = getRequestParameters();
        }

        public String getRemoteIP() {
            return remoteIP;
        }

        public String getUri() {
            return uri;
        }

        public String getStatus() {
            return status;
        }

        public String getUserAgent() {
            return userAgent;
        }

        public String getSize() {
            return size;
        }

        public String getTime() {
            return time;
        }

        public String getDate() {
            return date;
        }

        public String getRequestParameter(String key) {
            String[] values = requestParameter.get(key);
            if(values != null) {
                return values[0];
            }
            return null;
        }

        public String getRequestParameters(String key, String separator) {
            String[] values = requestParameter.get(key);

            if(values == null || values.length == 0) {
                return null;
            }

            String returnValue = "";
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                if(i != 0 && i != values.length ) {
                    returnValue = returnValue + separator + value;
                } else if(i == 0 || i == values.length){
                    returnValue += value;
                }
            }
            return returnValue;
        }

        private String[] asArray() {

            if(!"notify".equalsIgnoreCase(getRequestParameter("xad-request"))) {
                return new String[]{getDate(), getRemoteIP(), serverIP, instanceID, getSize(), getTime(),
                                    virtualHost, getStatus(), getUserAgent(), referrer,
                                    "Search", getRequestType(getRequestParameter("xad-request")),
                                    getRequestParameter("v"), decodeAccessKey(getRequestParameter("k")),
                                    getRequestParameter("appid"), getRequestParameter("uid"),
                                    getRequestParameter("reqid"), getRequestParameter("devid"),
                                    getRequestParameters("i", "\\,"), getRequestParameter("lat"),
                                    getRequestParameter("long"), getRequestParameter("loc"),
                                    // Address fields
                                    "-", "-", "-", getRequestParameter("co"), "-",
                                    getRequestParameter("n_ad"), getRequestParameter("o_fmt"),
                                    getRequestParameter("cb"), getRequestParameter("ref"),
                                    getRequestParameter("ip"), getRequestParameter("auto_imp"),
                                    getRequestParameters("f_cat", "\\,"), getRequestParameter("s"),
                                    getRequestParameter("n"), getRequestParameter("sort"),
                                    getRequestParameter("f_name"), getRequestParameter("l_name"),
                                    getRequestParameter("resulttype"), getRequestParameter("nbor"),
                                    getRequestParameter("f_radius"), getRequestParameter("ds"),
                                    getRequestParameter("rset"), getRequestParameter("placeid"),
                                    getRequestParameter("size"), getRequestParameter("age"),
                                    getRequestParameter("gender"), getRequestParameter("lang"),
                                    getRequestParameter("interest")
                                    };
            } else {
                return new String[]{getDate(), getRemoteIP(), serverIP, instanceID, getSize(), getTime(),
                                    virtualHost, getStatus(), getUserAgent(),
                                    getNotificationType(getRequestParameters("t", "\\,")), getRequestParameter("v"),
                                    decodeAccessKey(getRequestParameter("k")), getRequestParameters("l_id", "\\,"),
                                    getRequestParameter("uid"), getRequestParameter("ts"),
                                    getRequestParameter("ip"), referrer
                                    };
            }
        }

        /**
         * returns the request type
         * @param tCode
         * @return
         */
        protected String getNotificationType(String tCode) {

            if(tCode.contains(",")) {
                // shit handle it
                tCode.replace(",", "\\,");
            }

            if(tCode.equalsIgnoreCase("imp")) {
                return "Impression";
            } else if(tCode.equalsIgnoreCase("click")) {
                return "Click";
            } else if(tCode.equalsIgnoreCase("review")) {
                return "Review";
            } else if(tCode.equalsIgnoreCase("desc")) {
                return "Description";
            } else if(tCode.equalsIgnoreCase("map")) {
                return "Map";
            } else if(tCode.equalsIgnoreCase("direction")) {
                return "Direction";
            } else if(tCode.equalsIgnoreCase("arrival")) {
                return "Arrival";
            } else if(tCode.equalsIgnoreCase("checkin")) {
                return "Checkin";
            } else if(tCode.equalsIgnoreCase("call")) {
                return "Call";
            } else if(tCode.equalsIgnoreCase("sms")) {
                return "SMS";
            } // these parameters are from REST 1.2.1 API
              else if(tCode.equalsIgnoreCase("save_pb")) {
                return "Saved in Phone Book";
            } else if(tCode.equalsIgnoreCase("save_ms")) {
                return "Saved in My Searches";
            } else if(tCode.equalsIgnoreCase("callop")) {
                return "Call to Operator";
            } else if(tCode.equalsIgnoreCase("moreinfo")) {
                return "More Info";
            } else if(tCode.equalsIgnoreCase("bannercall")) {
                return "Banner Call";
            }

            System.out.println("tCode = " + tCode);
            return "Invalid:"+tCode;
        }

        public String getRequestType(String rCode) {
            if(rCode == null) {
                return "Invalid:NULL";
            } else if("local".equalsIgnoreCase(rCode)) {
                return "Local Search";
            } else if("more_info".equalsIgnoreCase(rCode)) {
                return "More Info";
            } else if("residential".equalsIgnoreCase(rCode)) {
                return "Residential Search";
            } else if("reverse".equalsIgnoreCase(rCode)) {
                return "Reverse Lookup";
            } else if("ebl".equalsIgnoreCase(rCode)) {
                return "EBL API";
            } else if("exebl".equalsIgnoreCase(rCode)) {
                return "Ex EBL API";
            } else if("localex".equalsIgnoreCase(rCode)) {
                return "Extended Local API";
            } else if("banner".equalsIgnoreCase(rCode)) {
                return "Local Display";
            } else if("bannerex".equalsIgnoreCase(rCode)) {
                return "Enhanced Local Display";
            }

            return "Invalid:"+rCode;
        }

        public Map<String, String[]> getRequestParameters() {
            try {
                return HttpUtils.parserAccessLogURLWithoutRequestType(getUri());
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected String decodeAccessKey(String accessKey) {
            return Utils.parseAccesskey(accessKey);
        }

        public String toCsv() {
            StringWriter writer = new StringWriter();
            CSVWriter csvWriter = new CSVWriter(writer);
            csvWriter.writeNext(asArray());
            return writer.toString();
        }

        @Override
        public String toString() {
            return "AccessLog{" +
                    "header=" + (header == null ? null : Arrays.asList(header)) +
                    ", remoteIP='" + remoteIP + '\'' +
                    ", uri='" + uri + '\'' +
                    ", status='" + status + '\'' +
                    ", userAgent='" + userAgent + '\'' +
                    ", size='" + size + '\'' +
                    ", time='" + time + '\'' +
                    ", date='" + date + '\'' +
//                    ", \n Request Params = " + getRequestParameters().toString() +
                    '}';
        }
    }
}