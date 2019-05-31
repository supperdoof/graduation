package com.spark;

import java.util.List;

/**
 * Created by hadoop on 17-11-28.
 * 用于解析JSON字符串
 */
public class Information {
    private String deviceid;
    private String accountid;
    private String date;
    private OsInfo os_info;
    private String versioncode;
    private String channel;
    private String resolution;
    private String access;
    private String ip;
    private String cpu;
    private String operators;
    private String network_type;
    private String subtype;
    private double version;
    private List<Data> data;

    public String getDeviceid(){ return deviceid; }
    public String getAccountid(){ return accountid; }
    public String getDate(){ return date; }
    public OsInfo getOs_info(){ return os_info; }
    public String getVersioncode(){ return versioncode; }
    public String getChannel(){ return channel; }
    public String getResolution(){ return resolution; }
    public String getAccess(){ return access; }
    public String getIp(){ return ip; }
    public String getCpu(){ return cpu; }
    public String getOperators(){ return operators; }
    public String getNetwork_type(){ return network_type; }
    public String getSubtype(){ return subtype; }
    public double getVersion(){ return version; }
    public List<Data> getData(){ return data; }
}
