package com.spark;

/**
 * Created by hadoop on 17-11-28.
 */
public class Data {
    private double serial_number;
    private String package_name;
    private String pagename;
    private String starttime;
    private String endtime;


    public double getSerialNumber(){ return serial_number;}

    public void setSerialNumber(double serialNumber){ this.serial_number = serialNumber; }

    public String getPackageName(){ return package_name; }

    public void setPackageName(String packageName){ this.package_name = packageName; }

    public String getPageName(){ return pagename; }

    public void setPageName(String pageName){ this.pagename = pageName; }

    public String getStartTime(){ return starttime; }

    public void setStartTime(String startTime){ this.starttime = startTime; }

    public String getEndTime(){ return endtime; }

    public void setEndTime(String endTime){ this.endtime = endTime; }
}
