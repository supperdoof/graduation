package com.spark;

import java.util.List;

/**
 * Created by hadoop on 17-11-29.
 * 用于解析Hive中查询出来的JSON字符串（pageName)
 */
public class PageName {
    private List<String> pageName;

    public List<String> getPageName()
    {
        return pageName;
    }

    public void setPageName(List<String> pageName)
    {
        this.pageName = pageName;
    }

}
