package com.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Created by hadoop on 17-11-29.
 */
public class PageNameInput {
    private String pageName;

    public void setPageName(String pageName)
    {
        this.pageName = pageName;
    }

    public String getPageName()
    {
        return pageName;
    }

    public Row toRow()
    {
        return RowFactory.create(pageName);
    }

}
