package com.spark;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by hadoop on 17-11-28.
 * 用于数据录入Hive
 */
public class DataInput {
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName("DataInput").setMaster("spark://master:7077").set("spark.executor.memory", "4g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("ERROR");
        HiveContext hiveContext = new HiveContext(javaSparkContext);

        JavaRDD<String> dataSet1 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/1.txt");
        JavaRDD<String> dataSet2 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/2.txt");
        JavaRDD<String> dataSet3 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/3.txt");
        JavaRDD<String> dataSet4 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/4.txt");
        JavaRDD<String> dataSet5 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/5.txt");
        JavaRDD<String> dataSet6 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/6.txt");
        JavaRDD<String> dataSet7 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/7.txt");
        JavaRDD<String> dataSet8 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/8.txt");
        JavaRDD<String> dataSet9 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/9.txt");
        JavaRDD<String> dataSet10 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/10.txt");
        JavaRDD<String> dataSet11 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/11.txt");
        JavaRDD<String> dataSet12 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/12.txt");
        JavaRDD<String> dataSet13 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/13.txt");
        JavaRDD<String> dataSet14 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/14.txt");
        JavaRDD<String> dataSet15 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/15.txt");
        //JavaRDD<String> dataSet16 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170927/16.txt");

        //JavaRDD<String> dataSet = dataSet1.union(dataSet2).union(dataSet3).union(dataSet4).union(dataSet5).union(dataSet6).union(dataSet7).union(dataSet8).union(dataSet9).union(dataSet10).union(dataSet11).union(dataSet12).union(dataSet13).union(dataSet14);
        JavaRDD<String> dataSet = dataSet1.union(dataSet2).union(dataSet3).union(dataSet4).union(dataSet5).union(dataSet6).union(dataSet7).union(dataSet8).union(dataSet9).union(dataSet10).union(dataSet11).union(dataSet12).union(dataSet13).union(dataSet14).union(dataSet15);
        //JavaRDD<String> dataSet = dataSet1.union(dataSet2).union(dataSet3).union(dataSet4).union(dataSet5).union(dataSet6).union(dataSet7).union(dataSet8).union(dataSet9).union(dataSet10).union(dataSet11).union(dataSet12).union(dataSet13).union(dataSet14).union(dataSet15).union(dataSet16);

        System.out.println();
        System.out.println();
        System.out.println();

        JavaRDD<String> mapDataSet = dataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<>();
                try {
                    Gson gson = new Gson();
                    Information information = new Information();
                    Type type = new TypeToken<Information>() {}.getType();
                    information = gson.fromJson(s,type);
                    String pageName = new String("{\"pageName\":[");
                    for (Data data : information.getData())
                    {
                        pageName += ("\""+data.getPageName()+"\""+",");
                    }
                    int pageNameLength = pageName.length();
                    pageName = pageName.substring(0,pageNameLength-1)+"]}";
                    list.add(pageName);
                }catch (Exception e)
                {
                    e.printStackTrace();
                }

                return list.iterator();
            }
        });

        //System.out.println("======================"+mapDataSet.count()+"==========================");


        hiveContext.sql("use default");

        JavaRDD<Row> rowMapDataSet = mapDataSet.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                PageNameInput pageNameInput = new PageNameInput();
                pageNameInput.setPageName(s);
                return pageNameInput.toRow();
            }
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("pageName",DataTypes.StringType,true));

        StructType structType = DataTypes.createStructType(fields);

        DataFrameWriter dataFrameWriter = hiveContext.createDataFrame(rowMapDataSet,structType).write();
        dataFrameWriter.mode(SaveMode.Append).saveAsTable("pagename_20170930");
        System.out.println("Load Data Finished!!!!!");



    }

}
