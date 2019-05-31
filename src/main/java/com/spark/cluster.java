package com.spark;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by hadoop on 17-11-23.
 */
public class cluster {
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName("Cluster").setMaster("spark://master:7077").set("spark.executor.memory", "4g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("ERROR");
        HiveContext hiveContext = new HiveContext(javaSparkContext);


        Dataset<Row> rowDataset1 = hiveContext.sql("select * from pagename_20170912");
        Dataset<Row> rowDataset2 = hiveContext.sql("select * from pagename_20170913");
        Dataset<Row> rowDataset3 = hiveContext.sql("select * from pagename_20170925");
        Dataset<Row> rowDataset4 = hiveContext.sql("select * from pagename_20170926");
        Dataset<Row> rowDataset5 = hiveContext.sql("select * from pagename_20170927");
        Dataset<Row> rowDataset6 = hiveContext.sql("select * from pagename_20170928");
        Dataset<Row> rowDataset7 = hiveContext.sql("select * from pagename_20170929");
        Dataset<Row> rowDataset8 = hiveContext.sql("select * from pagename_20170930");

        Dataset<Row> rowDataset = rowDataset1.union(rowDataset2).union(rowDataset3).union(rowDataset4).union(rowDataset5).union(rowDataset6).union(rowDataset7).union(rowDataset8);


        JavaRDD<Row> rowJavaRDD = rowDataset.toJavaRDD();

        JavaRDD<String> stringJavaRDD = rowJavaRDD.map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        });

        System.out.println();
        System.out.println();
        System.out.println();


        //System.out.println("================="+stringJavaRDD.count()+"====================");//查看总共有多少条数据
        //stringJavaRDD.saveAsTextFile("file:///home/hadoop/桌面/OutPut");

        JavaRDD<String> filterStringJavaRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                Gson gson = new Gson();
                PageName pageName = new PageName();
                Type type = new TypeToken<PageName>(){}.getType();
                pageName = gson.fromJson(s,type);

                for (String str : pageName.getPageName())
                {
                    if (str.equals("com.tencent.mm.plugin.luckymoney.ui.LuckyMoneyDetailUI"))
                    {
                        return true;
                    }
                }

                return false;
            }
        });


        //System.out.println("================="+filterStringJavaRDD.count()+"====================");//查看筛选后还有多少条数据

        JavaPairRDD<String,Integer> countAction = filterStringJavaRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String,Integer>> list = new ArrayList<>();

                Gson gson = new Gson();
                PageName pageName = new PageName();
                Type type = new TypeToken<PageName>(){}.getType();
                pageName = gson.fromJson(s,type);

                for (String str : pageName.getPageName())
                {
                    list.add(new Tuple2<>(str,1));
                }

                return list.iterator();
            }
        });

        //System.out.println("================="+countAction.count()+"====================");//查看行为总数

        final long actionNum = countAction.count();//行为总数

        JavaPairRDD<String,Double> actionWeightValue = countAction.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).mapValues(new Function<Integer, Double>() {
            @Override
            public Double call(Integer integer) throws Exception {
                return integer*1.0/actionNum;
            }
        });

        /*
        System.out.println("================="+actionWeightValue.count()+"====================");//查看行为权重总数

        for (Tuple2<String,Double> s : actionWeightValue.collect())
        {
            System.out.println(s);
            break;
        }
        */

        final List<Tuple2<String,Double>> weightValueList = actionWeightValue.collect();//行为权重链表

        JavaPairRDD<String,Double> recordWeightValue = filterStringJavaRDD.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                double weightValue = 0.0;

                Gson gson = new Gson();
                PageName pageName = new PageName();
                Type type = new TypeToken<PageName>(){}.getType();
                pageName = gson.fromJson(s,type);

                for (String str : pageName.getPageName())
                {
                    for ( Tuple2<String,Double> tuple2 : weightValueList )
                    {
                        if (tuple2._1().equals(str))
                        {
                            weightValue += tuple2._2();
                            break;
                        }
                    }
                }

                weightValue = weightValue/pageName.getPageName().size();

                return new Tuple2<>(s,weightValue);
            }
        });

        //recordWeightValue.values().saveAsTextFile("file:///home/hadoop/桌面/RecordWeightValue");

        //System.out.println("================="+recordWeightValue.count()+"====================");//查看记录权重总数

        /*
        int i = 0;

        for (double s : recordWeightValue.values().collect())
        {
            System.out.println(s);
            i++;
            if (i == 10) {
                break;
            }
        }
        */


        recordWeightValue.values().saveAsTextFile("file:///home/hadoop/桌面/KMeansDataSet-");

        /*
        ArrayList<float[]> KMeansDataSet=new ArrayList<>();

        for (double s : recordWeightValue.values().collect())
        {
            KMeansDataSet.add(new float[]{(float) s,(float) s});
        }

        */






    }
}
