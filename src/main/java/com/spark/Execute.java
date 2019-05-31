package com.spark;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-12-1.
 */
public class Execute {
    public static void main(String[] args)
    {
        Kmeans k = new Kmeans(2);//初始化一个Kmean对象，将k置为3
        OtherKmeans otherKmeans = new OtherKmeans(2);
        MyKmeans myKmeans = new MyKmeans(2);
        MyKmeansNew myKmeansNew = new MyKmeansNew(2);

        ArrayList<float[]> dataSet=new ArrayList<>();

        List<String> fileInputStringList = new ArrayList<>();

        for (int i = 0 ; i <= 5 ; i++) {
            try {
                FileReader fr = new FileReader("/home/hadoop/桌面/KMeansDataSet-/part-000"+i);
                BufferedReader br = new BufferedReader(fr);
                String line = "";
//                int count = 0;
                while ((line = br.readLine()) != null) {
                    fileInputStringList.add(line);
//                    count++;
//                    if (count > 1500)
//                    {
//                        break;
//                    }
                }
                br.close();
                fr.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        //System.out.println("==================="+fileInputStringList.size()+"=======================");

        for (String s : fileInputStringList)
        {
            dataSet.add(new float[]{(float) Double.parseDouble(s), (float) Double.parseDouble(s)});
        }

        //设置原始数据集
        k.setDataSet(dataSet);
        //执行算法
        k.execute();
        //得到聚类结果
        ArrayList<ArrayList<float[]>> cluster=k.getCluster();
        //查看结果
        /*
        for(int i=0;i<cluster.size();i++)
        {
            k.printDataArray(cluster.get(i), "cluster["+i+"]");
        }
        */

        //JFChart.displayData("聚类效果图",cluster);

        System.out.println("============="+k.getJC()+"===============");
        System.out.println("=============Finished===============");
        System.out.println();

        
        //设置原始数据集
        otherKmeans.setDataSet(dataSet);
        //执行算法
        otherKmeans.execute();
        //得到聚类结果
        ArrayList<ArrayList<float[]>> Cluster=otherKmeans.getCluster();
        //查看结果
        /*
        for(int i=0;i<cluster.size();i++)
        {
            k.printDataArray(cluster.get(i), "cluster["+i+"]");
        }
        */

        //JFChart.displayData("聚类效果图",Cluster);

        System.out.println("============="+otherKmeans.getJC()+"===============");
        System.out.println("=============Finished===============");
        System.out.println();


        //设置原始数据集
        myKmeans.setDataSet(dataSet);
        //执行算法
        myKmeans.execute();
        //得到聚类结果
        ArrayList<ArrayList<float[]>> myCluster=otherKmeans.getCluster();
        //查看结果
        /*
        for(int i=0;i<cluster.size();i++)
        {
            k.printDataArray(cluster.get(i), "cluster["+i+"]");
        }
        */

        //JFChart.displayData("聚类效果图",myCluster);

        System.out.println("============="+myKmeans.getJC()+"===============");
        System.out.println("=============Finished===============");
        System.out.println();


        //设置原始数据集
        myKmeansNew.setDataSet(dataSet);
        //执行算法
        myKmeansNew.execute();
        //得到聚类结果
        ArrayList<ArrayList<float[]>> myClusterNew=otherKmeans.getCluster();
        //查看结果
        /*
        for(int i=0;i<cluster.size();i++)
        {
            k.printDataArray(cluster.get(i), "cluster["+i+"]");
        }
        */

        //JFChart.displayData("聚类效果图",myCluster);

        System.out.println("============="+myKmeansNew.getJC()+"===============");
        System.out.println("=============Finished===============");

        ArrayList<Integer> countList = myKmeansNew.getCountList();
        int m = myKmeansNew.getM();
        List<Integer> errorStringList = new ArrayList<>();
        for( int i=0;i<countList.size();i++)
        {
            if (countList.get(i) > m)
            {
                errorStringList.add(i);
            }
        }

        System.out.println(errorStringList);


        //测试
        MyKmeansNew myKmeansNewTest = new MyKmeansNew(3);
        //设置原始数据集
        myKmeansNewTest.setDataSet(dataSet);
        //执行算法
        myKmeansNewTest.execute();

        ArrayList<Integer> countListTest = myKmeansNewTest.getCountList();
        int mTest = myKmeansNewTest.getM();
        List<Integer> errorStringListTest = new ArrayList<>();
        for( int i=0;i<countListTest.size();i++)
        {
            if (countListTest.get(i) > mTest)
            {
                errorStringListTest.add(i);
            }
        }

        int errorCount = 0;
        for(int i=0;i<errorStringListTest.size();i++)
        {
            if(errorStringList.contains(errorStringListTest.get(i)))
            {
                errorCount++;
            }
        }

        System.out.println("=================="+errorCount/errorStringList.size()+"===================");




    }
}
