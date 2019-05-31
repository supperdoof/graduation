package com.spark;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RefineryUtilities;

import java.util.ArrayList;


/**
 * Created by hadoop on 17-11-27.
 */
public class JFChart {
    public static void displayData(String title,ArrayList<ArrayList<float[]>> dataArray)
    {
        XYSeriesCollection xySeriesCollection = new XYSeriesCollection();

        for(int i=0;i<dataArray.size();i++)
        {
            XYSeries xySeries = new XYSeries(i+1);
            for (int j=0;j<dataArray.get(i).size();j++ ) {
                xySeries.add(Double.parseDouble("" + dataArray.get(i).get(j)[0] + ""), Double.parseDouble("" + dataArray.get(i).get(j)[1] + ""));
                xySeriesCollection.addSeries(xySeries);

                if ( j == 30)//避免循环次数过多，可注释
                {
                    break;
                }

            }
        }

        final JFreeChart chart =ChartFactory.createScatterPlot(title,"","",xySeriesCollection,PlotOrientation.VERTICAL,false,false,false);


        ChartFrame frame = new ChartFrame(title,chart);
        frame.pack();//确定frame的最佳大小
        RefineryUtilities.centerFrameOnScreen(frame);
        frame.setVisible(true);
    }

    /**
    public static void main(String[] args)
    {
        float[] floatOne = {1.0f,2.0f};
        float[] floatTwo= {3.0f,3.0f};
        float[] floatThree= {2.0f,4.0f};

        ArrayList<float[]> arrayList = new ArrayList<float[]>();
        arrayList.add(floatOne);
        arrayList.add(floatTwo);
        arrayList.add(floatThree);

        ArrayList<ArrayList<float[]>> dataArray = new ArrayList<ArrayList<float[]>>();
        dataArray.add(arrayList);

        JFChart.displayData("散点图",dataArray);

    }
     */

}
