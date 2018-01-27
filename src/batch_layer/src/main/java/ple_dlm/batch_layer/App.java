package ple_dlm.batch_layer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;

public class App 
{
    public static void main( String[] args )
    {
    	SparkConf sparkConf = new SparkConf().setAppName("PLE_DLM batch layer");
    	Configuration hbaseConf = HBaseConfiguration.create();
        PointsToHBase pthb = new PointsToHBase(
        		sparkConf, hbaseConf,
        		"/user/lsannic/simple-gps-points.min.txt", "dlm");
        pthb.execute();
    }
}
