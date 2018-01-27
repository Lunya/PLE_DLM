package ple_dlm.batch_layer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.spark.SparkConf;

public class App 
{
    public static void main( String[] args )
    {
    	SparkConf sparkConf = new SparkConf().setAppName("PLE_DLM batch layer");
    	Configuration hbaseConf = null;
    	try {
	    	hbaseConf = HBaseConfiguration.create();
	    	hbaseConf.set("hbase.zookeeper.quorum", "localhost");
	    	//hbaseConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
	    	HBaseAdmin.checkHBaseAvailable(hbaseConf);
	    	System.out.println("HBase is running");
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
        PointsToHBase pthb = new PointsToHBase(
        		sparkConf, hbaseConf,
        		"/user/lsannic/simple-gps-points.min.txt", "dlm");
        pthb.execute();
    }
}
