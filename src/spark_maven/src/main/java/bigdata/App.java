package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.spark.SparkConf;

public class App {
	public static void main(String[] args) {
		final String tableName = "DLM";

		SparkConf sparkConf = new SparkConf().setAppName("PLE_DLM batch layer");
		Configuration hbaseConf = null;
		
		try {
			hbaseConf = HBaseConfiguration.create();
			System.out.println(hbaseConf.get("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily"));
			hbaseConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "2048");
			System.out.println(hbaseConf.get("hbase.hstore.blockingStoreFiles"));
			hbaseConf.set("hbase.zookeeper.quorum", "beetlejuice");
			HBaseAdmin.checkHBaseAvailable(hbaseConf);
			System.out.println("HBase is running");
		} catch (Exception e) {
			e.printStackTrace();
		}

		FilesJoint.joint(sparkConf, hbaseConf);
	}
}
