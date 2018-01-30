package ple_dlm.batch_layer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class App 
{
    public static void main( String[] args )
    {
    	final String tableName = "DLM";
    	
    	SparkConf sparkConf = new SparkConf().setAppName("PLE_DLM batch layer");
    	Configuration hbaseConf = null;
    	try {
	    	hbaseConf = HBaseConfiguration.create();
	    	System.out.println(hbaseConf.get("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily"));
	    	hbaseConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "2048");
	    	System.out.println(hbaseConf.get("hbase.hstore.blockingStoreFiles"));
	    	hbaseConf.set("hbase.zookeeper.quorum", "beetlejuice");
	    	//hbaseConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
	    	HBaseAdmin.checkHBaseAvailable(hbaseConf);
	    	System.out.println("HBase is running");
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	/*
    	JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    	
    	Connection hbaseConn = null;
		try {
			hbaseConn = ConnectionFactory.createConnection(hbaseConf);
			HBaseSetup.setup(hbaseConn, tableName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	
    	// TEST
    	TableName HtableName = TableName.valueOf(tableName);
    	Table table = null;
		try {
			table = hbaseConn.getTable(HtableName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	RegionLocator regionLocator = null;
		try {
			regionLocator = hbaseConn.getRegionLocator(HtableName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	Path hPath = new Path("/tmp/lsannic");
    	Admin admin = null;
		try {
			admin = hbaseConn.getAdmin();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    	
    	Job job = null;
		try {
			job = Job.getInstance(hbaseConf);
			job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
			job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			System.out.println("Job created");
		} catch (IOException e) {
			System.out.println("Job not created");
			e.printStackTrace();
		}
		
    	job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    	job.setMapOutputValueClass(KeyValue.class);
    	try {
			HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	List<Tuple2<Integer, Tuple3<Double, Double, Double>>> list = Arrays.asList(
    			new Tuple2<Integer, Tuple3<Double, Double, Double>>(
    					1, new Tuple3<Double, Double, Double>(1.0, 2.0, 3.0)),
    			new Tuple2<Integer, Tuple3<Double, Double, Double>>(
    					2, new Tuple3<Double, Double, Double>(2.0, 3.0, 4.0)),
    			new Tuple2<Integer, Tuple3<Double, Double, Double>>(
    					3, new Tuple3<Double, Double, Double>(3.0, 4.0, 5.0)),
    			new Tuple2<Integer, Tuple3<Double, Double, Double>>(
    					4, new Tuple3<Double, Double, Double>(4.0, 5.0, 6.0)),
    			new Tuple2<Integer, Tuple3<Double, Double, Double>>(
    					5, new Tuple3<Double, Double, Double>(5.0, 6.0, 7.0)));
    	
    	JavaPairRDD<Integer, Tuple3<Double, Double, Double>> heightWorldData = jsc.parallelizePairs(list);
    	
    	JavaPairRDD<ImmutableBytesWritable, KeyValue> hbasePuts = heightWorldData.sortByKey().flatMapToPair((x) -> {
    	            ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>> result = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
    	            
    	            result.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
    	            		new ImmutableBytesWritable(Bytes.toBytes(x._1 + HBaseSetup.HEIGHT_FAMILY + 0)),
    	                    new KeyValue(Bytes.toBytes(x._1), HBaseSetup.HEIGHT_FAMILY.getBytes(), HBaseSetup.LATITUDE_COL.getBytes(), Bytes.toBytes(x._2._2()))));
    	            
    	            result.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
    	            		new ImmutableBytesWritable(Bytes.toBytes(x._1 + HBaseSetup.HEIGHT_FAMILY + 1)), 
    	                    new KeyValue(Bytes.toBytes(x._1), HBaseSetup.HEIGHT_FAMILY.getBytes(), HBaseSetup.HEIGHT_COL.getBytes(), Bytes.toBytes(x._2._1()))));
    	            
    	            result.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
    	            		new ImmutableBytesWritable(Bytes.toBytes(x._1 + HBaseSetup.HEIGHT_FAMILY + 2)),
    	                    new KeyValue(Bytes.toBytes(x._1), HBaseSetup.HEIGHT_FAMILY.getBytes(), HBaseSetup.LONGITUDE_COL.getBytes(), Bytes.toBytes(x._2._3()))));
    	            
    	            return result.iterator();
    	        });
    	hbasePuts.saveAsNewAPIHadoopFile("/tmp/lsannic", ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, job.getConfiguration());
        LoadIncrementalHFiles loader;
		try {
			loader = new LoadIncrementalHFiles(hbaseConf);
			loader.doBulkLoad(hPath, admin, table, regionLocator);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		HeightPointsAggregationBis.aggregate(sparkConf, hbaseConf);
    	
        /*PointsToHBase pthb = new PointsToHBase(
        		sparkConf, hbaseConf,
        		"/user/lsannic/simple-gps-points.min.txt", "dlm");
        pthb.execute();*/
    }
}
