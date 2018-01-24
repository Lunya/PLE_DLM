package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Row;
//import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class SparkConnectHBase {

	public static void main(String[] args) throws Exception {
		// create connection with HBase
		Configuration config = null;
		try {
		       config = HBaseConfiguration.create();
		       config.set("hbase.zookeeper.quorum", "127.0.0.1");
		       //config.set("hbase.zookeeper.root", "/hbase");
		       config.set("hbase.zookeeper.property.clientPort","2181");
		       //config.set("hbase.master", "127.0.0.1:60000");
		       HBaseAdmin.checkHBaseAvailable(config);
		       System.out.println("HBase is running!");
		     } 
		catch (MasterNotRunningException e) {
		            System.out.println("HBase is not running!");
		            System.exit(1);
		}catch (Exception ce){ 
		        ce.printStackTrace();
		}
		 
		config.set(TableInputFormat.INPUT_TABLE, "DLM");
		 
		// new Hadoop API configuration
		Job newAPIJobConfiguration1 = Job.getInstance(config);
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "DLM");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		 
		// create Key, Value pair to store in HBase
		  /*JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = JavaRDD.mapToPair(
		      new PairFunction<Row, ImmutableBytesWritable, Put>() {
		    @Override
		    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
		         
		       Put put = new Put(Bytes.toBytes(row.getString(0)));
		       put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier1"), Bytes.toBytes(row.getString(1)));
		       put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier2"), Bytes.toBytes(row.getString(2)));
		     
		           return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
		    }
		     });*/
		
		/*
		JavaRDD<Tuple3<String, String, String>> rdd = null;
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd.mapToPair((row) -> {
			       Put put = new Put(Bytes.toBytes(row._1()));
			       put.addColumn(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier1"), Bytes.toBytes(row._2()));
			       put.addColumn(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier2"), Bytes.toBytes(row._3()));
			     
			           return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
			    });
		*/
		     
		     // save to HBase- Spark built-in API method
		    // hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
	}
	
}
