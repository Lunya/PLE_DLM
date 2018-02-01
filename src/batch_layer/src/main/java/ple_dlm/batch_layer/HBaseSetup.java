package ple_dlm.batch_layer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;

public class HBaseSetup {
	public static final String TABLE_BASENAME = "dlm";
	public static final String HEIGHT_FAMILY = "h";
	public static final String INTERESTING_POINT_FAMILY = "p";
	public static final String CITIES_FAMILY = "c";
	
	public static final String COUNTRY_COL = "C";
	public static final String CITY_COL = "c";
	public static final String POPULATION_COL = "p";

	public static final String LATITUDE_COL = "a";
	public static final String LONGITUDE_COL = "o";
	public static final String HEIGHT_COL = "h";
	public static final String ZOOM_COL = "z";

	private static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}

	private static void createHeightColumnFamily(HTableDescriptor table) {
		HColumnDescriptor columnFamily = new HColumnDescriptor((HEIGHT_FAMILY).getBytes());
		table.addFamily(columnFamily);
	}

	private static void createInterestingPointColumnFamily(HTableDescriptor table) {
		HColumnDescriptor columnFamily = new HColumnDescriptor((INTERESTING_POINT_FAMILY).getBytes());
		table.addFamily(columnFamily);
	}

	public static void setup(
			Connection connection,
			String tableName) throws IOException {
		final Admin admin = connection.getAdmin();
		
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		createHeightColumnFamily(table);
		createInterestingPointColumnFamily(table);
		createOrOverwrite(admin, table);
	}
	
	public static void saveRDD(
			Configuration hbaseConf,
			String tableName,
			JavaPairRDD<ImmutableBytesWritable, KeyValue> hbasePuts,
			String nonce) throws IOException, Exception {
		final String path = "/tmp/DLM" + nonce;
		hbasePuts.saveAsNewAPIHadoopFile(path, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, 
				Job.getInstance(hbaseConf).getConfiguration());
		Connection hbaseConn = ConnectionFactory.createConnection(hbaseConf);
		HBaseSetup.setup(hbaseConn, tableName);
		Table table = hbaseConn.getTable(TableName.valueOf(tableName));
		RegionLocator regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName));
		Admin admin = hbaseConn.getAdmin();
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
		loader.doBulkLoad(new Path(path), admin, table, regionLocator);
	}
}
