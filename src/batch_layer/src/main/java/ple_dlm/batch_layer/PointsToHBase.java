package ple_dlm.batch_layer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.github.davidmoten.geo.GeoHash;

import scala.Tuple2;

public class PointsToHBase {
	private final byte[] POINTS_FAMILY = Bytes.toBytes("p");
	
	private SparkConf sparkConf;
	private Configuration hbaseConf;
	private String filePath;
	private String tableName;
	
	public PointsToHBase(
			SparkConf sparkConf, Configuration hbaseConf,
			String filePath, String tableName) {
		this.sparkConf = sparkConf;
		this.hbaseConf = hbaseConf;
		this.filePath = filePath;
		this.tableName = tableName;
	}
	
	private void createTable() {
		try {
			Configuration hbaseConf = HBaseConfiguration.create();
			
			Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
			final Admin admin = hbaseConnection.getAdmin();
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			
			HColumnDescriptor pointsFamily = new HColumnDescriptor(POINTS_FAMILY);
			tableDescriptor.addFamily(pointsFamily);
			
			if (admin.tableExists(tableDescriptor.getTableName())) {
				admin.disableTable(tableDescriptor.getTableName());
				admin.deleteTable(tableDescriptor.getTableName());
			}
			admin.createTable(tableDescriptor);
			admin.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void execute() {
		// read file in spark rdd
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> file = sparkContext.textFile(filePath);
		
		// some computations on rdd
		JavaRDD<Tuple2<Double, Double>> points = file.sample(false, 0.001).map((line) -> {
			String values[] = line.split(",");
			Tuple2<Double, Double> res;
			try {
				res = new Tuple2<>(
						Double.parseDouble(values[0]),
						Double.parseDouble(values[1]));
			} catch (Exception e) {
				System.out.println("Parsing {" + line + "} failed");
				res = new Tuple2<>(0.0, 0.0);
			}
			return res;
		});
		//JavaRDD<> filteredPoints = points.reduce(f);

		// put rdd on hbase
		/*long nbPoints = points.count();
		System.out.println("Total number of points: " + nbPoints);*/
		createTable();
		JavaHBaseContext hbaseContext = new JavaHBaseContext(sparkContext, hbaseConf);
		hbaseContext.bulkPut(
				points, TableName.valueOf(tableName),
				(point) -> {
					Put row = new Put(Bytes.toBytes(
							GeoHash.encodeHash(
									point._1(),
									point._2(),
									6)));
					row.addColumn(Bytes.toBytes("p"),
							Bytes.toBytes("p"),
							Bytes.toBytes((int)(point._1() + point._2())));
					return row;
				});
	}
}
