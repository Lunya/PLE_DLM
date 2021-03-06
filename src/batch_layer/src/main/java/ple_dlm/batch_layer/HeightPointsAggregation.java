package ple_dlm.batch_layer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.github.davidmoten.geo.GeoHash;

import scala.Tuple2;
import scala.Tuple3;

public class HeightPointsAggregation {

	private static final String dem3Path = "/user/lsannic/dem3_lat_lng.med.txt";
	//private static final String dem3Path = "/raw_data/dem3_lat_lng.txt";

	
	public static void aggregate(SparkConf conf, Configuration hbaseConf) {
		//SparkConf conf = new SparkConf().setAppName("Aggregation PLE_DLM");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Done : Lire le fichier texte de 220 GO
		//Clé : NULL
		//Valeur : Ligne du fichier
		JavaRDD<String> rddDEM3File;
		rddDEM3File = context.textFile(dem3Path);
		
		//Done : Obtenir les points à partir de ces lignes
		//Clé : NULL
		//Valeur : Lat / Long / Alt
		JavaRDD<Tuple3<Double, Double, Integer> > rddDEM3 = rddDEM3File.map((s) -> {
			String[] split = s.toString().split(",");
			Double latitude = 0.0;
			Double longitude = 0.0;
			Integer height = 0;
			try {
				latitude = Double.parseDouble(split[0]);
			}
			catch (Exception e) {}
			try {
				longitude = Double.parseDouble(split[1]);
				}
			catch (Exception e) {}
			try {
				height = Integer.parseInt(split[2]);
			}
			catch (Exception e) {}
			return new Tuple3<Double, Double, Integer>(latitude, longitude, height); 
		});
		
		JavaRDD<Tuple3<Double, Double, Integer> > rddDEM3Filtered = rddDEM3.filter((s) -> {
			if (s._1() < -90 || s._1() >= 90 || s._2() < -180 || s._2() >= 180 || s._3() < 0 || s._3() > 9000)
				return false;
			else
				return true;
		});
		
		rddDEM3Filtered = rddDEM3Filtered.cache();
		
		String tableName = HBaseSetup.TABLE_BASENAME;
		
		Connection hbaseConn = null;
		try {
			hbaseConn = ConnectionFactory.createConnection(hbaseConf);
			HBaseSetup.setup(hbaseConn, tableName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
		Table table = null;
		try {
			table = hbaseConn.getTable(TableName.valueOf(tableName));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	RegionLocator regionLocator = null;
		try {
			regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
    	Admin admin = null;
		try {
			admin = hbaseConn.getAdmin();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
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
		
		
		for (int zoom=0; zoom<11; zoom++) {
			Path hPath = new Path("/tmp/DLM_" + zoom);
			//int zoom = 10;
			double latSeparator = (1 * Math.pow(2, zoom));
			double lonSeparator = (2 * Math.pow(2, zoom));
			System.out.println("LatSeparator : " + latSeparator);
			System.out.println("LonSeparator : " + lonSeparator);
			double latStep = 180/latSeparator;
			double lonStep = 360/lonSeparator;
			System.out.println("LatStep : " + latStep);
			System.out.println("LonStep : " + lonStep);
			//Doing : Regrouper les points appartenant à la même région et au même pixel.
			//Clé : Lat/Long (Region) + X/Y (Pixel Local).
			//Valeur : Iterable (Lat / Long / Alt)
			JavaPairRDD<String, Iterable<Tuple3<Double, Double, Integer> > > rddImages = rddDEM3Filtered.groupBy((t) -> {
				double latitude = t._1();
				double longitude = t._2();
				
				latitude +=90;
				longitude +=180;
				
				latitude = Math.floor(latitude/latStep);
				longitude = Math.floor(longitude/lonStep);
				
				double latKey = latitude * latStep;
				double lonKey = longitude * lonStep;
				
				int xKey =  (int) Math.floor(((t._2()+180) - lonKey) * (256.0/lonStep));
				int yKey = (int) Math.floor(((t._1()+90) - latKey) * (256.0/latStep));
				
				latKey -=90;
				lonKey -=180;
				
				String key = Double.toString(latKey) + "#" + Double.toString(lonKey) + "#" + Integer.toString(xKey) + "#" + Integer.toString(yKey);
				return key;
			});
			
			//rddImages.saveAsTextFile("/user/dimprestat/test_alpha_4");
			
			//Done : Il faut réduire l'Iterable de points à un seul Point (surtout : une altitude)
			//Clé : Lat/Long (Region) + X/Y (Pixel Local).
			//Valeur : Lat Long Altitude
			JavaPairRDD<String, Tuple3<Double, Double, Integer>> rddImagesAggregated = rddImages.mapValues((t) -> {
				int max = -1;
				Tuple3<Double, Double, Integer> maxElem = null;
				for (Tuple3<Double, Double, Integer> point : t) {
					if (max < point._3()) {
						max = point._3();
						maxElem = point;
					}
				}
				return maxElem;
			});
			
			//rddImagesAggregated.saveAsTextFile("/user/dimprestat/test_beta_4");
			//Done : On transforme le type de point en changeant le type de clé.
			//Clé : Lat/Long (Region).
			//Valeur : X/Y (Pixel) Altitude
			
			
			JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rddImagesTrans = rddImagesAggregated.mapToPair((t) -> {
				String split[] = t._1().split("#");
				String key = split[0] + "#" + split[1];
				Tuple3<Integer, Integer, Integer> val;// = t._2;
				int x = Integer.parseInt(split[2]); // Split de la Key
				int y = Integer.parseInt(split[3]); // Split de la Key
				int alt = t._2()._3();//Trois
				val = new Tuple3<Integer, Integer, Integer>(x, y, alt);
				return new Tuple2<String, Tuple3<Integer, Integer, Integer>>(key, val);
			});
			
			//Done : On regroupe les pixels en un iterable.
			//Clé : Lat/Long (Region).
			//Valeur : Iterable X/Y (Pixel) Altitude
			JavaPairRDD<String, Iterable<Tuple3<Integer, Integer, Integer>>> rddImagesRegionUnited = rddImagesTrans.groupByKey();
			
			//Todo : On a donc ensuite pour chaque pixel un seul point, il faut ensuite agréger tous les points d'une même région à une même information
			//Clé : Lat/Long (Region)
			//Valeur : Matrice d'altitude (256*256)
			JavaPairRDD<String, byte[]> rddImagesRegionMatrixed = rddImagesRegionUnited.mapValues((t) -> {
				final int imageSize = 256;
				//ByteBuffer res = ByteBuffer.allocateDirect(2 * imageSize * imageSize);
				byte res[] = new byte[2 * imageSize * imageSize];
				for (Tuple3<Integer, Integer, Integer> point : t) {
					int x = point._1();
					int y = point._2();
					int alt = point._3();
					//res.putShort(2*(x*imageSize + y), (short)(alt-Short.MAX_VALUE));
					short val = (short)(alt-Short.MAX_VALUE);
					int pos = 2*(x*imageSize + y);
					res[pos] = (byte)(val & 0xFF);
					res[pos+1] = (byte)((val >> 8) & 0xFF);
				}
				return res;
			});
			
			int patate = zoom;
			
			JavaPairRDD<ImmutableBytesWritable, KeyValue> hbasePuts = rddImagesRegionMatrixed.sortByKey().flatMapToPair((x) -> {
	            ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>> result = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
	            String split[] = x._1().split("#");
	            double lat = Double.valueOf(split[0]);
	            double lon = Double.valueOf(split[1]);
	            String key = GeoHash.encodeHash(lat, lon, 8);
	            result.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
	            		new ImmutableBytesWritable(Bytes.toBytes(x._1 + HBaseSetup.HEIGHT_FAMILY + 0)),
	                    new KeyValue(
	                    		key.getBytes(), // row key
	                    		HBaseSetup.HEIGHT_FAMILY.getBytes(), // column family
	                    		Bytes.toBytes((byte)patate), // column
	                    		x._2()))); // value
	            
	            return result.iterator();
	        });
			hbasePuts.saveAsNewAPIHadoopFile("/tmp/DLM_" + zoom, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, 
					job.getConfiguration());
			LoadIncrementalHFiles loader;
			try {
				loader = new LoadIncrementalHFiles(hbaseConf);
				loader.doBulkLoad(hPath, admin, table, regionLocator);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		//rddImagesRegionMatrixed.saveAsNewAPIHadoopDataset(conf);
		//rddImagesRegionMatrixed.saveAsTextFile("/user/dimprestat/test_gamma_4");
		//context.close();
		
		
		//Todo : Insertion dans hBase en convertissant la matrice en une autre structure de donnée.
		
		//rddImages.aggregateByKey(zeroValue, seqFunc, combFunc)
		
		
		
		
		
		//JavaDoubleRDD heights = rddDEM3.mapToDouble(t -> t._3()).cache();
		
		//long startTime, endTime;
		
	}
}