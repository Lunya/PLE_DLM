package ple_dlm.batch_layer;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
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
import org.apache.spark.input.PortableDataStream;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;

import scala.Tuple2;
import scala.Tuple3;

public class HeightPointsAggregationBis {

	//private static final String dem3Path = "/user/lsannic/dem3_lat_lng.med.txt";
	//private static final String dem3Path = "/raw_data/dem3_lat_lng.txt";
	private static final String dem3Path = "/raw_data/dem3_raw";
	//private static final String dem3Path = "/user/lsannic/dem3_raw";

	public static void aggregate(SparkConf conf, Configuration hbaseConf) {
		//SparkConf conf = new SparkConf().setAppName("Aggregation PLE_DLM");
		JavaSparkContext context = new JavaSparkContext(conf);
		//Done : Lire le fichier texte de 220 GO
		//Clé : NULL
		//Valeur : Ligne du fichier
		/*JavaRDD<String> rddDEM3File;
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
		});*/
		
		JavaPairRDD<String, PortableDataStream> dem3_files = context.binaryFiles(dem3Path);

		/*
		 * Read binary files to pair of lat;long coordinates and height value
		 */
		JavaPairRDD<Tuple2<Double, Double>, Short> points = dem3_files.flatMapToPair((file) -> {
			final int srtm_ver = 1201;
			final double latStep = 1.0 / (double)srtm_ver;
			final double lngStep = .001 / (double)srtm_ver;
			String filename = file._1();
			String filen = filename.substring(filename.length() - 11);
			ByteBuffer buffer = ByteBuffer.wrap(file._2().toArray());//DataInputStream stream = file._2().open();
			ArrayList<Tuple2<Tuple2<Double, Double>, Short>> result = new ArrayList<>();
			
			double lat = Double.parseDouble(filen.substring(1, 3));
			double lng = Double.parseDouble(filen.substring(4, 7));
			if (filen.charAt(0) == 'S' || filen.charAt(0) == 's') lat *= -1;
			if (filen.charAt(0) == 'W' || filen.charAt(0) == 'w') lng *= -1;
			
			for (int i = 0; i < srtm_ver; ++i ) {
				for (int j = 0; j < srtm_ver; ++j ) {
					//final short height = stream.readShort();
					final short height = buffer.getShort();
					if (height > 0) {
						// compute latitude and longitude
						double latitude = lat + ((double)i * latStep);
						double longitude = lng + ((double)j * lngStep);
						
						result.add(new Tuple2<Tuple2<Double, Double>, Short>(
							new Tuple2<Double, Double>(latitude, longitude), height));
					}
				}
			}
			return result.iterator();
		});

		JavaRDD<Tuple3<Double, Double, Integer> > rddDEM3Filtered = points.map((point) -> {
			return new Tuple3<Double, Double, Integer>(
					point._1()._1(),
					point._1()._2(),
					(int)point._2());
		}).cache();

		String tableName = HBaseSetup.TABLE_BASENAME+"_Full";

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


		
		//int zoom = 10;

		//Doing : Regrouper les points appartenant à la même région et au même pixel.
		//Clé : Lat/Long (Region) + X/Y (Pixel Local).
		//Valeur : Iterable (Lat / Long / Alt)
		short maxZoom = 10;
		Path hPath = new Path("/tmp/DLM"+maxZoom);
		double latSeparator = (1 * Math.pow(2, maxZoom));
		double lonSeparator = (2 * Math.pow(2, maxZoom));
		final double latStep = 180/latSeparator;
		final double lonStep = 360/lonSeparator;
		
		JavaPairRDD<String, Iterable<Integer>> rddImages = rddDEM3Filtered.flatMapToPair((t) -> {
			ArrayList<Tuple2<String, Integer>> set = new ArrayList<Tuple2<String, Integer>>();
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
			String key = Double.toString(latKey) + "#" + Double.toString(lonKey) + "#" + Integer.toString(maxZoom) + "#" + Integer.toString(xKey) + "#" + Integer.toString(yKey);
			set.add(new Tuple2<String, Integer>(key, t._3()));

			return set.iterator();
		}).groupByKey();

		//rddImages.saveAsTextFile("/user/dimprestat/test_alpha_4");

		//Done : Il faut réduire l'Iterable de points à un seul Point (surtout : une altitude)
		//Clé : Lat/Long (Region) + X/Y (Pixel Local).
		//Valeur : Lat Long Altitude
		JavaPairRDD<String, Integer> rddImagesAggregated = rddImages.mapValues((t) -> {
			int max = -1;
			for (int alt : t) {
				if (max < alt) {
					max = alt;
				}
			}
			return max;
		});

		//rddImagesAggregated.saveAsTextFile("/user/dimprestat/test_beta_4");
		//Done : On transforme le type de point en changeant le type de clé.
		//Clé : Lat/Long (Region).
		//Valeur : X/Y (Pixel) Altitude


		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rddImagesTrans = rddImagesAggregated.mapToPair((t) -> {
			String split[] = t._1().split("#");
			double lat = Double.parseDouble(split[0]);
			double lon = Double.parseDouble(split[1]);
			String geo = GeoHash.encodeHash(lat, lon, 8);
			String key = geo + "#" + split[2];
			Tuple3<Integer, Integer, Integer> val;// = t._2;
			int x = Integer.parseInt(split[3]); // Split de la Key
			int y = Integer.parseInt(split[4]); // Split de la Key
			int alt = t._2();//Trois
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
		
		JavaPairRDD<ImmutableBytesWritable, KeyValue> hbasePuts = rddImagesRegionMatrixed.sortByKey().flatMapToPair((x) -> {
			ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>> result = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
			String split[] = x._1().split("#");
			String key = split[0];
			String zoom = split[1];
			result.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
					new ImmutableBytesWritable(Bytes.toBytes(x._1 + HBaseSetup.HEIGHT_FAMILY + 0)),
					new KeyValue(
							key.getBytes(), // row key
							HBaseSetup.HEIGHT_FAMILY.getBytes(), // column family
							zoom.getBytes(), // column
							x._2()))); // value

			return result.iterator();
		});
		
		hbasePuts.saveAsNewAPIHadoopFile("/tmp/DLM"+maxZoom, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, 
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
		
		for (short i=9; i>= 0; i--) {
			short patate = i;
			latSeparator = (1 * Math.pow(2, i));
			lonSeparator = (2 * Math.pow(2, i));
			double zoomedLatStep = 180/latSeparator;
			double zoomedlonStep = 360/lonSeparator;
			JavaPairRDD<String, Iterable< Tuple2<Tuple2<Double, Double>, byte[]>>> test = rddImagesRegionMatrixed.mapToPair((t) -> {
				String split[] = t._1().split("#");
				LatLong ll = GeoHash.decodeHash(split[0]);
				double lat = ll.getLat();
				double lon = ll.getLon();
				Tuple2<Double, Double> pos = new Tuple2<Double, Double>(lat, lon);
				Tuple2<Tuple2<Double, Double>, byte[]> val = new Tuple2<Tuple2<Double, Double>, byte[]>(pos, t._2());
				
				lat +=90;
				lon +=180;
				lat = Math.floor(lat/zoomedLatStep);
				lon = Math.floor(lon/zoomedlonStep);

				double latKey = lat * zoomedLatStep;
				double lonKey = lon * zoomedlonStep;
				latKey -=90;
				lonKey -=180;
				String key = GeoHash.encodeHash(latKey, lonKey, 8) + "#" + patate;
				Tuple2<String, Tuple2<Tuple2<Double, Double>, byte[]>> res = new Tuple2<String, Tuple2<Tuple2<Double, Double>, byte[]>>(key, val);
				return res;
			}).groupByKey();
			
			rddImagesRegionMatrixed = test.mapToPair((t) -> {
				byte res[] = new byte[2 * 256 * 256];
				for (Tuple2<Tuple2<Double, Double>, byte[]> region : t._2()) {
					final String split[] = t._1().split("#");
					final LatLong ll = GeoHash.decodeHash(split[0]);
					final double newLat = ll.getLat();
					final double newLon = ll.getLon();
					final byte[] reg = region._2();
					
					int latRegion = 0;
					int lngRegion = 0;
					if (region._1()._1() != newLat)
						latRegion = 1;
					if (region._1()._2() != newLon)
						lngRegion = 1;
					
					for (int lat = 0; lat < 128; ++lat) {
						for (int lng = 0; lng < 128; ++lng) {
							final int Lat = lat*2;
							final int Lng = lng*2;
							res[(lat + 128*latRegion)*256 + (lng + 128*lngRegion)] =
									(byte) Math.max(reg[Lat*256 + Lng],
									Math.max(reg[Lat*256 + Lng+1],
									Math.max(reg[(Lat+1)*256 + Lng],
									reg[(Lat+1)*256 + Lng+1])));
						}
					}
				}
				return new Tuple2<String, byte[]>(t._1(), res);
			});
			
			hbasePuts = rddImagesRegionMatrixed.sortByKey().flatMapToPair((x) -> {
				ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>> result = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
				String split[] = x._1().split("#");
				String key = split[0];
				String zoom = split[1];
				result.add(new Tuple2<ImmutableBytesWritable, KeyValue>(
						new ImmutableBytesWritable(Bytes.toBytes(x._1 + HBaseSetup.HEIGHT_FAMILY + 0)),
						new KeyValue(
								key.getBytes(), // row key
								HBaseSetup.HEIGHT_FAMILY.getBytes(), // column family
								zoom.getBytes(), // column
								x._2()))); // value

				return result.iterator();
			});
			hPath = new Path("/tmp/DLM"+i);
			hbasePuts.saveAsNewAPIHadoopFile("/tmp/DLM"+i, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, 
					job.getConfiguration());
			try {
				loader = new LoadIncrementalHFiles(hbaseConf);
				loader.doBulkLoad(hPath, admin, table, regionLocator);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		

	}
}