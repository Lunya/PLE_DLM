package ple_dlm.batch_layer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.github.davidmoten.geo.GeoHash;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class CitiesToHBase {
	public static void insert(
			SparkConf sparkConf,
			Configuration hbaseConf,
			String citiesFile,
			String regionFile) {
		
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		// key: (country + region) value: city, popoulation, latitude, longitude 
		JavaPairRDD<String, Tuple4<String, Long, Double, Double>> cities =
				context.textFile(citiesFile).flatMapToPair((line) -> {
					// Country,City,AccentCity,Region,Population,Latitude,Longitude
					final String fields[] = line.split(",");
					Iterator<Tuple2<String, Tuple4<String, Long, Double, Double>>> res = Collections.emptyIterator();
					try {
						final String key = fields[0].toUpperCase() + fields[3];
						res = Collections.singletonList(new Tuple2<String, Tuple4<String, Long, Double, Double>>(
								key, new Tuple4<String, Long, Double, Double>(
										fields[2],
										Long.parseLong(fields[4]),
										Double.parseDouble(fields[5]),
										Double.parseDouble(fields[6])))).iterator();
					} catch(Exception e) {}
					return res;
				});
		
		// key: (country + region) value: country name
		JavaPairRDD<String, String> regionCodes = context.textFile(regionFile).flatMapToPair((line) -> {
			// Country,Region,CountryName
			final String fields[] = line.split(",");
			return Collections.singletonList(new Tuple2<String, String>(
					fields[0].toUpperCase() + fields[1],
					fields[2].substring(1, fields[2].length()-1))).iterator();
		});
		
		// key: Geohash value: city, popoulation, country name
		final byte geohashPrecision = 10; // not 8 because multiple cities can have same key, too near
		JavaPairRDD<String, Tuple2<Tuple4<String, Long, Double, Double>, String>> join = cities.join(regionCodes);
		JavaPairRDD<String, Tuple3<String, Long, String>> preparedValues = join.mapToPair((row) -> {
			return Tuple2.apply(
					GeoHash.encodeHash(row._2()._1()._3(), row._2()._1()._4(), geohashPrecision),
					Tuple3.apply(row._2()._1()._1(),  row._2()._1()._2(), row._2()._2()));
		});
		
		JavaPairRDD<ImmutableBytesWritable, KeyValue> hbasePuts = preparedValues.groupByKey().mapToPair((row) -> {
			Tuple3 res = Tuple3.apply("",  0L, "");
			return Tuple2.apply(row._1(), res);
		}).sortByKey().flatMapToPair((row) -> {
			List<Tuple2<ImmutableBytesWritable, KeyValue>> res = new ArrayList<>();
			res.add(Tuple2.apply( // country
					new ImmutableBytesWritable((row._1() + 0).getBytes()),
					new KeyValue(row._1().getBytes(), // row key
							HBaseSetup.CITIES_FAMILY.getBytes(), // column family
							HBaseSetup.COUNTRY_COL.getBytes(), // column
							row._2()._3().getBytes()// value
							)));
			
			res.add(Tuple2.apply( // city
					new ImmutableBytesWritable((row._1() + 0).getBytes()),
					new KeyValue(row._1().getBytes(), // row key
							HBaseSetup.CITIES_FAMILY.getBytes(), // column family
							HBaseSetup.CITY_COL.getBytes(), // column
							row._2()._1().getBytes()// value
							)));
			
			res.add(Tuple2.apply( // population
					new ImmutableBytesWritable((row._1() + 0).getBytes()),
					new KeyValue(row._1().getBytes(), // row key
							HBaseSetup.CITIES_FAMILY.getBytes(), // column family
							HBaseSetup.POPULATION_COL.getBytes(), // column
							Bytes.toBytes(row._2()._2())// value
							)));
			return res.iterator();
		});
		
		try {
			HBaseSetup.saveRDD(hbaseConf, HBaseSetup.TABLE_BASENAME + "_test", hbasePuts, "cities");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
