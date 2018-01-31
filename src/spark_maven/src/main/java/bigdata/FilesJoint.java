package bigdata;

import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple3;
import scala.Tuple7;

public class FilesJoint {

	private static final String pRegionCodes = "/raw_data/region_codes.csv";
	private static final String pWorldCitiesPop = "/raw_data/worldcitiespop.txt";

	// https://stackoverflow.com/a/140861
	public static byte hexStringToByteArray(String s) {
		return (byte) ((Character.digit(s.charAt(0), 16) << 4) + Character.digit(s.charAt(1), 16));
	}

	public static void joint(SparkConf conf, Configuration hbaseConf) {
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> fRegionCodes = context.textFile(pRegionCodes);
		JavaRDD<String> fWorldCitiesPop = context.textFile(pWorldCitiesPop);
		
		System.out.println("fichiers terminé");

		JavaRDD<Tuple3<String, Byte, String>> rRegionCodes = fRegionCodes.map((s) -> {
			String[] split = s.toString().split(",");

			String country = null;
			Byte region = null;
			String fullname = null;

			try {
				country = split[0];
			} catch (Exception e) {
			}
			try {
				region = hexStringToByteArray(split[1]);
			} catch (Exception e) {
			}
			try {
				fullname = split[2];
			} catch (Exception e) {
			}
			return new Tuple3<String, Byte, String>(country, region, fullname);
		});
		
		System.out.println("rRegionCodes terminé");

		JavaRDD<Tuple7<String, String, String, Byte, Integer, Double, Double>> rWorldCitiesPop = fWorldCitiesPop
				.map((s) -> {
					String[] split = s.toString().split(",");

					String country = null;
					String city = null;
					String accentCity = null;
					Byte region = null;
					Integer population = 0;
					Double latitude = 0.0;
					Double longitude = 0.0;

					try {
						country = split[0];
					} catch (Exception e) {
					}
					try {
						city = split[1];
					} catch (Exception e) {
					}
					try {
						accentCity = split[2];
					} catch (Exception e) {
					}
					try {
						region = hexStringToByteArray(split[3]);
					} catch (Exception e) {
					}
					try {
						population = Integer.parseInt(split[4]);
					} catch (Exception e) {
					}
					try {
						latitude = Double.parseDouble(split[5]);
					} catch (Exception e) {
					}
					try {
						longitude = Double.parseDouble(split[6]);
					} catch (Exception e) {
					}
					return new Tuple7<String, String, String, Byte, Integer, Double, Double>(country, city, accentCity,
							region, population, latitude, longitude);
				});
		
		System.out.println("rWorldCitiesPop terminé");
		
		JavaPairRDD
		
		context.close();
	}
}