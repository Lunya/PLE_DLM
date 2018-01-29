package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;
import scala.Tuple3;

public class StatsSpark {

	private static final String gpsPath = "/raw_data/simple-gps-points-120312.txt";

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Stats PLE_DLM");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> gpsFile;
		gpsFile = context.textFile(gpsPath);

		System.out.println("Nombre de partitions : " + Integer.toString(gpsFile.getNumPartitions()));
		System.out.println(conf.getExecutorEnv().toString());
		JavaRDD<Tuple2<Double, Double>> rdd2 = gpsFile.map((s) -> {
			String[] split = s.toString().split(",");
			Double latitude = 0.0;
			Double longitude = 0.0;
			Integer height = 0;
			try {
				latitude = Double.parseDouble(split[0]);
			} catch (Exception e) {
			}
			try {
				longitude = Double.parseDouble(split[1]);
			} catch (Exception e) {
			}
			return new Tuple2<Double, Double>(latitude, longitude);
		});

		JavaDoubleRDD heights = rdd2.mapToDouble(t -> t._2()).cache();
		long startTime, endTime;

		// calcul des stats sur toutes les hauteurs
		startTime = System.currentTimeMillis();
		StatCounter statHeights = heights.stats();
		endTime = System.currentTimeMillis();
		System.out.println(statHeights.toString());
		System.out.println(((endTime - startTime) / 1000) + " secondes");

		// calcul des stats sur 1% des hauteurs
		endTime = System.currentTimeMillis();
		StatCounter stat1percentHeights = heights.sample(false, 0.01).stats();
		endTime = System.currentTimeMillis();
		System.out.println(stat1percentHeights.toString());
		System.out.println(((endTime - startTime) / 1000) + " secondes");

		// double buckets[] = {0, 1, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000};
		int bucketCount = 100;
		double bucketMax = 8000;
		double buckets[] = new double[bucketCount];
		for (int i = 0; i < bucketCount; i++)
			buckets[i] = (double) i * (bucketMax / (double) bucketCount);
		// calculer l'histogramme d'après des valeurs pré-calculées
		startTime = System.currentTimeMillis();
		long[] histogram = heights.histogram(buckets);
		endTime = System.currentTimeMillis();
		System.out.println("Histogramme d'après valeurs");
		System.out.println(((endTime - startTime) / 1000) + " secondes");
		for (int i = 0; i < Math.min(buckets.length, histogram.length); i++) {
			System.out.println(buckets[i] + "  :  " + histogram[i]);
		}

		// calculer l'histogramme automatiquement
		startTime = System.currentTimeMillis();
		Tuple2<double[], long[]> histogram2 = heights.histogram(bucketCount);
		endTime = System.currentTimeMillis();
		System.out.println("Histogramme d'après nombre");
		System.out.println(((endTime - startTime) / 1000) + " secondes");
		for (int i = 0; i < Math.min(histogram2._1.length, histogram2._2.length); i++) {
			System.out.println(histogram2._1[i] + "  :  " + histogram2._2[i]);
		}

		System.out.println("Stats en vue :");
		System.out.println("Nombre de partitions : " + Integer.toString(gpsFile.getNumPartitions()));
		System.out.println("Au revoir");
	}
}