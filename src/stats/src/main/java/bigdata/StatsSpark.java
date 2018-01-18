package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;
import scala.Tuple3;

public class StatsSpark {

	private static final String dem3Path = "/raw_data/dem3_lat_lng.txt";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Stats PLE_DLM");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rddDEM3File;
		rddDEM3File = context.textFile(dem3Path);
		
		System.out.println("Nombre de partitions : " + Integer.toString(rddDEM3File.getNumPartitions()));
		System.out.println(conf.getExecutorEnv().toString());
		JavaRDD<Tuple3<Double, Double, Integer> > rdd2 = rddDEM3File.map((s) -> {
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
		
		StatCounter sc = rdd2.mapToDouble(t -> t._3()).stats();
		
		double bucket[] = {0, 1, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000};
		
		long[] histogram = rdd2.mapToDouble(t -> t._3()).histogram(bucket);
		for (int i = 0; i < Math.min(bucket.length, histogram.length); i++) {
			System.out.print(bucket[i]);
			System.out.print("  :  ");
			System.out.println(histogram[i]);
		}
		
		System.out.println("Stats en vue :");
		System.out.println(sc.toString());
		System.out.println("Nombre de partitions : " + Integer.toString(rddDEM3File.getNumPartitions()));
		System.out.println("Au revoir");
	}
}