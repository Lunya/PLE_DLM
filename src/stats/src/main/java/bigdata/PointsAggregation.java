package bigdata;

import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;
import scala.Tuple3;

public class PointsAggregation {

	private static final String dem3Path = "/raw_data/dem3_lat_lng.txt";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Aggregation PLE_DLM");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rddDEM3File;
		rddDEM3File = context.textFile(dem3Path);
		
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
		
		int zoom = 0;
		double latSeparator = (1 * Math.pow(2, zoom));
		double lonSeparator = (2 * Math.pow(2, zoom));
		
		double latStep = 180/latSeparator;
		double lonStep = 360/lonSeparator;
		
		
		JavaPairRDD<String, Iterable<Tuple3<Double, Double, Integer> > > rddImages = rddDEM3.groupBy((t) -> {
			double latitude = t._1();
			double longitude = t._2();
			
			latitude +=90;
			longitude +=180;
			
			latitude = latitude/latStep;
			longitude = longitude/lonStep;
			
			int latKey = (int) Math.floor(latitude);
			int lonKey = (int) Math.floor(longitude);
			int xKey = 0;
			int yKey = 0;
			
			latKey -=90;
			lonKey -=180;
			
			//int xKey = (t._1 - latKey) * latStep/256
			//int yKey = (t._2 - lonKey) * lonStep/256
			
			String key = Integer.toString(latKey) + "#" + Integer.toString(lonKey) + "#" + Integer.toString(xKey) + "#" + Integer.toString(yKey);
			return key;
		});
		
		for (Tuple2<String, Iterable<Tuple3<Double, Double, Integer>>>e : rddImages.take(100)) {
			System.out.println(e._1());
		}
		
		//JavaDoubleRDD heights = rddDEM3.mapToDouble(t -> t._3()).cache();
		
		//long startTime, endTime;
		
	}
}