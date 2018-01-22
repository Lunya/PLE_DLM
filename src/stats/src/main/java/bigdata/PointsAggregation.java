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
		
		/*
		
		*/
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
			
			//Todo :
			// Il faut obtenir les coordonnées x et y du pixel de la région locale à ce point.
			
			//int xKey = (t._1 - latKey) * latStep/256
			//int yKey = (t._2 - lonKey) * lonStep/256
			
			String key = Integer.toString(latKey) + "#" + Integer.toString(lonKey) + "#" + Integer.toString(xKey) + "#" + Integer.toString(yKey);
			return key;
		});
		
		/*
		for (Tuple2<String, Iterable<Tuple3<Double, Double, Integer>>> e : rddImages.cache().take(100)) {
			System.out.println(e.toString());
		}
		*/
		
		//Done : Lire le fichier texte de 220 GO
		//Clé : NULL
		//Valeur : Ligne du fichier
		
		//Done : Obtenir les points à partir de ces lignes
		//Clé : NULL
		//Valeur : Lat / Long / Alt
		
		//Done : Regrouper les points appartenant à la même région et au même pixel.
		//Clé : Lat/Long (Region) + X/Y (Pixel Local).
		//Valeur : Iterable (Lat / Long / Alt)
		
		//Todo : Il faut réduire l'Iterable de points à un seul Point (surtout : une altitude)
		//Clé : Lat/Long (Region) + X/Y (Pixel Local).
		//Valeur : Lat Long Altitude
		
		//Todo : Il faut réduire l'Iterable de points à un seul Point (surtout : une altitude)
		//Clé : Lat/Long (Region).
		//Valeur : Iterable X/Y (Pixel) Altitude
		
		
		//Todo : On a donc ensuite pour chaque pixel un seul point, il faut ensuite agréger tous les points d'une même région à une même information
		//Clé : Lat/Long (Region)
		//Valeur : Matrice d'altitude (256*256)
		
		//Todo : Insertion dans hBase en convertissant la matrice en une autre structure de donnée.
		
		/*
		JavaPairRDD<String, Tuple3<Double, Double, Integer>> rddImagesAggregated = rddImages.mapValues((key, t) -> {
			int max = 0;
			Tuple3<Double, Double, Integer> maxElem;
			for (Tuple3<Double, Double, Integer> point : t) {
				if (max < point._3()) {
					max = point._3();
					maxElem = maxElem;
				}
			}
			return maxElem;
		});
		*/
		
		//JavaDoubleRDD heights = rddDEM3.mapToDouble(t -> t._3()).cache();
		
		//long startTime, endTime;
		
	}
}