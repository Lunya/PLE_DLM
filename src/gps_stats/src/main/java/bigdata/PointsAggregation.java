package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class PointsAggregation {

	private static final String gpsPath = "/raw_data/simple-gps-points-120312.txt";

	public static Tuple3<Double, Double, Integer> main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Aggregation Points GPS");
		JavaSparkContext context = new JavaSparkContext(conf);

		// Done : Lire le fichier texte de 220 GO
		// Clé : NULL
		// Valeur : Ligne du fichier
		JavaRDD<String> gpsFile;
		gpsFile = context.textFile(gpsPath);

		// Done : Obtenir les points à partir de ces lignes
		// Clé : NULL
		// Valeur : Lat / Long
		JavaRDD<Tuple2<Double, Double>> rddDEM2 = gpsFile.map((s) -> {
			String[] split = s.toString().split(",");
			Double latitude = 0.0;
			Double longitude = 0.0;
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

		JavaRDD<Tuple2<Double, Double>> gpsFileFiltered = rddDEM2.filter((s) -> {
			if (s._1() < -90 || s._1() >= 90 || s._2() < -180 || s._2() >= 180)
				return false;
			else
				return true;
		});

		// rddDEM3Filtered = rddDEM3Filtered.cache();

		int zoom = 0;
		double latSeparator = (1 * Math.pow(2, zoom));
		double lonSeparator = (2 * Math.pow(2, zoom));
		System.out.println("LatSeparator : " + latSeparator);
		System.out.println("LonSeparator : " + lonSeparator);
		double latStep = 180 / latSeparator;
		double lonStep = 360 / lonSeparator;
		System.out.println("LatStep : " + latStep);
		System.out.println("LonStep : " + lonStep);

		// Doing : Regrouper les points appartenant à la même région et au même pixel.
		// Clé : Lat/Long (Region) + X/Y (Pixel Local).
		// Valeur : Iterable (Lat / Long / Alt)
		JavaPairRDD<String, Iterable<Tuple2<Double, Double>>> rddImages = gpsFileFiltered.groupBy((t) -> {
			double latitude = t._1();
			double longitude = t._2();

			latitude += 90;
			longitude += 180;

			latitude = Math.floor(latitude / latStep);
			longitude = Math.floor(longitude / lonStep);

			double latKey = latitude * latStep;
			double lonKey = longitude * lonStep;

			int xKey = (int) Math.floor(((t._2() + 180) - lonKey) * (256.0 / lonStep));
			int yKey = (int) Math.floor(((t._1() + 90) - latKey) * (256.0 / latStep));

			latKey -= 90;
			lonKey -= 180;

			String key = Double.toString(latKey) + "#" + Double.toString(lonKey) + "#" + Integer.toString(xKey) + "#"
					+ Integer.toString(yKey);
			return key;
		});

		// rddImages.saveAsTextFile("/user/dimprestat/test_alpha_4");

		// Done : Il faut réduire l'Iterable de points à un seul Point (surtout : une
		// altitude)
		// Clé : Lat/Long (Region) + X/Y (Pixel Local).
		// Valeur : Lat Long Altitude
		JavaPairRDD<String, Tuple3<Double, Double, Integer>> rddImagesAggregated = rddImages.mapValues((t) -> {
			int max = 0;
			Double medianLat = 0.0;
			Double medianLng = 0.0;
			Tuple3<Double, Double, Integer> maxElem = null;
			for (Tuple2<Double, Double> point : t) {
					max++;
					medianLat += point._1;
					medianLng += point._2;
			}
			if (max > 0)
			{
				medianLat /= max;
				medianLng /= max;
			}
			maxElem.copy(medianLat, medianLng, max);

			return maxElem;
		});

		rddImagesAggregated.saveAsTextFile("/user/marespiaut/testgps");
		// Done : On transforme le type de point en changeant le type de clé.
		// Clé : Lat/Long (Region).
		// Valeur : X/Y (Pixel) Altitude

		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rddImagesTrans = rddImagesAggregated.mapToPair((t) -> {
			String split[] = t._1().split("#");
			String key = split[0] + "#" + split[1];
			Tuple3<Integer, Integer, Integer> val;// = t._2;
			int x = Integer.parseInt(split[2]); // Split de la Key
			int y = Integer.parseInt(split[3]); // Split de la Key
			Integer alt = t._2()._3();// Trois
			val = new Tuple3<Integer, Integer, Integer>(x, y, alt);
			return new Tuple2<String, Tuple3<Integer, Integer, Integer>>(key, val);
		});
/*
		// Done : On regroupe les pixels en un iterable.
		// Clé : Lat/Long (Region).
		// Valeur : Iterable X/Y (Pixel) Altitude
		JavaPairRDD<String, Iterable<Tuple3<Integer, Integer, Integer>>> rddImagesRegionUnited = rddImagesTrans
				.groupByKey();

		// Todo : On a donc ensuite pour chaque pixel un seul point, il faut ensuite
		// agréger tous les points d'une même région à une même information
		// Clé : Lat/Long (Region)
		// Valeur : Matrice d'altitude (256*256)
		JavaPairRDD<String, byte[]> rddImagesRegionMatrixed = rddImagesRegionUnited.mapValues((t) -> {
			final int imageSize = 256;
			// ByteBuffer res = ByteBuffer.allocateDirect(2 * imageSize * imageSize);
			byte res[] = new byte[2 * imageSize * imageSize];
			for (Tuple3<Integer, Integer, Integer> point : t) {
				int x = point._1();
				int y = point._2();
				int alt = point._3();
				// res.putShort(2*(x*imageSize + y), (short)(alt-Short.MAX_VALUE));
				short val = (short) (alt - Short.MAX_VALUE);
				int pos = 2 * (x * imageSize + y);
				res[pos] = (byte) (val & 0xFF);
				res[pos + 1] = (byte) ((val >> 8) & 0xFF);
			}
			return res;
		});
*/
		// rddImagesRegionMatrixed.saveAsNewAPIHadoopDataset(conf);
		// rddImagesRegionMatrixed.saveAsTextFile("/user/dimprestat/test_gamma_4");
		// context.close();
		return null;

		// Todo : Insertion dans hBase en convertissant la matrice en une autre
		// structure de donnée.

		// rddImages.aggregateByKey(zeroValue, seqFunc, combFunc)

		// JavaDoubleRDD heights = rddDEM3.mapToDouble(t -> t._3()).cache();

		// long startTime, endTime;

	}
}