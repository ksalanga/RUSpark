package com.RUSpark;

/* author: Kenneth Salanga */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("NetflixMovieAverage")
                .getOrCreate();

        JavaRDD<Row> movies = spark.read().option("delimiter", ",").csv(InputPath).javaRDD();

        JavaPairRDD<Integer, Integer> ratings = movies.mapToPair(movie -> {
            int id = Integer.parseInt((String) movie.get(0));

            int rating = Integer.parseInt((String) movie.get(2));

            return new Tuple2<>(id, rating);
        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> oneRating = ratings.mapValues(rating -> new Tuple2(rating, 1));

        JavaPairRDD<Integer, Double> averages = oneRating.reduceByKey((rating1, rating2) -> {
            int rating1Sum = rating1._1();
            int rating2Sum = rating2._1();

            int rating1Count = rating1._2();
            int rating2Count = rating2._2();

            return new Tuple2<>(rating1Sum + rating2Sum, rating1Count + rating2Count);
        }).mapValues(rating -> rating._1() / (double) rating._2());
        

        List<Tuple2<Integer, Double>> output = averages.collect();

        for (Tuple2<Integer,Double> tuple : output) {
            System.out.println(tuple._1() + " " + tuple._2());
        }

        spark.stop();
	}

}
