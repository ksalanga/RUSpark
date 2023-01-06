package com.RUSpark;

/* author: Kenneth Salanga */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("NetflixGraphGenerate")
                .getOrCreate();

        JavaRDD<Row> movies = spark.read().option("delimiter", ",").csv(InputPath).javaRDD();

        // For each (movie, rating) pair, identify the set of customers who rated the movie with that particular rating value
        JavaPairRDD<Tuple2<Integer, Integer>, ArrayList<Integer>> movieRatingPairs =
        movies.mapToPair(movie -> {
            int id = Integer.parseInt((String) movie.get(0));

            int customerID = Integer.parseInt((String) movie.get(1));
            int rating = Integer.parseInt((String) movie.get(2));

            return new Tuple2<>(new Tuple2<>(id, rating), new ArrayList<Integer>(Arrays.asList(customerID)));
        }).reduceByKey((customers1, customers2) -> {
            return (ArrayList<Integer>) Stream.concat(customers1.stream(), customers2.stream()).collect(Collectors.toList());
        });

        // For each (movie, rating) pair, generate an edge between each pair of customers within the set of customers that gave the movie the same rating

        // map the values of movie Rating Pairs from an array list of customers
        // to an array list of tuples: (customer1, customer2), where the list of tuples is every possible pair of customers (edge) in the original array
        // make sure that customer1 < customer2
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> customerEdges = movieRatingPairs.flatMapValues(customers -> {
            ArrayList<Tuple2<Integer, Integer>> edges = new ArrayList<>();
            for (int i = 0; i < customers.size(); i++) {
                for (int j = i + 1; j < customers.size(); j++) {
                    Integer customer1 = customers.get(i);
                    Integer customer2 = customers.get(j);

                    if (customer1 < customer2) {
                        edges.add(new Tuple2<>(customer1, customer2));
                    } else {
                        edges.add(new Tuple2<>(customer2, customer1));
                    }
                }
            }
            return edges.iterator();
        });

        // Aggregate the edges for each customer pair to find the edge weights.
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> edgeCounts = customerEdges.mapToPair(movieRating -> {
            Tuple2<Integer, Integer> customerEdge = movieRating._2();
            return new Tuple2<Tuple2<Integer, Integer>, Integer>(customerEdge, 1);
        }).reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = edgeCounts.collect();

        for (Tuple2<Tuple2<Integer, Integer>, Integer> tuple : output) {
            Tuple2<Integer, Integer> customerEdge = tuple._1();
            String customer1 = Integer.toString(customerEdge._1());
            String customer2 = Integer.toString(customerEdge._2());

            String weight = Integer.toString(tuple._2());
            System.out.println("(" + customer1 + "," + customer2 + ")" + " " + weight);
        }

        spark.stop();
	}

}
