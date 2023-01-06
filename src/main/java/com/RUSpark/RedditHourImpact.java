package com.RUSpark;

/* author: Kenneth Salanga */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class RedditHourImpact {
    public static int convertUnixTimeToHourOfDay(long unixTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH");

        String hourString = Instant.ofEpochSecond(unixTime)
                .atZone(ZoneId.of("US/Eastern"))
                .format(formatter);

        return Integer.parseInt(hourString);
    }

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }

        String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("RedditHourImpact")
                .getOrCreate();

        JavaRDD<Row> posts = spark.read().option("delimiter", ",").csv(InputPath).javaRDD();

        JavaPairRDD<Integer, Integer> impactScores = posts.mapToPair(post -> {
            long unixtime = Long.parseLong((String) post.get(1));

            int hourOfDay = convertUnixTimeToHourOfDay(unixtime);

            int upvotes = Integer.parseInt((String) post.get(4));
            int downvotes = Integer.parseInt((String) post.get(5));
            int comments = Integer.parseInt((String) post.get(6));

            int impact = upvotes + downvotes + comments;

            return new Tuple2<>(hourOfDay, impact);
        });

        JavaPairRDD <Integer, Integer> impactScoreTotals = impactScores.reduceByKey((i1, i2) -> i1 + i2);

        HashMap<Integer, Integer> output = new HashMap<>(impactScoreTotals.collectAsMap());

        for (int hour = 0; hour < 24; hour++) {
            String hourString = Integer.toString(hour);
            String impactScore = Integer.toString(output.getOrDefault(hour, 0));

            System.out.println(hourString + " " + impactScore);
        }

        spark.stop();
	}

}
