package com.RUSpark;

/* any necessary Java packages here */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

import static scala.Predef.Map;

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
        /* Implement Here */
        SparkSession spark = SparkSession
                .builder()
                .appName("RedditPhotoImpact")
                .getOrCreate();

//		Get rows in csv
        JavaRDD<Row> posts = spark.read().option("delimiter", ",").csv(InputPath).javaRDD();

//      row: image_id, unixtime, title, subreddit, #_upvotes, #_downvotes, #_comments

//      For each row, create a new Tuple2 of (id, #_upvotes + #_downvotes + #_comments)

        JavaPairRDD<Integer, Integer> impactScores = posts.mapToPair(post -> {
            int id = Integer.parseInt((String) post.get(0));

            int upvotes = Integer.parseInt((String) post.get(4));
            int downvotes = Integer.parseInt((String) post.get(5));
            int comments = Integer.parseInt((String) post.get(6));

            int impact = upvotes + downvotes + comments;

            return new Tuple2<>(id, impact);
        });

//      Reduce Tuples by Key where: i1 + i2
        JavaPairRDD <Integer, Integer> impactScoreTotals = impactScores.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<Integer, Integer>> output = impactScoreTotals.collect();

        for (Tuple2<Integer,Integer> tuple : output) {
            System.out.println(tuple._1() + " " + tuple._2());
        }

        spark.stop();
	}

}
