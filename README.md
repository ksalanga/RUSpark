## CS 417 Distributed Systems Project: Analyzing Reddit and Netflix Data with Apache Spark

### In this project I:

- Processed large datasets (100,000 entries) in [< 1 second](https://i.gyazo.com/4c83b52ed90a9122ffa9b6961777a913.png).
- Processed huge datasets (1,000,000 entries) in [~1 second](https://i.gyazo.com/c71196e9d03a1be9941eda4deee490b5.png).
- Parsed CSV files into Java Spark RDDs.
- Used iterators and maps to transform the RDDs and get a final result.

### Data I analyzed and processed:

#### Reddit Photo Dataset:
- Which photos had the most impact based on upvotes, downvotes, and comments.
- Which time of day had the most impact.

one reddit csv row:

`image_id, unixtime, title, subreddit, #_upvotes, #_downvotes, #_comments`

`123, 1616703628, A, funny, 200, 50, 6`

#### Netflix Dataset:
- Movie rating averages.
- Generating a graph of movie recommendations based on user common interests.

one netflix csv row:

`movie_id, customer_id, rating, date`
`201, 1062, 2, 3-13-20`

### Run programs:
- use spark-submit (I used my university's ilab machines)
- ex: spark-submit myprogram.jar inputdata.csv > out.txt
