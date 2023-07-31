package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Data processing
  // Aggregations and grouping
  val genresCountDF = moviesDF.select(countDistinct(col("Major_Genre")))

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .count()

  val avgRatingByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .avg("IMDB_Rating")

  val aggByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .agg(
      count("*").as("n_movies"),
      avg("IMDB_Rating").as("avg_rating")
    )
    .orderBy("avg_rating")

  // Exercises
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()
  moviesDF.select(countDistinct("Director")).show()
  moviesDF.select(
    mean("US_Gross").as("mean_us_gross"),
    stddev("US_Gross").as("std_us_gross")
  ).show()
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("avg_imdb_rating"),
      avg("US_Gross").as("avg_us_gross")
    )
    .orderBy("avg_imdb_rating")
    .show()
}
