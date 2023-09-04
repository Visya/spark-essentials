package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("ComplexDataTypes")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  moviesDF
    .select(
      col("Title").as("title"),
      // The year is wrong since spark uses base 2000 for the year that includes only two digits
      // 98 -> 2098
      to_date(col("Release_Date"), "d-MMM-yy").as("actual_release")
    )
    .withColumn("today", current_date())
    .withColumn("right_now", current_timestamp())
    .withColumn(
      "movie_age",
      datediff(col("today"), col("actual_release")) / 365
    )

  // Exercise
  val stocksDF = spark.read
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")
  // stocksDF.show()

  // 1. How to deal with different data formats?
  // You can parse the dataframe multiple times with different formats that exist in the data.
  // Discard incorrect data

  val stocksDF2 = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  stocksDF2
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  // Structures
  // 1 - column operators
  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    // .show()

  // 2 expressions
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords =
    moviesDF.select(col("Title"), split(col("Title"), " |,").as("title_words"))

  moviesWithWords
    .select(
      col("Title"),
      expr("title_words[0]"),
      size(col("title_words")),
      array_contains(col("title_words"), "Love")
    )
    .show()
}
