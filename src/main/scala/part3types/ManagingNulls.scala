package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {
  val spark = SparkSession
    .builder()
    .appName("ManagingNulls")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  StructType(
    StructField("Name", StringType, nullable=False)
  )
  In this case nullable=False is NOT a constraint, it's a flag for Spark to optimize for nulls
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
      .as("Rating")
  )
  //   .show()

  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // remove nulls
  moviesDF
    .select("Title", "IMDB_Rating")
    .na
    .drop() // remove rows containing nulls

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show()
  moviesDF.na.fill(
    Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 0,
      "Director" -> "Unknown"
    )
  )

  moviesDF
    .selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",
      "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
      "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
      "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",
      "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) then second else third
    )
    .show()
}
