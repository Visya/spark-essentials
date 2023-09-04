package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // insert value into a column
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(preferredFilter)
  val moviesWithGoodnessFlagDF =
    moviesDF.select(col("Title"), preferredFilter.as("is_good_movie"))

  // filter on a boolean column
  moviesWithGoodnessFlagDF.where("is_good_movie")
  moviesWithGoodnessFlagDF.where(not(col("is_good_movie")))

  // Numbers
  val movieAvgRatingDF = moviesDF.select(
    col("Title"),
    (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
  )

  // correlation between columns Pearson correlation coefficient (-1 to 1)
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Dataprocessing on Strings
  // Capitalisation: initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("vw_extr")
    )
    .where(col("vw_extr") =!= "")
    .drop("vw_extr")

  // replacing
  vwDF
    .select(
      col("Name"),
      regexp_replace(col("Name"), regexString, "Blah").as("repl")
    )

  // Exercise
  // filter carsDF by a list of car names
  def getCarNames: List[String] = List("volkswagen", "vw", "dodge")

  def filterCarsByName(carNames: List[String]) = {
    val regexFilter = carNames.map(_.toLowerCase()).mkString("|")
    carsDF
      .select("*")
      .where(regexp_extract(col("Name"), regexFilter, 0).as("regex") =!= "")
  }

  val filteredCars = filterCarsByName(getCarNames)
  filteredCars.show()

  val carNameFilters =
    getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter =
    carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) =>
      combinedFilter or newCarNameFilter
    )
  carsDF.filter(bigFilter).show()
}
