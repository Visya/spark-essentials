package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import java.util.Date

object DataFramesBasics extends App {
  val spark = SparkSession.builder()
    .appName("DataFramesBasics")
    .config("spark.master", "local")
    .getOrCreate()

  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // inspecting DF
  firstDF.show()
  firstDF.printSchema()

  // get rows
  firstDF.take(10).foreach(println)

  // first Spark type
  val longType = LongType

  // first schema
  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  val carsDFSchema = firstDF.schema

  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-created

  // create DFs with implicits
  import spark.implicits._

  val manualCarsDFWthImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Origin")

  manualCarsDF.printSchema()
  manualCarsDFWthImplicits.printSchema()

  /**
   * Exercise 1
   */

  val phones = Seq(
    ("Google", "Pixel 7 Pro", "6 in", 16),
    ("Apple", "IPhone 4s", "4 in", 4),
    ("OnePlus", "1T", "5.5 in", 8)
  )

  val phonesManual = phones.toDF("Company", "Model", "Screen_Size", "RAM")

  phonesManual.printSchema()

  val movies = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  movies.printSchema()
  movies.take(5)
  println(s"Amount of rows in movies: ${movies.count()}")
}
