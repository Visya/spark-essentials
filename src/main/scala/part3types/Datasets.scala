package part3types

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Datasets extends App {
  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  // DataFrame is a Dataset[Row]
  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // numbersDS.filter(_ < 100).show

  // Dataset of a complex type
  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double],
      Cylinders: Long,
      Displacement: Double,
      Horsepower: Option[Long],
      Weight_in_lbs: Long,
      Acceleration: Double,
      Year: Date,
      Origin: String
  )

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // implicit val carEncoder = Encoders.product[Car] // Encode any class that extends Product
  import spark.implicits._ // includes everything you need for dataset
  val carsDF = spark.read
    .schema(carsSchema)
    .json(s"src/main/resources/data/cars.json")
  val carsDS = carsDF.as[Car]

  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  // carNamesDS.show()

  // Datasets are less optimized than Dataframes

  // Excersize
  // 1. Count cars
  // 2. Count powerful cars Horsepower > 140
  // 3. Compute average HP

  val carsCount = carsDS.count
  // println(carsCount)
  // println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)
  // println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // computing avg HP in a different way
  // carsDS.select(avg(col("Horsepower"))).show()

  // Joins for datasets
  case class Guitar(
      id: Long,
      make: String,
      model: String,
      guitar_type: String
  )

  case class GuitarPlayer(
      id: Long,
      name: String,
      guitars: Seq[Long],
      band: Long
  )

  case class Band(
      id: Long,
      name: String,
      hometown: String,
      year: Long
  )

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  // Result Dataset[(GuitarPlayer, Band)]
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDS.joinWith(
      bandsDS,
      guitarPlayersDS.col("band") === bandsDS.col("id"),
      "inner"
    )
  // guitarPlayerBandsDS.show()

  // Exercise 2
  // join guitarPlayers with guitars
  val guitarPlayerGuitarsDS = guitarPlayersDS.joinWith(
    guitarsDS,
    array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
    "outer"
  )
  // guitarPlayerGuitarsDS.show()

  // Grouping
  // KeyValueGroupedDataset[String, Car]
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin)
  carsGroupedByOrigin.count().show()
  // Groups and joins are wide trasformations, that change number of partitions and can move data between nodes
  // So EXPENSIVE  
}
