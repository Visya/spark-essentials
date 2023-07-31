package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DataFrame colums and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")
  // projection
  val carNamesDF = carsDF.select(firstColumn)

  import spark.implicits._
  val car = carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year,               // scala symbol, auto-converted to column
    $"Horsepower",       // interpolated string, returns Column object
    expr("Origin") // expression
  )

  // plain strings
  carsDF.select("Name", "Acceleration", "Year")

  val weightInKilos = carsDF.col("Weight_in_lbs") / 2.2
  val carsWithWeights = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKilos.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  carsWithWeights.show()

  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  val carsWithKgs3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / "2.2.")
  val carsWithColumnRenamedDF = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  carsWithColumnRenamedDF.drop("Cylinders", "Displacement")

  // Filters
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where("Origin != 'USA'")
  val americanCarsDF = carsDF.filter(col("Origin") === "USA")
  val americanPowerfulCarsDF = americanCarsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)

  // unioning (adding more rows)

  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  val allCountriesDF = allCarsDF.select("Origin").distinct()
  val countriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()
  countriesDF.show()

  // Excersizes
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesWithSelectedColumnsDF = moviesDF.select("Title", "Production_Budget")
  val moviesWithTotalProfitsDF = moviesDF.select(
    col("Title"),
    expr("US_Gross + Worldwide_Gross + US_DVD_Sales").as("Total_Profits")
  )
  val goodComediesDF = moviesDF.where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  goodComediesDF.show()
}
