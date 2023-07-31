package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data sources and formats")
    .config("spark.master", "local")
    .getOrCreate()

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

  /**
   * Reading a DF:
   *  - format
   *  - schema or inferSchema=true
   *  - path
   *  - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .options(Map(
      "mode" -> "failFast", // dropMalformed, permissive (default)
      "path" -> "src/main/resources/data/cars.json",
    ))
    .load()

  /**
   * Writing DFs:
   *  - format
   *  - mode
   *  - path
   *  - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_duplicate.json")

  // JSON flags
//  spark.read
//    .schema(carsSchema)
//    .option("dateFormat", "yyyy-MM-DD")
//    .option("allowSingleQuotes", "true")
//    .option("compression", "uncompressed")
//    .json("src/main/resources/data/cars.json")

  // CSV
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
  spark.read
    .schema(stocksSchema)
    .options(Map(
      "dateFormat" -> "MMM dd yyyy",
      "header" -> "true",
      "sep" -> ",",
      "nullValue" -> ""
    ))
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  // default storage format for DFs
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sample_text.txt").show()

  // Reading from remote
  val employeesDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.employees"
    ))
    .load()

  employeesDF.show()

  /**
   * Exercise 1
   */
    
  val movieSchema = StructType(Array(
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Distributor", StringType),
    StructField("IMDB_Rating", DoubleType),
    StructField("IMDB_Votes", LongType),
    StructField("MPAA_Rating", StringType),
    StructField("Major_Genre", StringType),
    StructField("Production_Budget", LongType),
    StructField("Release_Date", DateType),
    StructField("Rotten_Tomatoes_Rating", LongType),
    StructField("Running_Time_min", LongType),
    StructField("Source", StringType),
    StructField("Title", StringType),
    StructField("US_DVD_Sales", LongType),
    StructField("US_Gross", LongType),
    StructField("Worldwide_Gross", LongType),
  ))
  val moviesDF = spark.read
//    .schema(movieSchema)
    .option("inferSchema", "true")
//    .option("dateFormat", "dd-MMM-yy")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .options(Map(
      "sep" -> "\t",
      "header" -> "true",
      "nullValue" -> ""
    ))
    .csv("src/main/resources/data/movies.csv")

  moviesDF.write.save("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.movies"
    ))
    .save()
}
