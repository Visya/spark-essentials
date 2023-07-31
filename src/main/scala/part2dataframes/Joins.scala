package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {
  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")
  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")
  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val joinCond = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitarPlayersDF
    .join(bandsDF, joinCond, "inner")

  // semi-joins
  guitarPlayersDF
    .join(
      bandsDF,
      joinCond,
      "left_semi"
    ) // like inner, but doesn't include the data from the right DF

  guitarPlayersDF
    .join(
      bandsDF,
      joinCond,
      "anti"
    ) // all rows in the left DF, where there is no row in the right DR

  // removing duplicate column names after join
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // joins using complex types
  guitarPlayersDF
    .join(
      guitarsDF
        .withColumnRenamed("id", "guitar_id"),
      expr("array_contains(guitars, guitar_id)")
    )

  // Exercise
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .options(
      Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
        "user" -> "docker",
        "password" -> "docker",
        "dbtable" -> s"public.$tableName"
      )
    )
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val titlesDF = readTable("titles")
  val deptManagersDF = readTable("dept_manager")

  // 1
  val maxSalariesPerEmp = salariesDF
    .groupBy("emp_no")
    .agg(max("salary").as("max_salary"))
  val employeesWithMaxSalaries = employeesDF.join(maxSalariesPerEmp, "emp_no")
  // employeesWithMaxSalaries.show()

  // 2
  val neverManagersDF =
    employeesDF.join(
      deptManagersDF,
      employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
      "left_anti"
    )
  // neverManagersDF.show()

  // 3
  val mostRecentJobTitles =
    titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmp =
    employeesWithMaxSalaries.orderBy(col("max_salary").desc).limit(10)
  val bestPaidJobs = bestPaidEmp.join(mostRecentJobTitles, "emp_no")
  bestPaidJobs.show()
}

/*
with s as (select
  emp_no,
  title
from best_paid
inner join titles
group by emp_no, title
order by emp_no asc, to_date desc
)

 */
