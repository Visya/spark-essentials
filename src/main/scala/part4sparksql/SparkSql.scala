package part4sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object SparkSql extends App {
  val spark = SparkSession
    .builder()
    .appName("SparkSql")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // SQL
  carsDF.createOrReplaceTempView(
    "cars"
  ) // creates an alias in spark to be able to write cars in SQL
  val americanCarsDF = spark.sql(
    """
        |select Name from cars where Origin = 'USA'
    """.stripMargin
  )
  // americanCarsDF.show()

  spark.sql(
    "create database rtjvm"
  ) // creates a database and returns empty DataFrame
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
//   databasesDF.show()

  // how to transfer tables from DB to Spark tables
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

  def transferTables(
      tableNames: List[String]
  ) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.write
      .mode(SaveMode.Overwrite)
      .option("path", s"src/main/resources/warehouse/rtjvm.db/$tableName")
      .saveAsTable(tableName)
  }

  transferTables(
    List(
      "employees",
      "departments",
      "titles",
      "dept_emp",
      "salaries",
      "dept_manager"
    )
  )

  // reading DF from loaded tables
  val employeesDF2 = spark.read.table("employees")

  /*
    Exercises:
    1. Read movies and store as table
    2. Count how many employees we have between Jan 1 2000 and Jan 1 2001 (hire date)
    3. Show average salaries for the employees from above
    4. Show the best paying department from above
   */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

//   moviesDF.write
//     .mode(SaveMode.Overwrite)
//     .option("path", "src/main/resources/warehouse/rtjvm.db/movies")
//     .saveAsTable("movies")

  // 2
  spark
    .sql(
      """
        |select count(*)
        |from employees
        |where hire_date > '1998-01-01' and hire_date < '2000-01-01'
    """.stripMargin
    )

  // 3
  spark
    .sql(
      """
        |select de.dept_no, avg(s.salary)
        |from employees e, dept_emp de, salaries s
        |where
        |  e.hire_date > '1998-01-01' and e.hire_date < '2000-01-01'
        |  and e.emp_no = de.emp_no
        |  and s.emp_no = de.emp_no
        |group by 1
    """.stripMargin
    )
    // .show()

  // 4
  spark.sql("""
    |with dept_avg as (
    |   select de.dept_no, avg(s.salary)
    |   from employees e, dept_emp de, salaries s
    |   where
    |       e.hire_date > '1998-01-01' and e.hire_date < '2000-01-01'
    |       and e.emp_no = de.emp_no
    |       and s.emp_no = de.emp_no
    |   group by 1
    |   order by 2 desc
    |   limit 1
    |)
    |select dept_name
    |from dept_avg, departments d
    |where dept_avg.dept_no = d.dept_no
  """.stripMargin)
  .show()
}
