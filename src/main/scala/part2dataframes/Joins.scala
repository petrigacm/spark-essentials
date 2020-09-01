package part2dataframes

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, concat, expr, max}
import part2dataframes.DataSources.spark

object Joins extends App {

  // Joins combine data from multiple dataframes or tables
  // one or more columns from table 1 is compared to one or more column from table 2
  // table 1 is called left dataframe
  // table 2 is called right dataframe
  // if condition passes, rows are combined
  // non-matching row are discarded
  // Joins are wide transformations (expensive)

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferScheme", "true").json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.option("inferScheme", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferScheme", "true").json("src/main/resources/data/bands.json")


  // Join

  // inner join = only matching data
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristsDF.join(
    bandsDF, // other dataframe
    joinCondition, // condition
    "inner" // join type, inner is default,
  )

  // outer joints

  // left outer join = everything in the inner join + all row in the LEFT dataframe with null where data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer join = everything in the inner join + all row in the RIGHT dataframe with null where data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all row in the BOTH dataframe with null where data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = data only from left table which HAVE matching row in right table
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-join = data only from left table which DOES NOT HAVE matching row in right table
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // things to bear in mind
  // guitaristBandsDF.select("id", "band").show() - tnis crashes,  Reference 'id' is ambiguous, could be: id, id.;
  // Option 1: rename id column
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
  // Option 2: drop dupe column
  guitaristBandsDF.drop(bandsDF.col("id")) // we need ne specify DF to drop from
  // Option 3: rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristBandsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))


  // using complex types
  guitaristsDF.join(
    guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)") // we can use any type as joining condition
  )

  /*
    Exercises
    1) show all employees and their max salary
    2) show all employees which were never managers
    3) find job title of the best paid 10 employees in the company
   */

 def readTable(table: String): DataFrame = spark.read
   .format("jdbc")
   .option("driver", "org.postgresql.Driver")
   .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
   .option("user", "docker")
   .option("password", "docker")
   .option("dbtable", s"public.$table")
   .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1)
  val maxSalaryPerEmpNo = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesMaxSalaryDF = employeesDF.join(maxSalaryPerEmpNo, "emp_no")
  employeesMaxSalaryDF.show()

  // 2)
  employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"), "left_anti")
    .show()

  // 3)
  val mostRecentJobTitleDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeeDF = employeesMaxSalaryDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobs = bestPaidEmployeeDF.join(mostRecentJobTitleDF, "emp_no")
  bestPaidJobs.show()





}
