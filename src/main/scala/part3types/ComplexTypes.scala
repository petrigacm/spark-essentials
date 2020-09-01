package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY") // temporary fix
  val moviesWithReleaseDates = moviesDF.select(
    col("title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")
  )
  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Now", current_timestamp())
    .withColumn("Age", datediff(col("Today"), col("Actual_Release")) / 365)

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()


  /*
    Exercise
    1) How do we deal with multiple date formats?
      - when spark does not recognize a format it set value to null
      - solution is to parse using all formats we can identify and then union
      ( - or we can ignore invalid date and discard data, it depends on use case)
    2) Read the stock DF and parse dates
   */

  val stockDF = spark.read
    .option("sep", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  stockDF
    .select(
      col("symbol"),
      to_date(col("date"), "MMM dd yyyy").as("Actual_Date"),
      col("price"),
    )


  // Structures

  // 1. - with col operator
  moviesDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Total_Gross")
  ).select(col("Title"), col("Total_Gross").getField("US_Gross").as("US_Profit"))

  // 2. - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWordsDF = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  moviesWithWordsDF.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love").as("About_love")
  ).show()


}
