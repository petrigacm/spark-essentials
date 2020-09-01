package part3types

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter // this can be used as boolean column in DF

  moviesDF.select("Title").where(dramaFilter)
  val moviesWithGoodnessFLagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

  moviesWithGoodnessFLagsDF.filter("good_movie") // we can filter on column value (true/false)
  moviesWithGoodnessFLagsDF.filter(not(col("good_movie"))).show()

  // Numbers
  // - can be used for mathematical calculation between columns in dataframe

  val moviesAvgRatingDF = moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating") / 2 // if columns are not numerical spark will crash
  )

  // TODO: Check correlation
  // correlation
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an ACTION

  // String
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).show()


  /*
    Exercise
   */
  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val carsRegex = getCarNames.map(_.toLowerCase()).mkString("|")
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), carsRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")

  val carNameFilter = getCarNames
    .map(_.toLowerCase())
    .map(col("Name").contains(_))
    .fold(lit(false))(_ or _)

  carsDF.filter(carNameFilter).show()
}
