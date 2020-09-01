package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all values except null
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*"))  // count of all rows including rows
//  genresCountDF.show()

  // counting distinct values
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) // approximate row count, it does not do full row scan

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")),
  ).show()

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))  // include nulls
    .count()

//  countByGenreDF.show()

  moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")

  val aggregationsByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenre.show()

  /*
    Exercises:
    1) Sum all the profit of all the movies in DF
    2) Count how many directors we have
    3) Show the mean and standard deviation of US gross revenue for the movies
    4) Compute the average IMDB rating and total US gross revenue PER DIRECTOR
   */

  moviesDF.select(sum(col("US_Gross") + col("Worldwide_Gross"))).show()

  moviesDF.select(countDistinct(col("Director"))).show()

  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross")),
  ).show()


  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Average_Rating"),
      sum("US_Gross").as("Total_Gross")
    )
    .orderBy(col("Average_Rating").desc_nulls_last)
    .show()


  // Aggregation is too expensive because it is wide transformation which means that
  // data are moved between partitions - so do aggregation at the end of computation


}
