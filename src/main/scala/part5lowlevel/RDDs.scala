package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {


  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext // low level API

  // 1 - parallelize an existing collections
  val number = 1 to 100000
  val numbersRDD = sc.parallelize(number)

  // 2a - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stockRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

   // 3 - read form a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd


  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers")  // We lose the type information

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD)

  // Transformations

  val msftRDD = stocksRDD.filter(_.symbol == "MSFT")  // lazy transformation
  val msCount = msftRDD.count() // eager ACTION

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // lazy transformation

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  val minMstf = msftRDD.min() // action

  // reducing
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStockRDD = stocksRDD.groupBy(_.symbol) // grouping is very expensive, it requires shuffling


  // Partitioning

  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  // repartitioning is EXPENSIVE. It involves shuffling
  // Best practice: partition EARLY, then process that
  // Size of partition should be 10-100MB


  // Coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does not involve "true" shuffling, half of data stays on their partitions

  /*
    Exercises
   */

  case class Movie(title: String, genre: String, rating: Double)
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    ).where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  val genresRDD = moviesRDD.map(_.genre).distinct()

  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

//  moviesRDD.toDF.show()
//  genresRDD.toDF.show()
//  goodDramasRDD.toDF.show()

  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show()

  /* Reference
+-------------------+------------------+
|          Adventure| 6.345019920318729|
|              Drama| 6.773441734417339|
|        Documentary| 6.997297297297298|
|       Black Comedy|6.8187500000000005|
|  Thriller/Suspense| 6.360944206008582|
|            Musical|             6.448|
|    Romantic Comedy| 5.873076923076922|
|Concert/Performance|             6.325|
|             Horror|5.6760765550239185|
|            Western| 6.842857142857142|
|             Comedy| 5.853858267716529|
|             Action| 6.114795918367349|
+-------------------+------------------+

+-------------------+------------------+
|              genre|            rating|
+-------------------+------------------+
|Concert/Performance|             6.325|
|            Western| 6.842857142857142|
|            Musical|             6.448|
|             Horror|5.6760765550239185|
|    Romantic Comedy| 5.873076923076922|
|             Comedy| 5.853858267716529|
|       Black Comedy|6.8187500000000005|
|        Documentary| 6.997297297297298|
|          Adventure| 6.345019920318729|
|              Drama| 6.773441734417339|
|  Thriller/Suspense| 6.360944206008582|
|             Action| 6.114795918367349|
+-------------------+------------------+

   */

}
