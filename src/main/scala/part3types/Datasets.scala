package part3types


import org.apache.spark.sql.functions.{array_contains, avg, col}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}


object Datasets extends App {

  // Dataset == Typed Dataframe
  // - distributed collection of JVM objects
  // - most useful when we want to maintain type information

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

//  numbersDF.filter(_ > 100) - we cannon do that

  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int] // => distributed collection of Ints
  // ^^^ it works ok wor single column data set
  numbersDS.filter(_ < 100) //

  // what about cars? => dataset of a complex type
  // 1 - define your case class
  // 2 - read DF from the file
  // 3 - define an encoder (importing the implicits)
  // 4 - convert DF to DS
  case class Car(
                Name: String,     // name of variable must be the same as in dataframe
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )
  def readDF(name: String) =
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$name")

  import spark.implicits._
  val carsDF = readDF("cars.json")

  val carsDS = carsDF.as[Car]

  // DS collection function
  numbersDS.filter(_ < 100).show()

  // map, flatMap, reduce...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()

  // !! Avoid when
  // - performance is critical: Spark cannot optimize transformations

  /*
    Exercise
    1) How many cars we have
    2) How many powerful cars we have
    3) Average horsepower
   */

  val carsCount = carsDS.count;
  println(carsCount)
  println(carsDS.filter(_.Horsepower.nonEmpty).flatMap(_.Horsepower).filter(_ > 140).count())
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // we can also use DF functions!
  carsDS.select(avg(col("Horsepower"))).show()

  // Dataset = DataFrame[Row]

  // Joins

  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayerDS
    .joinWith(bandsDS, guitarPlayerDS.col("band") === bandsDS.col("id"), "inner")

  guitarPlayerDS.show()
  guitarPlayerBandsDS.show()

  val guitarPlayerGuitarsDS: Dataset[(GuitarPlayer, Guitar)] = guitarPlayerDS
    .joinWith(guitarsDS, array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "outer")

  guitarPlayerGuitarsDS.show()

  // Grouping Datasets
  val carsByOrigin = carsDS.groupByKey(_.Origin).count()
  carsByOrigin.show()

  // Joins and groups are WIDE transformations, will involve SHUFFLE operation (data exchange between nodes)

}
