package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, desc, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // Select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

//  carsWithWeightsDF.show()

  // select expr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`") // use `` when col name contains spaces
  // remove oolumn
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA") // filter == where
  val americanCars = carsDF.filter("Origin = 'USA'")  // filtering with expression strings
  val americanCars2 = carsDF.where(col("Origin") === "USA")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // union = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works only when DFs have the same schema


  // distinct values
   val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()


  /*
    Exercise
    1) Read the movies json and select two columns
    2) Create another column summing up total profit of movie: US_Gross + Worldwide_gross + DVD_sales
    3) Select all movies which are comedies and IMDB rating above 6

   Use as many versions as possible
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesByYearDF = moviesDF.select(
    col("Title"),
    expr("Release_Date")
  )

  moviesByYearDF.show()

  val totalGrossDF = moviesDF
    .withColumn("Total_Gross1",
      col("US_Gross")
        + col("Worldwide_Gross"))
    .withColumn("Total_Gross2", expr("US_Gross + Worldwide_Gross"))

  totalGrossDF.select("Title", "Total_Gross1", "Total_Gross2").show()

  totalGrossDF.select(
    col("Title"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total")
  ).show()

  moviesDF
    .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    .select("Title", "Major_Genre", "IMDB_Rating", "Release_Date")
//    .orderBy(desc("IMDB_Rating"))
    .show()








}
