package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  // Creating a spark session
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // Read a dataframe
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true") // do not use inferSchema in production, define own schema, see below
    .load("src/main/resources/data/cars.json")

  // show a DF
  firstDF.show()
  firstDF.printSchema()

  // Dataframe - distributed collection
  // every row is array of data which can be numbers, dates, strings etc.

  // get rows
  firstDF.take(10).foreach(println)

  // spark type
  val longType = LongType

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDFSchema = firstDF.schema
//  println(carsDFSchema)


  // read DF with own schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF form tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars)  // schema auto-inferred

  // DF has schema, rows do not

  // create DF with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinder", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /*
  Exercise:
    1) Create a manual DF describing smartphones
      - make
      - model
      - screen dimension
      - camera megapixels

    2) Read another file form data folder, eg movies.json
      - print its schema
      - count the number of rows
   */

  val smartPhones = Seq(
    ("Apple", "IPhone", 5.5, 11),
    ("Samsung", "Galaxy", 6.5, 22),
  )
  val smartPhonesDF = smartPhones.toDF("Make", "Model", "Screen dimension", "Camera megapixels")
  smartPhonesDF.show()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true") // do not use inferSchema in production, define own schema
    .load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(moviesDF.count)

  // Dataframes are like distributed spreadsheet which is distributed to multiple nodes in spark nodes
  // the information which each spark nodes receives is the schema od the dataframe and a few of the rows from dataframe
  //  - types are not available in compile time, we will see later how to create type-save dataframe
  //  - arbitrary number of columns
  //  - all rows have the same structure
  //
  // DataFrame must be distributed
  //  - data too big for a single computer
  //  - to long to process the entire data on single cpu
  //
  // Partitioning - data are split into files and than distributed into notes in cluster
  //
  // Dataframes are immutable
  //
  // Dataframes transformations:
  //  - narrow = one input partition contributes to at most one output partition (e.g. map)
  //  - wide = input partitions (one or more) create many output partitions (e.g. sort)
  //
  // Shuffle - data exchange between cluster noted
  //  - occurs in wide transformations
  //  - massive perf topic
  //
  // Spark evaluates transformations lazy
  //
  // Spark compiles the Dataframe to graph before running any code - so all steps are known before computation starts
  // There are two plans:
  //  - logical plan = DF dependency graph + narrow/wide transformation sequence
  //  - physical plan = optimize sequence of steps for noted in cluster so it knows which node executes which transformations
  // As Spark know all plans it includes optimization like avoid processing data multiple times, chaining predicates etc.
  //
  // Transformation vs Actions
  // Transformations - describe how new data are obtained e.g. map
  // Actions - start spark execution e.g. show, count, etc.





}
