package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFramesBasics.spark

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data source and formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
   Reading a DF
    - format
    - schema or inferSchema = true
    - zero or more options
   */

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // or dropMalformed (drop malformed rows) or permissive (default)
//    .option("path", "src/main/resources/data/cars.json").load()
    .load("src/main/resources/data/cars.json") // file on compute r or every else

  carsDF.show()

  val carsDfWithOptionsMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
    Writing DFs
    - format
    - save mode = override, append, ignore, errorIfExists
    - path
    - zero or more options
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")


  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // works only with specified schema; if spark fails parsing it will put null
    .option("allowSingleQuotes", "true") // when we use single quotes in json
    .option("compression", "uncompressed") // uncompressed(default), bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")


  // CSV flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYY") // works only with specified schema; if spark fails parsing it will put null
    .option("header", "true")  // header may or may not be present
    .option("sep", ",") // separator, "," is default
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet [parkej] - default storage format for dataframes
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  // This is common pattern
  // Data are often migrated fromDB to spark for processing
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /*
    Exercise
    1) read movies DF and write it as
        - tab-separated values file
        - snappy parquet
        - table public movies in the Postgres DB
   */
  val moviesDF = spark.read
//    .option("inferSchema", "true")  inferSchema == true if not specified
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  moviesDF.write
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .mode(SaveMode.Overwrite) // default is ErrorIfExists
    .save("src/main/resources/data/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .mode(SaveMode.Overwrite)
    .save()


}
