package dataframes

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataSources extends App {

  val conf = new SparkConf()
    .setMaster("local[3]")
    .setAppName("Data Sources And Formats")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val spark = SparkSession.builder().config(conf=conf).getOrCreate()

  val car_schema = StructType(Array(
    StructField("Name", StringType, true),
    StructField("Miles_per_Gallon", DoubleType, true),
    StructField("Cylinders", LongType, true),
    StructField("Displacement", DoubleType, true),
    StructField("Horsepower", LongType, true),
    StructField("Weight_in_lbs", LongType, true),
    StructField("Acceleration", DoubleType, true),
    StructField("Year", DateType, true),
    StructField("Origin", StringType, true)
  ))

  /**
   *  Reading a Dataframe
   *  - Format
   *  - Schema / inferSchema=true
   *  - 0 or more options ex - mode ---> failFast,dropMalformed,permissive(default)
   */
  val cars_df = spark.read.format("json")
    .schema(car_schema).option("mode", "permissive")
    .option("path", "src/main/resources/data/cars.json")
    .option("dateFormat","yyyy-mm-dd")
    .load()
  // Alternative with options map
  val cars_df_with_option_map = spark.read.format("json")
    .schema(car_schema)
    .options(Map(
      "mode"->"permissive",
      "path"->"src/main/resources/data/cars.json"
    )).load()

  cars_df.printSchema()
  cars_df.show()

  /**
   * Json Flags while reading
   *  spark.read,format("json")
   * .schema(car_schema).option("mode", "permissive")
   * .option("dateFormat","yyyy-mm-dd")
   * .option("allowSingleQuotes","true")
   * .option(compression","uncompressed") // bzip2,gzip,lz4,snappy,deflated
   * .option("dateFormat","yyyy-mm-dd")
   * .load()
   */

  /**
   * CSV Flag while reading
   */
  val stocks_schema = StructType(Array(
    StructField("symbols",StringType,true),
    StructField("date",DateType,true),
    StructField("price",DoubleType,true)
  ))

  val stock_df = spark.read.format("csv")
    .schema(stocks_schema)
    .option("header","true")
    .option("sep",",")
    .option("mode","permissive")
    .option("dateFormat","MMM dd yyyy")
    .option("path","src/main/resources/data/stocks.csv")
    .option("nullValue","")
    .load()

  stock_df.show(10)

  // Reading from Remote database // From postgres
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

//  val employees_df = spark.read
//    .format("jdbc")
//    .option("driver", driver)
//    .option("url", url)
//    .option("user", user)
//    .option("password", password)
//    .option("dbtable", "public.employees")
//    .load()
//
//  employees_df.show(10)

  /**
   *  Writing a dataframe
   *  - Format
   *  - Save Mode - overwrite,append,ignore,errorIfExists
   *  - path
   *  - partition by
   *  - bucket by
   *  - sort by
   */
//  cars_df.write.format("parquet")
//    .mode("overwrite")
//    .option("path","file:///C:/Intelij/Scala-Spark/src/main/resources/data/output/cars_dup")
//    .save()

  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */

  val movie_df = spark.read.format("json")
    .option("mode", "permissive").option("inferSchema", "true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

//  movie_df.write.format("csv")
//    .mode("overwrite")
//    .option("sep","\t")
//    .option("path","file:///C:/Intelij/Scala-Spark/src/main/resources/data/output/movies_dup")
//    .save()

//  movie_df.write.format("parquet")
//    .mode("overwrite")
//    .option("compression","snappy")
//    .option("path","file:///C:/Intelij/Scala-Spark/src/main/resources/data/output/movies_dup")
//    .save()

//  movie_df.write
//    .format("jdbc")
//    .option("driver", driver)
//    .option("url", url)
//    .option("user", user)
//    .option("password", password)
//    .option("dbtable", "public.movies")
//    .save()

  spark.stop()

}
