package dataframes

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,Row}
import org.apache.spark.sql.types.{StructType,StructField,DoubleType,LongType,StringType}
object DataFrameBasics extends App {

  val conf = new SparkConf().setMaster("local[3]").setAppName("DataFrameBasics")
  /*
  conf.setAppName("YourAppName")
  conf.setMaster("local[2]")
  conf.set("spark.executor.memory", "2g")
  conf.set("spark.executor.cores", "2")
   */

  val spark = SparkSession.builder().config(conf = conf).getOrCreate()

  val car_df = spark.read.format("json").option("inferSchema","true")
    .option("mode","permissive")
    .option("path","file:///C:/Intelij/Scala-Spark/src/main/resources/data/cars.json")
    .load()
  car_df.show(10)
  car_df.printSchema()

  //Get Rows
  val car_collection = car_df.take(10).toList
  car_collection.foreach(x=>println(x))

  // Spark Types
  val car_schema = StructType(Array(
   StructField("Name",StringType,true),
    StructField("Miles_per_Gallon", DoubleType,true),
    StructField("Cylinders", LongType,true),
    StructField("Displacement", DoubleType,true),
    StructField("Horsepower", LongType,true),
    StructField("Weight_in_lbs", LongType,true),
    StructField("Acceleration", DoubleType,true),
    StructField("Year", StringType,true),
    StructField("Origin", StringType,true)
  ))

  // Obtain Existing data frame schema
  val car_df_schema = car_df.schema
  println(car_df_schema)

  // Dataframe with custom schema
  val car_df_with_schema = spark.read.format("json")
    .schema(car_schema).option("mode","permissive")
    .option("path","src/main/resources/data/cars.json")
    .load()

  car_df_with_schema.show(5,false)

  // Create a Row object and Dataframe manually

  val one_row = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // sequence of row object or sequence of tuple
  val car_data = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manual_car_df = spark.createDataFrame(car_data) // Schema Auto inferred
  manual_car_df.show()

  // Df have schemas. Rows Don't
  // Create dataframe with Spark Implicits with column names
  import spark.implicits._
  val manual_cad_df_implicits = car_data.toDF("Name","Miles_per_Gallon","Cylinders","Displacement","Horsepower","Weight_in_lbs","Acceleration","Year","Origin")
  manual_cad_df_implicits.show()

  /**
   * Exercise
   * 1 - Create manual DF describing Smartphones
   *    --- Name,model,processor,battery,screen dimension
   * 2 - Read movies.json file and count no of rows
   */

  val phone_data = Seq(
    ("POCO","X4-Pro","SD-650","5000 Mah",1080),
    ("Motorola","GS-15","SD-650","5000 Mah",720),
    ("IPHONE","15-Pro","IOS","5000 Mah",1080)
  )

  val phone_df = phone_data.toDF("Name","Model","Processor","Battery","Screen Resolution")
  phone_df.show()
  phone_df.printSchema()

  val movie_df = spark.read.format("json")
    .option("mode","permissive").option("inferSchema","true")
    .option("path","src/main/resources/data/movies.json")
    .load()

  movie_df.printSchema()

  println(s"No of rows = ${movie_df.count()}")

  spark.stop()
}
