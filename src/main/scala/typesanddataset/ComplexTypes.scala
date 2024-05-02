package typesanddataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,functions => func}
import org.apache.spark.sql.functions.{col,expr}
import org.apache.spark.sql.types._
object ComplexTypes extends App {

  val conf = new SparkConf()
    .setAppName("CommonTypes")
    .setMaster("local[3]")
    .set("spark.sql.legacy.timeParserPolicy","LEGACY")

  val spark = SparkSession.builder().config(conf = conf).getOrCreate()

  val moviesDF = spark.read.format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  moviesDF.select(col("Title"),col("Release_Date")).withColumn("Release_Date",
    func.to_date(col("Release_Date"),"dd-MMM-yy"))
    .withColumn("Today",func.current_date())
    .withColumn("Current Time",func.current_timestamp())
    .withColumn("Age",func.round(func.datediff(col("Today"),col("Release_Date"))/365,2))
//    .show(5,false)

  /**
   * Exercise
   * 1. How do we deal with multiple date formats? 1 - parse the DF multiple times, then union the small DFs
   * 2. Read the stocks DF and parse the dates
   */

  val stocks_schema = StructType(Array(
    StructField("symbols", StringType, true),
    StructField("date", StringType, true),
    StructField("price", DoubleType, true)
  ))

  val stock_df = spark.read.format("csv")
    .schema(stocks_schema)
    .option("header", "true")
    .option("sep", ",")
    .option("mode", "permissive")
    .option("path", "src/main/resources/data/stocks.csv")
    .option("nullValue", "")
    .load()

  stock_df
    .withColumn("date",func.to_date(col("date"),"MMM dd yyyy"))
//    .show(5)
  // 1 - with col operators
  moviesDF
    .select(col("Title"), func.struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), func.split(col("Title"), " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // indexing
    func.size(col("Title_Words")), // array size
    func.array_contains(col("Title_Words"), "Love") // look for value in array
  )

  spark.stop()
}
