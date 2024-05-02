package dataframes

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => func}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, column, expr, lit}

object ColumnsAndExpressions extends App {

  val conf = new SparkConf()
    .setMaster("local[3]")
    .setAppName("ColumnsAndExpressions")

  val spark = SparkSession.builder().config(conf = conf).getOrCreate()

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
  val carsDF = spark.read.format("json")
    .schema(car_schema).option("mode", "permissive")
    .option("path", "src/main/resources/data/cars.json")
    .option("dateFormat", "yyyy-mm-dd")
    .load()

  val new_cal_df = carsDF.select("name","Miles_per_Gallon")
  new_cal_df.show(5)

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

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

  // select with plain column names
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

  // selectExpr
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
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
//  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
//  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
//  val allCountriesDF = carsDF.select("Origin").distinct()

  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   */

  val moves_df = spark.read.format("json")
    .option("inferSchema","true")
    .option("mode","failFast")
    .option("path","src/main/resources/data/movies.json")
    .load()

  val result_movies_df = moves_df.select("Title","IMDB_Rating","Major_Genre","US_Gross","Worldwide_Gross","US_DVD_Sales")
    .withColumn("Total_Profit",func.coalesce(col("US_Gross"),func.lit(0))
                                       +func.coalesce(col("Worldwide_Gross"),func.lit(0))
                                       +func.coalesce(col("US_DVD_Sales"),func.lit(0))
    ).filter(expr("upper(trim(Major_Genre)) = 'COMEDY' and IMDB_Rating > 6"))

  result_movies_df.orderBy(col("IMDB_Rating").desc).show(5,false)
  
  spark.stop()

}
