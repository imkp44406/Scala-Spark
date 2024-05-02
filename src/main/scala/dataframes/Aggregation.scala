package dataframes

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,functions => func}
import org.apache.spark.sql.functions.{col,expr}


object Aggregation extends App {

  val conf = new SparkConf()
    .setMaster("local[3]")
    .setAppName("Aggregation and Grouping")

  val spark = SparkSession.builder().config(conf = conf).getOrCreate()

  val moviesDF = spark.read.format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  val distinctGenres = moviesDF.select("Major_Genre").distinct()
  distinctGenres.show()

  // Counting
  val genresCount = moviesDF.select(func.countDistinct(col("Major_Genre")).alias("Count")) // all the values except null
  genresCount.show()

  val coumtRecords = moviesDF.select(func.count("*").alias("Count")) // all the values wih null
  coumtRecords.show()

  // approximate count
  moviesDF.select(func.approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(func.min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // data science
  moviesDF.select(
    func.mean(col("Rotten_Tomatoes_Rating")),
    func.stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count() // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      func.count("*").as("N_Movies"),
      func.avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  moviesDF.select(func.sum(
    func.coalesce(col("US_Gross"),func.lit(0)) +
    func.coalesce(col("Worldwide_Gross"),func.lit(0)) +
    func.coalesce(col("US_DVD_Sales"),func.lit(0))
  ).alias("Total_Gross")).show()

  moviesDF.select(func.countDistinct(col("Director"))).show()

  moviesDF.select(
    func.mean(col("US_Gross")).alias("Mean_US_Gross"),
    func.stddev(col("US_Gross")).alias("Stddev_US_Gross")
  ).show()

  moviesDF.select("Director","US_Gross","IMDB_Rating")
    .groupBy("Director")
    .agg(
      func.round(func.avg(col("US_Gross")),2).alias("Avg_US_Gross") ,
      func.round(func.avg(col("IMDB_Rating")),2).alias("Avg_IMDB_Rating")
    ).orderBy(col("Avg_IMDB_Rating").desc)
    .show()

  spark.stop()

}
