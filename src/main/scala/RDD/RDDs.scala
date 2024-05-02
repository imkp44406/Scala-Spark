package RDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit}

import scala.io.Source
object RDDs extends App {

  val conf = new SparkConf()
    .setMaster("local[3]")
    .setAppName("RDDs")

  val spark = SparkSession.builder().config(conf=conf).getOrCreate()

  val sc = spark.sparkContext

  /**
   * Create RDD
   */
//  1 - Parallelize existing collections
  val nums = 1 to 1000
  val nums_rdd = sc.parallelize(nums)

//  2 - Reading from files
  case class StockValues(company:String,date:String,price:Double)
  def readFromFile(name:String):List[StockValues]={
    Source.fromFile(name)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(ele => StockValues(ele(0),ele(1),ele(2).toDouble))
      .toList
  }

  val stocks_rdd = sc.parallelize(readFromFile("src/main/resources/data/stocks.csv"))
//  2B - Reading from files
  val stocks_rdd2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(row => row.split(","))
    .filter(row => row(0) != "symbol")
    .map(row => StockValues(row(0),row(1),row(2).toDouble))

//  3 - Read from a Dataframe
val stocksDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
//  val stocksDS = stocksDF.as[StockValues]
//  val stocks_rdd3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = nums_rdd.toDF("numbers") // you lose the type info

  // RDD -> DS
  val numbersDS = spark.createDataset(nums_rdd) // you get to keep type info

  val ms_rdd = stocks_rdd2.filter(stocks=> stocks.company == "MSFT")
  val ms_count = ms_rdd.count()
  println(ms_count)

//  Distinct
val companyNamesRDD = stocks_rdd2.map(_.company).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValues] = Ordering.fromLessThan[StockValues]((sa: StockValues, sb: StockValues) => sa.price < sb.price)
  val minMsft = ms_rdd.min() // action

  // Group By operations
  val distinct_cny_rdd = stocks_rdd2.map(stock=>(stock.company,1))
    .reduceByKey((x,y)=>x+y)
  println(distinct_cny_rdd.take(5).toList)
//  OR
  val groupedStocksRDD = stocks_rdd2.groupBy(_.company)
  // ^^ very expensive

  // Partitioning

  val repartitionedStocksRDD = stocks_rdd2.repartition(30)
//  repartitionedStocksRDD.toDF.write
//    .mode("Overwrite")
//    .parquet("src/main/resources/data/stocks30")
  /*
    Repartitioning is EXPENSIVE. Involves Shuffling.
    Best practice: partition EARLY, then process that.
    Size of a partition 10-100MB.
   */

  // coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
//  coalescedRDD.toDF.write
//    .mode("Overwrite")
//    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   *
   * 1. Read the movies.json as an RDD.
   * 2. Show the distinct genres as an RDD.
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   * 4. Show the average rating of movies by genre.
   */
  case class Movie(title: String, genre: String, rating: Double)

  val movie_df = spark.read.format("json")
    .option("mode", "permissive").option("inferSchema", "true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  val movies_req_df = movie_df.select(
    coalesce(col("Title"),lit("UNKNOWN")).alias("title")
    ,coalesce(col("Major_Genre"),lit("UNKNOWN")).alias("genre")
    ,coalesce(col("IMDB_Rating"),lit(0)).alias("rating")
  )
  val movies_ds = movies_req_df.as[Movie]
  val movies_rdd = movies_ds.rdd

  val distinct_genre = movies_rdd.map(movies=>movies.genre).distinct()
  println(distinct_genre.collect().toList)

  val good_drama_movies_rdd = movies_rdd.filter(movies => (movies.genre.toLowerCase == "drama" && movies.rating > 6))
  println(good_drama_movies_rdd.take(5).toList)

  val avg_rating_by_genre = movies_rdd.map(movies=>(movies.genre,(movies.rating,1)))
    .reduceByKey((x,y) => ((x._1+y._1) , (x._2+y._2)))
    .mapValues(v=> BigDecimal(v._1/v._2).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble)

//  println(avg_rating_by_genre.collect().toList)
  avg_rating_by_genre.toDF().show()

  // 4 OR
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = movies_rdd.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  movies_rdd.toDF.groupBy(col("genre")).avg("rating").show

  spark.stop()

}
