package typesanddataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
object CommonTypes extends App {

  val conf = new SparkConf()
    .setAppName("CommonTypes")
    .setMaster("local[3]")

  val spark = SparkSession.builder().config(conf = conf).getOrCreate()
  val moviesDF = spark.read.format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  // Adding a plain value to a dataframe
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")
  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  // Numbers
  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)

  // Strings

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap ---> Camel case, lower, upper
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /**
   * Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *   - contains
   *   - regexes
   */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  def listElemetLoop(myList: List[String]): String = {
    var s = ""
    for(ele <- myList){
      s = s + ele + "|"
    }
    if (s.nonEmpty) s.dropRight(1) else s
  }
  def listElements(myList: List[String],res:String = ""): String = {
    if(myList.isEmpty){
      res
    }else{
      if(myList.length == 1){
        listElements(myList.tail,res = res + myList.head.toLowerCase)
      }else{
        listElements(myList.tail,res = res + myList.head.toLowerCase+"|")
      }
    }
  }
//  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|")
  val myFilter:String = listElements(getCarNames)

//  println(myFilter)
  carsDF.filter(regexp_extract(lower(col("Name")),myFilter,0) =!="").show()

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show

  spark.stop()

}
