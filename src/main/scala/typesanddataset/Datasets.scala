package typesanddataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col}

object Datasets extends App {

  val conf = new SparkConf()
    .setMaster("local[3]")
    .setAppName("Datasets")

  val spark = SparkSession.builder().config(conf = conf).getOrCreate()

  val numbers_df = spark.read.format("csv").option("header","true").option("inferSchema","true")
    .option("path","src/main/resources/data/numbers.csv").load()

  numbers_df.filter(col("numbers") <= "snau")
//    .show() /// This will not throw any error

  implicit val intEncoder = Encoders.scalaInt
  val numbers_ds = numbers_df.as[Int]

//  numbers_ds.filter(x=> x<="sanu")  // Will throw error at compile time
  numbers_ds.filter(x=> x<=100)
//    .show()

  val car_df = spark.read.format("json").option("inferSchema", "true")
    .option("mode", "permissive")
    .option("path", "file:///C:/Intelij/Scala-Spark/src/main/resources/data/cars.json")
    .load()

//  implicit val carEncoder = Encoders.product[Cars]  ---> One way to create an jvm object encoder OR we can import spark.implicits
  import spark.implicits._
  val cars_ds = car_df.as[Cars]

  cars_ds.map(car => Cars(car.Name.toUpperCase,car.Miles_per_Gallon,car.Cylinders,car.Displacement,car.Horsepower,car.Weight_in_lbs,car.Acceleration,car.Year,car.Origin))
//    .show(5)

  /**
   * Exercises
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

  println(cars_ds.count())
  println(cars_ds.filter(car => car.Horsepower.getOrElse(0L) >= 140).count())
  println(cars_ds.map(car => car.Horsepower.getOrElse(0L)).reduce( (x,y) => x+y )/cars_ds.count())

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")
  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /**
   * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)
   */

  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // Grouping DS

  val carsGroupedByOrigin = cars_ds
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

  spark.stop()

}
case class Cars(
                 Name:String,
                 Miles_per_Gallon:Option[Double],
                 Cylinders:Long,
                 Displacement:Double,
                 Horsepower:Option[Long],
                 Weight_in_lbs:Long,
                 Acceleration:Double,
                 Year:String,
                 Origin:String
               )


