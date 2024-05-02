package part2dataframes

import org.apache.spark.sql.{SparkSession, functions => func}
import org.apache.spark.sql.functions.{col, expr, lit, max}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  //StructType(StructField(band,LongType,true),StructField(guitars,ArrayType(LongType,true),true),StructField(id,LongType,true),StructField(name,StringType,true))

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.alias("A").join(bandsDF.alias("B"), joinCondition, "inner")

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")


  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  guitaristsBandsDF.select("A.id", "A.band","A.name","A.guitars")

  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */

  val salaries_schema = StructType(Array(
  StructField("emp_no",IntegerType,true),
  StructField("salary",IntegerType,true),
  StructField("from_date",DateType,true),
  StructField("to_date",DateType,true)
  ))

  val salaries_df = spark.read.format("csv")
    .schema(salaries_schema)
    .option("header","false")
    .option("mode","failFast")
    .option("dateFormat","dd-mm-yyyy")
    .option("path","src/main/resources/data/salaries.csv")
    .load()

  val employee_schema = StructType(Array(
    StructField("emp_no",IntegerType,true),
    StructField("birth_date",DateType,true),
    StructField("first_name",StringType,true),
    StructField("last_name",StringType,true),
    StructField("gender",StringType,true),
    StructField("hire_date",DateType,true)
  ))

  val employee_df = spark.read.format("csv")
    .schema(employee_schema)
    .option("header", "false")
    .option("mode", "failFast")
    .option("dateFormat", "dd-mm-yyyy")
    .option("path", "src/main/resources/data/employee.csv")
    .load()

  val max_salary_by_id = salaries_df.select("emp_no","salary").groupBy("emp_no")
    .agg(func.max(col("salary")).alias("Max_Salary"))

  val emp_details_df = employee_df.select(
    col("emp_no"),
    func.concat(col("first_name"),func.lit(" "),col("last_name")).alias("Name")
  )

  val employee_salary_details = max_salary_by_id.alias("a").join(emp_details_df.alias("b"),
    max_salary_by_id.col("emp_no") === emp_details_df.col("emp_no"),
    "inner"
  ).select("a.emp_no","b.name","a.Max_Salary")

  val dept_manager_schema = StructType(Array(
    StructField("dept_no",StringType,true),
    StructField("emp_no",IntegerType,true),
    StructField("from_date",DateType,true),
    StructField("to_date",DateType,true)
  ))

  val dept_manager_df = spark.read.format("csv")
    .schema(dept_manager_schema)
    .option("header", "false")
    .option("mode", "failFast")
    .option("dateFormat", "dd-mm-yyyy")
    .option("path", "src/main/resources/data/dept_manager.csv")
    .load()

  val emp_no_manager = employee_salary_details.alias("a").join(dept_manager_df.alias("b"),
    employee_salary_details.col("emp_no") === dept_manager_df.col("emp_no"),"left_anti")
    .select("a.emp_no","a.name")

  val title_schema = StructType(Array(
    StructField("emp_no", IntegerType, true),
    StructField("title", StringType, true),
    StructField("from_date", DateType, true),
    StructField("to_date", DateType, true)
  ))

  val title_df = spark.read.format("csv")
    .schema(title_schema)
    .option("header", "false")
    .option("mode", "failFast")
    .option("dateFormat", "dd-mm-yyyy")
    .option("path", "src/main/resources/data/titles.csv")
    .load()

  val title_df_de_dupe = title_df.withColumn("rank",func.row_number() over(
    Window.partitionBy(col("emp_no")).orderBy(col("to_date").desc)
  )).filter(col("rank") === 1)

  val top_10_salaried_employee = employee_salary_details.orderBy(col("Max_Salary").desc).limit(10)
  val top_10_mose_paid_profession = top_10_salaried_employee.alias("a").join(title_df_de_dupe.alias("b"),
    top_10_salaried_employee.col("emp_no") === title_df_de_dupe.col("emp_no"),"inner")
    .select("a.emp_no","a.name","a.Max_Salary","b.title")

  top_10_mose_paid_profession.orderBy(col("Max_Salary").desc).show()
  spark.stop()
}
