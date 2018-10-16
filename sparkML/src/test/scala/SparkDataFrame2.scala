import java.sql.{Date, Timestamp}

import SparkDataFrame.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * create by colin on 2018/7/12
  */
object SparkDataFrame2 extends  App{
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(Seq(("2018-05-23 14:00:00.0",1),("2018-05-23 14:00:00.0",2),("2018-05-23 14:00:00.0",3),("2018-05-23 14:00:00.0",4),("2018-05-23 14:00:00.0",5),("2018-05-23 14:00:00.0",6),("2018-05-23 14:00:00.0",7),("2018-05-23 14:00:00.0",8),("2018-05-23 14:00:00.0",9),("2018-05-23 14:00:00.0",10),("2018-05-23 14:00:00.0",11),("2018-05-23 14:00:00.0",12),("2018-05-23 14:00:00.0",13),("2018-05-23 14:00:00.0",14),("2018-05-23 14:00:00.0",15),("2018-05-23 14:00:00.0",16),("2018-05-23 14:00:00.0",17),("2018-05-23 14:00:00.0",18),("2018-05-23 14:00:00.0",19),("2018-05-15 15:00:00.0",20),("2018-05-10 22:00:00.0",21))).toDF("biz_report_expect_at","overdue_days")

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)


  df.show()

  val t = df.dtypes.filter{case(_, d) => d !="IntegerType"}.map{case (f, _) => f}

  val s = 0.3
  val st = 0.3d



  t.foreach(println(_))

  import spark.implicits._

//
//  val df1 = df.selectExpr("CAST(biz_report_expect_at AS timestamp)", "overdue_days").select("biz_report_expect_at","overdue_days")
//
//  var cols = ArrayBuffer[String]()
//
//  val ar = Array("biz_report_expect_at")
//  cols ++= ar
//
//  df1.drop(cols:_*).show()



//  case class Exa(biz_report_expect_at: Timestamp, overdue_days:Int) extends Serializable
////val people = spark.read.parquet("...").as[Person]
//  val testDs = df1.as[Exa]
//
//  val weights = Array(0.8, 0.2)
//  val res = testDs.randomSplit(weights,10)
//
//  val train_X = res(0).drop("overdue_days")
//  val test_X = res(1).drop("overdue_days")
//
//  val train_Y = res(0).select("overdue_days")
//  val test_Y = res(1).select("overdue_days")
//
//  println("train data----")
//  train_X.show()
//
//  println("train label----")
//  train_Y.show()
//
//  println("test data.....")
//  test_X.show()
//  println("test label....")
//  test_Y.show()

//
//
//  df1.createOrReplaceTempView("test")
//  spark.sql("select * from test where biz_report_expect_at < cast('2018-05-13' as date)").show()
//


//  df1.select("biz_report_expect_at", "overdue_days").where("biz_report_expect_at < cast('2018-05-13 00:00:00' as timestamp)").show()
//
//  val filterField = "biz_report_expect_at"
//  val startDate = "2018-05-10 00:00:00"
//  val endDate = "2018-05-13 00:00:00"
//val sql = s"$filterField > cast ('$startDate' as timestamp) and $filterField < cast('$endDate' as timestamp)"
//  println(sql)
//  df.where(sql).show()


//
//  df1.select("biz_report_expect_at", "overdue_days").where("biz_report_expect_at > cast('2018-05-13' as date) and biz_report_expect_at < cast('2018-05-20' as date)").show()
//
//  val tempDate = "'2018-05-13'"
//  df1.select("biz_report_expect_at", "overdue_days").where(s"biz_report_expect_at < cast($tempDate as date)").show()
//
//  df1.select("biz_report_expect_at", "overdue_days").where($"overdue_days" < 1).show()
//
//  df1.where($"overdue_days" >= 1).show()


//  df.where($"column1" === "" && $"column2" === 34) or
//    df.where($"class" === "").df.where($"id" === 32)
//  df.select($"date".substr(0,10) as "date", $"page")
//  df.sort(asc("column")) or df.sort(desc("column"))



//  You can also manually specify the data source that will be used along with any extra options
//  that you would like to pass to the data source. Data sources are specified by their fully qualified name
//  (i.e., org.apache.spark.sql.parquet),
//  but for built-in sources you can also use their short names
//  (json, parquet, jdbc, orc, libsvm, csv, text). DataFrames loaded from any data source type can be converted into other types
//    using this syntax.

//  val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
//  peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

}
