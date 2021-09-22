package com.niuniuzcd.demo.test

import org.apache.spark.sql.functions.{avg, col, sum}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object Base3 extends App {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  //  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.createDataFrame(Seq(
    (1, 143.5, 5.3, 28, "a"),
    (1, 141.5, 2.3, 28, "b"),
    (2, 154.2, 5.5, 45, "c"),
    (3, Double.NaN, 5.1, 5, "d"),
    (4, 144.5, Double.NaN, 33, ""),
    (5, 133.2, 0d, 54, null),
    (6, 124.1, 5.1, 21, "d"),
    (7, 129.2, 5.3, 42, null)
  )) toDF("id", "weight", "height", "age", "str")


//  val  percentA = Array(0.25, 0.5, 0.75)
//  println(percentA.indexOf(0.25))
//  println(percentA.indexOf(0.5))
//  println(percentA.indexOf(0.75))
//
//  df.describe().show() //不带分为点

  //如果有NaN，那么均值也会是NaN，因此需要删除NaN
  //如何使用中位数填充，不去掉NaN，如何使用Mean填充需要去掉NaN
  df.summary().show() //带分位点,

  //  import df.sparkSession.implicits._

//  val fillMap = Map("col1" -> 0.3, "col2" -> 0.4)
//  val methodMap = Map("mean" -> fillMap)

  val method = "mean" //mean/median
  if (method == "mean") {
    val summary = df.na.drop().summary()
    val indexMap = fillMap(summary, method)
    println("#############")
    println(indexMap)
  } else if (method == "median") {
    val summary = df.summary()
    val indexMap = fillMap(summary, method)
    println("#############")
    println(indexMap)
  }
  //  summary.createOrReplaceTempView("t_summary")
  //  import spark.sql
  //
  //  val method = "25%"
  //  sql(s"select height from t_summary  where summary = '$method'").show()


  def fillMap(df: DataFrame, method: String):Map[String, String] = {
    val innerMethod = if(method == "median") "50%" else method
    val indexMap = scala.collection.mutable.Map[String,String]()
    for (col <- df.columns if(col != "summary")) {
      val mean = df.select(df(col)).filter(s"summary = '$innerMethod'").head().getString(0)
      indexMap += (col -> mean)
    }
    indexMap.toMap
  }
}


