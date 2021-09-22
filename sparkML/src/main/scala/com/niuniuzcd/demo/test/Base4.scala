package com.niuniuzcd.demo.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType


/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object Base4 extends App {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  //  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val userDF = spark.createDataFrame(Seq(
    (1, 143.5, 5.3, 28, "a"),
    (1, 141.5, 2.3, 28, "b"),
    (2, 154.2, 5.5, 45, "c"),
    (3, Double.NaN, 5.1, 5, "d"),
    (4, 144.5, Double.NaN, 33, ""),
    (5, 133.2, 0d, 54, null),
    (6, 124.1, 5.1, 21, "d"),
    (7, 129.2, 5.3, 42, null)
  )) toDF("id", "weight", "height", "age", "str")

  val cols: Array[String]= Array("weight", "height", "age")
  val cols2: Array[String]= Array("id", "str")

  userDF.withColumn("age", col("age").cast("double")).show()

  userDF.select(cols.map(f => col(f).cast(DoubleType)): _*).show()
}


