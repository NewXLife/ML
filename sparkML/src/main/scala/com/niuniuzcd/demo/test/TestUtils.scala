package com.niuniuzcd.demo.test

import com.niuniuzcd.demo.test.TestPercentileApprox.spark
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object TestUtils extends App {

  import org.apache.spark.sql.functions.{callUDF, col, lit, udf}

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

//  type pa = (Double, Array[Double]) => Double
//
//  val s: pa = {
//    (x, y) => {
//      if (x > y(0)) 9999.0 else -99.0
//    }
//  }
//
//  def percentComputer(a: Double, b: Array[Double], c: Any = 99.0) = {
//    if (a > b(0)) c else -99.0
//  }

  spark.udf.register("continuouPercentileApprox", new ContinuouPercentileApprox)

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.createDataFrame(Seq(
    (1, 143.5, 5.3, 28),
    (2, 154.2, 5.5, 45),
    (3, 142.3, 5.1, 99),
    (4, 144.5, 5.5, 33),
    (5, 133.2, 5.4, 54),
    (6, 124.1, 5.1, 21),
    (6, 924.1, 505.1, 21),
    (6, 5.1, 0.1, 21),
    (7, 129.2, 5.3, 42)
  )) toDF("id", "weight", "height", "age")

  import df.sparkSession.implicits._

  val featureCols = Array("weight", "height")
  df.createOrReplaceTempView("index")


  val row2ColDf = df.selectExpr(s"${TestStack.getStackParams(featureCols: _*)} as (feature, value)").coalesce(100).cache()
  row2ColDf.show()
  //|feature|value|
  //| weight|143.5|
  //| height|  5.3|
                     //q1, q2   q3
  val qqq = Array(0, 0.25, 0.5, 0.75, 1)
  //    * IQR = Q3 - Q1
  //    * lower = Q1  - 1.5 * IQR
  //    * upper = Q3 + 1.5 * IQR
  import org.apache.spark.sql.functions._
  val rrrr = row2ColDf.groupBy("feature").agg(
    callUDF("percentile_approx", $"value", lit(qqq)).as("percentile"),
    mean("value").as("mean2")
  )


  println("percentile_approx#################")
  //weight [124.1, 129.2, 154.2, 342.3]
  rrrr.show(false)
  //+-------+----------------------------+
  //|feature|percentile                  |
  //+-------+----------------------------+
  //|height |[5.1, 5.1, 5.5, 5.5]        |
  //|weight |[124.1, 129.2, 154.2, 342.3]|
  //+-------+----------------------------+
  val rtt = row2ColDf.join(rrrr, "feature")
  rtt.show()

  //+-------+-----+--------------------+
  //|feature|value|          percentile|
  //+-------+-----+--------------------+
  //| weight|143.5|[5.1, 129.2, 144....|
  //| height|  5.3|[0.1, 5.1, 5.5, 5...|

  rtt.groupBy("feature").agg(
    callUDF("continuouPercentileApprox", col("value"), col("percentile")).as("percentile_approx") //分位点
  ).show(false)

  //+-------+-----+----------------------------+
  //|feature|value|percentile                  |
  //+-------+-----+----------------------------+
  //|weight |143.5|[124.1, 129.2, 154.2, 342.3]|
  //|height |5.3  |[5.1, 5.1, 5.5, 5.5]        |


  //  println("concat_ws#################")
  //  val res = row2ColDf.groupBy("feature").agg(
  //    callUDF("concat_ws", lit(","), callUDF("collect_list", $"value".cast("string"))).as("tValue")
  //  ).repartition(100).cache()
  //
  //  res.show()
  //
  //
  //
  //  rrrr.show(truncate = false)
  //
  //  type RowFilter  = Row => Boolean
  //
  //
  //  /**
  //    * IQR = Q3 - Q1
  //    * lower = Q1  - 1.5 * IQR
  //    * upper = Q3 + 1.5 * IQR
  //    */
  //  val LowerUpper: RowFilter =
  //    row => true
  ////  val IQR: RowFilter  =
  ////    row => false
  ////
  ////  val

}


