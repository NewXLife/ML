package com.niuniuzcd.demo.test

import com.niuniuzcd.demo.test.Base.spark
import com.niuniuzcd.demo.test.OutlinerProcess.df
import com.niuniuzcd.demo.test.TestUtils.{df, featureCols}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object Base2 extends App {
//
//  val spark = SparkSession
//    .builder
//    .master("local[4]")
//    .appName("StructuredNetworkWordCount")
//    .getOrCreate()

  import spark.implicits._

//  val df = spark.createDataFrame(Seq(
//    (1, 143.5, 5.3, 28),
//    (1, 141.5, 2.3, 28),
//    (1, 154.2, 5.5, 45),
//    (3, Double.NaN, 5.1, 5),
//    (3, 144.5, Double.NaN, 33),
//    (5, 133.2, 0d, 54),
//    (6, 124.1, 5.1, 21),
//    (7, 129.2, 5.3, 42)
//  )) toDF("id", "weight", "height", "age")
//

  val t1 = Array("weight", "height", "age")
  val pk = "id"

  val rr = if(t1.contains(pk)) t1 else (pk +: t1)
  println(rr.mkString(","))
//  println(df.dtypes.mkString(","))
//  val numericTypeString = Array("ByteType", "DecimalType", "DoubleType", "FloatType", "IntegerType", "LongType", "ShortType")
//  val firstNumberColName = df.dtypes.find( f => numericTypeString.contains(f._2)).get
//  println(firstNumberColName._1)
//  PreProcessTools.genRowNumber(df, "id").show()

//  val cols = Array("id","weight", "height","age")
//  val cols2 = Array("rn","weight", "height")
//
//  val ss = cols intersect  cols2
//  val ss2 = cols2 diff cols
//  println(ss.mkString(","))
//  println(ss2.mkString(","))



//  val row2col = df.selectExpr(s"${TestStack.getStackParams(cols:_*)} as (feature, value)").coalesce(100).cache()
//
//  row2col.show()

  //  val df2 = row2col.withColumn("sta_date", col("2020-05-13"))

  //  df2.groupBy("sta_date").pivot("item", Seq("item1", "item2", "item3","itemx"))
}


