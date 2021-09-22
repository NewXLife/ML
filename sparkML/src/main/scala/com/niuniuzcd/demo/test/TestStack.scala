package com.niuniuzcd.demo.test

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 行转列，列转行   spark中使用pivot函数进行透视
  * Spark没有提供内置函数来实现unpivot操作，不过我们可以使用Spark SQL提供的stack函数来间接实现需求。有几点需要特别注意：
  * 使用selectExpr在Spark中执行SQL片段；
  * 如果字段名称有中文，要使用反引号把字段包起来
  */
object TestStack extends App {
//  def getStackParams(s1: String, s2: String*): String = {
//    val buffer = StringBuilder.newBuilder
//    var size = 0
//    if (s1 != null) size = 1
//    size += s2.length
//    buffer ++= s"stack($size, '$s1', $s1"
//    for (s <- s2) buffer ++= s",'$s', $s"
//    buffer ++= ")"
//    buffer.toString()
//  }

  def  col2row(df: DataFrame) = {
    import org.apache.spark.sql.functions._
//    df.groupBy("sta_date").pivot("item", Seq("item1", "item2", "item3","itemx")).agg(sum("month_number"))
    df.groupBy("sta_date").pivot("item", Seq("item1", "item2", "item3","itemx"))
  }

  def getStackParams(s2:String*):String ={
    val buffer = StringBuilder.newBuilder
    var size = 0
    size += s2.length
    buffer ++= s"stack($size "
    for(s <- s2) buffer ++=  s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }
}


