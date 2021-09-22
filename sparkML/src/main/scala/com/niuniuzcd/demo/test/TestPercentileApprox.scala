package com.niuniuzcd.demo.test

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object TestPercentileApprox extends App {
//  val colors = Map("test" -> TestTFF)
//  setP(colors)
//
//  def setP(map: Map[String, Any])={
//    val tff = map.get("test").asInstanceOf[TestTFF];
//    val ss = tff.getClass
//    println(ss)
//  }


//  def testStr(dt: String):DataType = {
//    dt match {
//      case "boolean" => BooleanType
//      case "byte" => ByteType
//      case "short" => ShortType
//      case "integer" => IntegerType
//      case "date" => DateType
//      case "long" => LongType
//      case "float" => FloatType
//      case "double" => DoubleType
//      case "decimal" => DoubleType
//      case "binary" => BinaryType
//      case "string" => StringType
//    }
//  }
//
//  val dataType = testStr("integer")
//  println(dataType)


  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.createDataFrame(Seq(
    (1, 143.5, 5.3, 28),
    (2, 154.2, 5.5, 45),
    (3, 342.3, 5.1, 99),
    (4, 144.5, 5.5, 33),
    (5, 133.2, 5.4, 54),
    (6, 124.1, 5.1, 21),
    (7, 129.2, 5.3, 42)
  )) toDF("id", "weight", "height", "age")
  df.createOrReplaceTempView("index")

  val col = "weight"
  val sqlStr =
    s"""
      |select
      |percentile_approx($col,array(0,0.25,0.75,1)) as quartiles
      |from index
      |""".stripMargin

  import  spark.sql

//  println(now())
  val dftemp = sql(sqlStr)
  dftemp.show(5, truncate = false)
//  println(now())


  val qnull  =  quantile(df, col, Array(0,  0.25, 0.5, 0.75, 1)).mkString(",")
  println(qnull)//124.1,131.2,143.5,149.35,342.3

//  println(now())

//  val cols = List("weight", "height", "age")
//
//  for (col <- cols){
//    val q = df
//  }
  //  spark.listenerManager.register()

//  Thread.currentThread().join()


  def quantile(df: DataFrame, col: String, q: Array[Double], filterNa: Boolean = true): Array[Double] = {
    val newDf = df.where(s"$col is not null")
    if (newDf.count() == 0) Array(0.0) else {
      val newDf = if (filterNa) df.select(s"$col").na.drop().sort(df(s"$col")) else df.select(s"$col").sort(df(s"$col"))
      val indexDf = addIndexDf(newDf, col)
      for (at <- q) yield getScore(indexDf, at)
    }
  }

  private def getScore(df: DataFrame, at: Double): Double = {
    val len = df.count()
    val idx = at * (len - 1)
    if (len == 0) None
    if (idx % 1 == 0) getIndexVal(df, idx.toLong)
    else interpolate(getIndexVal(df, idx.toLong), getIndexVal(df, (idx + 1).toLong), idx % 1)
  }

  private def getIndexVal(df: DataFrame, idx: Long): Double = {
    df.filter(df("index") === idx).first().get(0).toString.toDouble * 1.0
  }

  private def interpolate(a: Double, b: Double, fraction: Double): Double = {
    a + (b - a) * fraction
  }

  def addIndexDf(df: DataFrame, f: String): DataFrame = {
    val mRdd = df.na.drop().rdd
    val newRdd = mRdd.mapPartitions(par => par.map(x => x.toString.substring(1, x.toString.length - 1))).zipWithIndex()

    //value-index
    val rowRdd = newRdd.mapPartitions(par => par.map(a => Row(a._1.toDouble, a._2.toInt)))

    val sf = StructField(f, DoubleType, nullable = true)
    val schema = StructType(
      Array(
        sf,
        StructField("index", IntegerType, nullable = true)
      )
    )

    val resDf = df.sparkSession.createDataFrame(rowRdd, schema)
    resDf
  }

  def now(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(now)
  }
}


