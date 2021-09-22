package com.niuniuzcd.demo.test

import com.niuniuzcd.demo.test.Base.df
import com.niuniuzcd.demo.test.TestUtils.{row2ColDf, spark}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.DoubleType

import scala.util.Try

/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object OutlinerProcess extends App {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()


  val df = spark.createDataFrame(Seq(
    (1, 143.5, 5.3, 28),
    (1, 141.5, 2.3, 28),
    (2, 154.2, 5.5, 45),
    (3, null, 5.1, 5),
    (4, 144.5, null, 33),
    (5, 133.2, null, 54),
    (6, 124.1, 5.1, 21),
    (7, 129.2, 5.3, 42)
  )) toDF("id", "weight", "height", "age")

  //row_number() over(partition by xxx order by xxxx desc) xxx
//  var dfNew = df.withColumn("pk", row_number()
//    .over(Window.orderBy("id")).cast("double"))
//  dfNew.show()

  var dfNew = PreProcessTools.genRowNumber(df, "id")

  spark.sparkContext.setLogLevel("ERROR")

  val cols = Array("_rn","weight", "height") //需要同类型

  val  tdf = PreProcessTools.rowColumns2Double(dfNew, cols).get


  //行转列，目的是多特征并行计算
  //staDF.selectExpr("label", s"${Tools.getStackParams(featureCols: _*)} as (feature, value)").coalesce(100).cache()

//  val row2col = dfNew.selectExpr("rn", s"${TestStack.getStackParams(cols: _*)} as (feature, value)").coalesce(100).cache()
  val row2col = PreProcessTools.row2Col(tdf,cols)

  row2col.show()

  //计算分位点值
  //q1, q2   q3
  val qqq = Array(0.25, 0.5, 0.75)

  import org.apache.spark.sql.functions._
  //  import df.sparkSession.implicits._

  //中位数
  //data = data.filter(data("rn") > 2).toDF();
  // var median = data
  //    .withColumn("units",data("value").cast(FloatType))
  //    .select("units")
  //    .stat.approxQuantile("units",Array(0.5),0.0001)
  //    .head;
//  val percentDf = row2col.groupBy("feature").agg(
//    callUDF("percentile_approx", col("value"), lit(qqq)).as("percentile"),
//    avg("value").as("avg") //需要删除 NaN参数
//  )

  val avgFlag = true
  val percentDf = PreProcessTools.percentileApproxOrMean(row2col, avgFlag)

  percentDf.show(false)
  //+-------+--------------------+
  //|feature|          percentile|
  //+-------+--------------------+
  //| height|     [5.1, 5.3, 5.5]|
  //| weight|[129.2, 141.5, 14...|
  //+-------+--------------------+

  //和行转列数据进行join连接
  //joinDF1.join(joinDF2, Seq("id", "name")）
  //joinDF1.join(joinDF2, Seq("id", "name"), "inner"）
  //joinDF1.join(joinDF2 , joinDF1("id" ) === joinDF2( "t1_id"), "inner")
  //统计 row2col.stat.

  val joindf = row2col.join(percentDf, "_feature")
  joindf.show(false)
  //    * IQR = Q3 - Q1
  //    * lower = Q1  - 1.5 * IQR
  //    * upper = Q3 + 1.5 * IQR
  //+-------+---+-----+---------------------+
  //|feature|rn |value|percentile           |
  //+-------+---+-----+---------------------+
  //|weight |1.0|143.5|[129.2, 141.5, 144.5]|
  //|height |1.0|5.3  |[2.3, 5.1, 5.3]      |
  //|weight |2.0|141.5|[129.2, 141.5, 144.5]|

  //分割列
  //joindf.explode( "c3" , "c3_" ){time: String => time.split( " " )}
//  val splitdf = joindf.withColumn("q1", col("percentile").getItem(0))
//    .withColumn("q3", col("percentile").getItem(2))
//
//  splitdf.show(false)

  //dataframe 的乘法的 * 要放到尾部 否则要报 函数overwrite 错误
//  val outlineDF = joindf.withColumn("IQR", col("percentile").getItem(2) - col("percentile").getItem(0))
//    .withColumn("lower", col("percentile").getItem(0) - (col("percentile").getItem(2) - col("percentile").getItem(0)) * 1.5)
//    .withColumn("upper", col("percentile").getItem(2) + (col("percentile").getItem(2) - col("percentile").getItem(0)) * 1.5)


//  val outlineDF = joindf.withColumn("median", col("percentile").getItem(1))
//    .withColumn("lower", col("percentile").getItem(0) - (col("percentile").getItem(2) - col("percentile").getItem(0)) * 1.5)
//    .withColumn("upper", col("percentile").getItem(2) + (col("percentile").getItem(2) - col("percentile").getItem(0)) * 1.5)

  val outlineDF  = PreProcessTools.outlierDf(joindf)
  println("outlineDF===================")
  outlineDF.show()


  val fillDF: DataFrame = PreProcessTools.fillOutlierDf(outlineDF)(avgFlag = true)
//  val fillDF = outlineDF.drop("percentile").withColumn("value",
//      when(col("value") <= col("lower")
//        || col("value") >= col("upper"), lit(Double.NaN)).otherwise(col("value")))

  fillDF.show()

  //data = data.select("*").withColumn("_c1", when(data("_c1") === "NA", 0.0).otherwise(data("_c1"))).toDF();
  //val ReadDf = rawDF.na.replace("columnA", Map( "" -> null));
  //df.withColumn("columnA", when($"columnA" === "", lit(null)).otherwise($"columnA"))
  //计算 outline 值，进行标记，字符串就标记为null，数字类型标记为 Double.NaN



  //列转行
  val res = PreProcessTools.col2Row(fillDF,Array("weight", "height"))

  val col1 = dfNew.columns.filter(name => name != "_rn")
  val col2 = res.columns
  val droCols = col1 intersect  col2

  res.join(dfNew.drop(droCols:_*), "_rn").drop("_rn").show()
  //current
  //+---+------+------+
  //| rn|weight|height|
  //+---+------+------+
  //|1.0| 143.5|   5.3|
  //|2.0| 141.5|   2.3|
  //|3.0| 154.2|   5.5|
  //|4.0|   NaN|   5.1|
  //|5.0| 144.5|   NaN|
  //|6.0| 133.2|   0.0|
  //|7.0| 124.1|   5.1|
  //|8.0| 129.2|   5.3|
  //+---+------+------+
  // origin
  //+---+------+------+---+---+
  //| id|weight|height|age| rn|
  //+---+------+------+---+---+
  //|  1| 143.5|   5.3| 28|1.0|
  //|  1| 141.5|   2.3| 28|2.0|
  //|  2| 154.2|   5.5| 45|3.0|
  //|  3|   NaN|   5.1|  5|4.0|
  //|  4| 144.5|   NaN| 33|5.0|
  //|  5| 133.2|   0.0| 54|6.0|
  //|  6| 124.1|   5.1| 21|7.0|
  //|  7| 129.2|   5.3| 42|8.0|
  //+---+------+------+---+---+
}


