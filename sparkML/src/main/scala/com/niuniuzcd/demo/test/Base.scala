package com.niuniuzcd.demo.test

import com.niuniuzcd.demo.test.TestUtils.df
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object Base extends App {
  import org.apache.spark.sql.functions.{callUDF, col, lit, udf}

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()
  import spark.implicits._
  val df = spark.createDataFrame(Seq(
    (1, 143.5, 5.3, 28),
    (1, 141.5, 2.3, 28),
    (20, 154.2, 5.5, 45),
    (3, Double.NaN, 5.1, 5),
    (4, 144.5, Double.NaN, 33),
    (5, 133.2, 0d, 54),
    (6, 124.1, 5.1, 21),
    (7, 129.2, 5.3, 42)
  )) toDF ("id", "weight", "height", "age")

  // 求众数
  //开窗函数
  //开窗函数格式：
  //row_number() over (partitin by XXX order by XXX) 同个分组内生成连续的序号，每个分组内从 1 开始且排序相同的数据会标不同的号。
  //rank() over (partitin by XXX order by XXX) 同个分组内生成不连续的序号，在每个分组内从 1 开始，同个分组内相同数据标号相同。
  //dense_rank() over (partitin by XXX order by XXX)同个分组内生成连续的序号，在每个分组内从 1 开始，同个分组内相同数据标号相同，之后的数据标号连续
//  var dfNew = df.withColumn("rn",row_number()
//      .over(Window.orderBy("id"))).toDF();

//  dfNew = dfNew.filter(dfNew("rn") > 2).toDF();
//  dfNew = dfNew.filter(dfNew("age") !== "0").groupBy("age").count();
//  val mode = dfNew.orderBy(dfNew("count").desc).first().getAs[Number](0).doubleValue()
//  dfNew.show()
//  println("mode======{}", mode)
  //众数填充，指定列/或者所有列进行填充
//  df.na.fill(mode, df.columns.toSeq).show()

  //多列填充
  //特定列使用自己的数进行填充 ,df.na.fill(Map[String, Any]("pm" -> 0).asJava).show
//  df.na.fill(Map[String, Any]("height" -> mode,"weight"->0.99)).show
//  dfNew = df.select("*")
//    .withColumn("mode", when(df("_c1") === Double.NaN, mode)
//      .otherwise(df("_c1"))).toDF();

  spark.sparkContext.setLogLevel("ERROR")
//  df.na.drop().show()

  // (1).any,只要有一个列NaN就丢弃
//  df.na.drop("any").show()
//  df.na.drop().show()
  // (2).all,所有的列数据都是NaN的行才丢弃
//  df.na.drop("all").show()
  // (3).某些列的规则
//  df.na.drop("any",List("year","month","day","hour")).show()

  //5.填充
  //规则：
  //1.针对所有列数据进行默认值填充
//  df.na.fill(0).show()
  //2.针对特定列填充
//  df.na.fill(0,List("year","month")).show()

  /**
    * 使用Dataset方式 调用agg
    */
//  val ds = df.as[Record]
//  val avg2 = TestAverageUDAF2.toColumn.name("u_age2")
//  ds.select(avg2).show()
//  ds.groupBy("id").agg(avg(ds("age"))).show()


//  @transient val exprs = df.columns.filter(x => !x.equals("id")).map(c => TestAverageUDAF3(c).toColumn.alias(s"avg3($c)"))
//  df.agg(TestAverageUDAF3("age").toColumn.alias(s"xxxx")).show()
//  df.groupBy("id").agg(exprs.head, exprs.tail: _*).show()

//  df.summary().show()
//  df.describe().show()


  //使用ml评估器填充
  import org.apache.spark.ml.feature.Imputer

  val imputer = new Imputer().setStrategy("median").setMissingValue(2.3).setInputCols(Array("weight", "height")).setOutputCols(Array("weight", "height"))


  val model = imputer.fit(df)
  val data = model.transform(df)
  data.show(false)

  /**
    * 使用Dataframe的方式
    */
  //组册函数
//  spark.udf.register("u_avg", TestAverageUDAF1)
//
//  import df.sparkSession.implicits._
//  df.agg(
//    callUDF("u_avg", col("age")).as("u_age"),
//    callUDF("u_avg", $"height").as("u_height")
//  ).show()

  //添加一列
//  df.withColumn("tiger", lit((0.0 to 1.0 by 1.0/10).toArray)).show(truncate = false)
}


