package ml.feature.encoder

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Locale

import base.BoxLineDiagramTest.spark
import com.niuniuzcd.demo.ml.transformer.{BaseEncoder, CategoryEncoder}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.SparkSession
object BaseEncoderTest extends App {

  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()

  def loadCSVData(csv:String,filePath:String, hasHeader:Boolean=true) ={
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

  import org.joda.time.DateTime

  final val SECOND_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  final val DAY_DATE_FORMAT_ONE = "yyyy-MM-dd"

  final val DAY_DATE_FORMAT_TWO = "yyyyMMdd"
  //时间字符串=>日期
  def convertDateStr2Date(dateStr: String, pattern: String): DateTime = {
    new DateTime(new SimpleDateFormat(pattern).parse(dateStr))
  }
  //时间戳=>日期
  def convertTimeStamp2Date(timestamp: Long): DateTime = {
    new DateTime(timestamp)
  }

  //时间戳=>字符串
  def convertTimeStamp2DateStr(timestamp: Long, pattern: String): String = {
    new DateTime(timestamp).toString(pattern)
  }

//  val f1 = DateTimeFormat.forPattern("yyyyMMdd")
//  val f2 = DateTimeFormat.forPattern("yyyy-MM-dd")
//  val f3 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val f3 = new SimpleDateFormat("yyyyMMdd")
  val f4 = new SimpleDateFormat("yyyy/MM/dd")
  val f5 = new SimpleDateFormat("yyyy-MM-dd")
  val f6 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val f7 = new SimpleDateFormat("yyyy-MM-dd HH")

  val tempDf = loadCSVData("csv", "D:\\NewX\\ML\\docs\\testData\\base2.csv")

  val x = "2015-01-01 00:00:00"
  val xx = "2015-01-01 00"
  println(xx.length)
  val x1 = "20160513"
  val x2 = "2016/05/13"
  val x3 = "2016/5/3"
  val t1 = f5.format(f6.parse(x))
  val t2 = f5.format(f3.parse(x1))
  val t3 = f5.format(f4.parse(x2))
  val t4 = f5.format(f4.parse(x3))

  println("------:", t1)
  println("------:", t2)
  println("------:", t3)
  println("------:", t4)


  val timeFormat = (x:String) => {
    x.length match {
      case 8 if !x.contains("/")=>
        f5.format(f3.parse(x))
      case 10 if x.contains("/") =>
        f5.format(f4.parse(x))
      case 10 if x.contains("-") =>
        f5.format(f5.parse(x))
      case 13  =>
        f5.format(f7.parse(x))
      case 19 =>
        f5.format(f6.parse(x))
    }
  }

  spark.sqlContext.udf.register("timeFormat", timeFormat)

  tempDf.show(5)

  tempDf.createOrReplaceTempView("test")

  spark.sql("select cast(timeFormat(ad) as date) from test where cast(timeFormat(ad) as date) = cast('2018-06-24' as date)").show()
}
