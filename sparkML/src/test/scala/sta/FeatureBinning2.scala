package sta

import com.niuniuzcd.demo.util.DataUtils
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ArrayBuffer

object FeatureBinning2 extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions._
  import spark.implicits._

  ///user/hive/warehouse/base
  println(s"start load data time:${DataUtils.getNowDate}")
  val test = loadCSVData("csv", "C:\\NewX\\newX\\ML\\docs\\testData\\base.csv")
  println(s"end load time:${DataUtils.getNowDate}")

  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

//  test.show()

  //d14,ad,day7,m1,m3,m6,m12,m18,m24,m60
  println(s"start time:${DataUtils.getNowDate}")
  val cols = "day7,m1,m3,m6,m12,m18,m24,m60"
  val testDf = test.selectExpr(cols.split(","): _*).coalesce(5).cache()

  testDf.show(5, truncate = 0)
  /**
    * +----+----+----+----+----+----+----+----+
    * |day7|m1  |m3  |m6  |m12 |m18 |m24 |m60 |
    * +----+----+----+----+----+----+----+----+
    * |-1.0|2.0 |6.0 |13.0|42.0|48.0|54.0|54.0|
    * |4.0 |5.0 |12.0|21.0|67.0|73.0|80.0|80.0|
    * |3.0 |10.0|25.0|36.0|66.0|68.0|68.0|68.0|
    * |-1.0|16.0|33.0|33.0|33.0|33.0|35.0|35.0|
    * |-1.0|2.0 |7.0 |30.0|33.0|36.0|36.0|36.0|
    * +----+----+----+----+----+----+----+----+
    */

  def getStackParams(s1: String, s2:String*):String ={
    val buffer = StringBuilder.newBuilder
    var size = 0
    if(s1 != null) size =1
    size += s2.length
    buffer ++= s"stack($size, '$s1', $s1"
    for(s <- s2) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
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

  val columns = testDf.columns

  val staDf = testDf.selectExpr(s"${getStackParams(columns:_*)} as (feature, value)")
  println(staDf.count())

  staDf.createOrReplaceTempView("test")
  //concat_ws(';',collect_set(callPhoneArray)) as callPhoneArrays
  //collect_set 将某字段的值进行去重汇总
  //collect_list 对某列不进行去重
 val  sta2df = spark.sql("select feature, concat_ws(',',collect_list(value)) as NewValue from test group by feature")
//  val sta2df = staDf.groupBy("feature").agg(
//    callUDF("concat_ws", lit(","), callUDF("collect_set", $"value")).as("newValue")
//  )

  sta2df.show(100, truncate=0)

  println("good-----------result")
  sta2df.withColumn("newValue", udf{ str:String => {
    val res = for(t <- str.split(",") if str.nonEmpty) yield Tuple1(t)
    val tempdf = spark.createDataFrame(res.toSeq).toDF("f")
    val bucketizer = new QuantileDiscretizer().setInputCol("f").setNumBuckets(10).setRelativeError(0d).setHandleInvalid("skip").fit(tempdf.select($"f".cast(DoubleType)))
    bucketizer.getSplits
  }}.apply(col("newValue"))).show(100, truncate =0)
  println(s"end time:${DataUtils.getNowDate}")
}
