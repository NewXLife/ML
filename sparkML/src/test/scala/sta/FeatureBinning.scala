package sta

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import java.util

import com.niuniuzcd.demo.util.DateUtils

import scala.collection.mutable.ArrayBuffer

object FeatureBinning extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()

  import scala.collection.JavaConversions._
  import spark.implicits._
  import org.apache.spark.sql.functions._

  spark.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions._
  import spark.implicits._

  ///user/hive/warehouse/base
  println(s"start load data time:${DateUtils.getNowDate}")
  val test = loadCSVData("csv", "C:\\NewX\\newX\\ML\\docs\\testData\\base.csv")
  println(s"end load time:${DateUtils.getNowDate}")

  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

  //  test.show()

  //d14,ad,day7,m1,m3,m6,m12,m18,m24,m60
  val cols = "day7,m1,m3,m6,m12,m18,m24,m60"
  val testDf = test.selectExpr(cols.split(","): _*).coalesce(5).cache()


  println(s"start time:${DateUtils.getNowDate}")
  val res = testDf.rdd.flatMap(row => {
    var rows = ArrayBuffer[(String, String)]()
    for (fieldName <- row.schema.fieldNames) {
      var value = row.getAs[String](fieldName)
      if (value != null) {
        val temp = value.toString
        if (temp == null || temp.trim.isEmpty) value = null
        rows +:= (fieldName, value)
      }
    }
    rows.iterator
  })

  val res1 = res.groupByKey().cache()
    .map(row => {
      var datas = Seq[Tuple1[String]]()
      for (v <- row._2) {
        if (v != null) {
          datas = datas :+ Tuple1(v)
        }
      }
      //        val schema = StructType(Array(StructField("valueField", DoubleType, nullable = true)))
      val sk = SparkSession.builder().master("local[2]").getOrCreate().createDataFrame(datas).toDF("valueField")
      val bucketizer = new QuantileDiscretizer().setInputCol("valueField").setNumBuckets(10).setRelativeError(0d).setHandleInvalid("skip").fit(sk.select($"valueField".cast(DoubleType)))
      var res = row._1
      for (d <- bucketizer.getSplits) {
        println(d)
        res = res + "" + d
      }
      res
    }).collect()
  println(s"end time:${DateUtils.getNowDate}")

  var temp = ""
  for (item <- res) {
    println("temp:", item)

    /**
      * (temp:,day7-Infinity-1.02.03.04.05.08.0Infinity)
      * (temp:,m24-Infinity23.031.038.045.052.059.068.078.094.0Infinity)
      * (temp:,m18-Infinity22.030.037.044.050.058.066.076.092.0Infinity)
      * (temp:,m6-Infinity12.017.022.027.031.036.042.049.058.0Infinity)
      */
    temp = temp + item + "\n"
  }
  println(temp)

}
