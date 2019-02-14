package sta

import com.niuniuzcd.demo.util.{DSHandler, DateUtils}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType}

import scala.collection.mutable.ArrayBuffer

object FeatureBinning2 extends App {

  //自定义spark运行方式
  val spark = SparkSession.builder().appName("test-binning").master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
  spark.conf.set("spark.sql.default.parallelism", 100)
  spark.conf.set("spark.sql.shuffle.partitions", 20)
  spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", value = true)
  spark.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions._
  import spark.implicits._

  ///user/hive/warehouse/base
  /**
    * step 1,load data from hive
    */
//  val test = spark.sql("select * from dw_tmp.lk_statistic_table")

val test = loadCSVData("csv", "C:\\NewX\\newX\\ML\\docs\\testData\\tongdun1.csv").cache().coalesce(10).limit(100)

  val   ttt = test.schema.fields.filter(x => !x.dataType.equals(StringType))
  println(ttt.mkString(","))
//  test.show(20, truncate = 0)
//  test.na.fill("-1").show(20, truncate = 0)


  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }
  //  test.show()

  //d14,ad,day7,m1,m3,m6,m12,m18,m24,m60
//  val cols = "d14,day7,m1,m3,m6,m12,m18,m24,m60"
  //  val cols = "d14,day7,m1"
  //  val testDf = test.selectExpr(cols.split(","): _*).withColumnRenamed("d14", "label").coalesce(5).cache()

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

  def getStackParams(s1: String, s2: String*): String = {
    val buffer = StringBuilder.newBuilder
    var size = 0
    if (s1 != null) size = 1
    size += s2.length
    buffer ++= s"stack($size, '$s1', $s1"
    for (s <- s2) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }

  def getStackParams(s2: String*): String = {
    val buffer = StringBuilder.newBuilder
    var size = 0
    size += s2.length
    buffer ++= s"stack($size "
    for (s <- s2) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }

  //day7,m1,m3,m6,m12,m18,m24,m60
  /**
    * feature columns
    */
  val labelCol = "7d"
  val featureCols = test.columns.toBuffer
  val excludeCol = Array("1d", labelCol,"etl_time","dt")
  for( col <- excludeCol) featureCols.remove(featureCols.indexOf(col))

 //输入所有的连续特征大概453个左右，特征数目可以通过slice 切分
  val row2ColDf = test.withColumnRenamed(labelCol, "label").selectExpr("label", s"${getStackParams(featureCols: _*)} as (feature, value)")
//  row2ColDf.show(20, truncate = 0)
  /**
    * +-----+-------+-----+
    * |label|feature|value|
    * +-----+-------+-----+
    * |0    |day7   |-1.0 |
    * |0    |m1     |2.0  |
    * |0    |day7   |4.0  |
    * |0    |m1     |5.0  |
    * |0    |day7   |3.0  |
    * +-----+-------+-----+
    */
  //  staDf.createOrReplaceTempView("test")
  //concat_ws(';',collect_set(callPhoneArray)) as callPhoneArrays
  //collect_set 将某字段的值进行去重汇总
  //collect_list 对某列不进行去重
  // val  sta2df = spark.sql("select feature, concat_ws(',',collect_list(value)) as NewValue from test group by feature")
//  println(s"start concatValue time:${DataUtils.getNowDate}")
//  val contactValueDF = row2ColDf.groupBy("feature").agg(
//    callUDF("concat_ws", lit(","), callUDF("collect_list", $"value")).as("tValue")
//  )
//  println(s"end concatValue time:${DataUtils.getNowDate}")




  def getBinsArray(bins: Array[Double]): Array[(Double, Double)] = {
    val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
    res.toArray
  }


  val tempDf = row2ColDf.groupBy("feature").agg(
    callUDF("percentile_approx", $"value", lit((0.0 to 1.0 by 1.0/10).toArray)).as("bin")
  )

 tempDf.show()

  val binsArrayDF = tempDf.withColumn("bin", udf{ splits:Seq[Double] =>
    var buffer = splits.toBuffer
    buffer(0) =  Double.NegativeInfinity
    buffer(buffer.length - 1) = Double.PositiveInfinity
//    buffer = Double.NegativeInfinity +: buffer
    buffer =  Double.NegativeInfinity +: buffer.filter(_ >0)
    buffer.toArray
  }.apply(col("bin")))

  binsArrayDF.show(10, truncate = 0)
/**
  *
    * +----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
  * |feature                           |bin                                                                                                                                       |
  * +----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
  * |apply_risk_id                     |[-Infinity, 4.3067906E7, 4.3069972E7, 4.3071247E7, 4.3072516E7, 4.3073812E7, 4.3075087E7, 4.3076123E7, 4.3077652E7, 4.3079182E7, Infinity]|
  * |overdue_days                      |[-Infinity, -2.0, -1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 17.0, Infinity]                                                                    |
  * |td_1month_platform_count          |[-Infinity, 3.0, 5.0, 6.0, 7.0, 10.0, 12.0, 14.0, 18.0, 22.0, Infinity]                                                                   |
  * |td_1month_platform_count_for_model|[-Infinity, 3.0, 5.0, 6.0, 7.0, 10.0, 12.0, 14.0, 18.0, 22.0, Infinity]                                                                   |
  * |td_3month_platform_count          |[-Infinity, 6.0, 9.0, 11.0, 13.0, 15.0, 20.0, 25.0, 30.0, 36.0, Infinity]                                                                 |
  * |td_3month_platform_count_model    |[-Infinity, 6.0, 9.0, 11.0, 13.0, 15.0, 20.0, 25.0, 30.0, 36.0, Infinity]                                                                 |
  * |td_60month_platform_count         |[-Infinity, 17.0, 23.0, 27.0, 31.0, 38.0, 44.0, 50.0, 58.0, 72.0, Infinity]                                                               |
  * |td_6month_platform_count          |[-Infinity, 9.0, 13.0, 17.0, 20.0, 23.0, 28.0, 34.0, 40.0, 46.0, Infinity]                                                                |
  * |td_6month_platform_count_model    |[-Infinity, 9.0, 13.0, 17.0, 20.0, 23.0, 28.0, 34.0, 40.0, 46.0, Infinity]                                                                |
  * |td_7d_euquipment_num              |[-Infinity, -1.0, -1.0, -1.0, -1.0, -1.0, 0.0, 0.0, 0.0, 0.0, Infinity]                                                                   |
  * +----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
    * +
  * good ----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
    * |apply_risk_id                     |[-Infinity, 4.3067906E7, 4.3069972E7, 4.3071247E7, 4.3072516E7, 4.3073812E7, 4.3075087E7, 4.3076123E7, 4.3077652E7, 4.3079182E7, Infinity]|
    * |overdue_days                      |[-Infinity, 1.0, 17.0, Infinity]                                                                                                          |
    * |td_1month_platform_count          |[-Infinity, 3.0, 5.0, 6.0, 7.0, 10.0, 12.0, 14.0, 18.0, 22.0, Infinity]                                                                   |
    * |td_1month_platform_count_for_model|[-Infinity, 3.0, 5.0, 6.0, 7.0, 10.0, 12.0, 14.0, 18.0, 22.0, Infinity]                                                                   |
    * |td_3month_platform_count          |[-Infinity, 6.0, 9.0, 11.0, 13.0, 15.0, 20.0, 25.0, 30.0, 36.0, Infinity]                                                                 |
    * |td_3month_platform_count_model    |[-Infinity, 6.0, 9.0, 11.0, 13.0, 15.0, 20.0, 25.0, 30.0, 36.0, Infinity]                                                                 |
    * |td_60month_platform_count         |[-Infinity, 17.0, 23.0, 27.0, 31.0, 38.0, 44.0, 50.0, 58.0, 72.0, Infinity]                                                               |
    * |td_6month_platform_count          |[-Infinity, 9.0, 13.0, 17.0, 20.0, 23.0, 28.0, 34.0, 40.0, 46.0, Infinity]                                                                |
    * |td_6month_platform_count_model    |[-Infinity, 9.0, 13.0, 17.0, 20.0, 23.0, 28.0, 34.0, 40.0, 46.0, Infinity]                                                                |
    * |td_7d_euquipment_num              |[-Infinity, Infinity]                                                                                                                     |
    * |td_7d_euquipment_num_m            |[-Infinity, Infinity]                                                                                                                     |
    * |td_7day_platform_count            |[-Infinity, 2.0, 3.0, 4.0, 5.0, 8.0, 10.0, Infinity]                                                                                      |
    * |td_7day_platform_count_for_model  |[-Infinity, 2.0, 3.0, 4.0, 5.0, 8.0, 10.0, Infinity]                                                                                      |
    * +----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
  *
  * +----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
  * |feature                           |bin                                                                                                                                       |
  * +----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
  * |apply_risk_id                     |[-Infinity, 4.3067906E7, 4.3069972E7, 4.3071247E7, 4.3072516E7, 4.3073812E7, 4.3075087E7, 4.3076123E7, 4.3077652E7, 4.3079182E7, Infinity]|
  * |overdue_days                      |[-Infinity, 1.0, 17.0, Infinity]                                                                                                          |
  * |td_1month_platform_count          |[-Infinity, 3.0, 5.0, 6.0, 7.0, 10.0, 12.0, 14.0, 18.0, 22.0, Infinity]                                                                   |
  * |td_1month_platform_count_for_model|[-Infinity, 3.0, 5.0, 6.0, 7.0, 10.0, 12.0, 14.0, 18.0, 22.0, Infinity]                                                                   |
  * |td_3month_platform_count          |[-Infinity, 6.0, 9.0, 11.0, 13.0, 15.0, 20.0, 25.0, 30.0, 36.0, Infinity]                                                                 |
  * |td_3month_platform_count_model    |[-Infinity, 6.0, 9.0, 11.0, 13.0, 15.0, 20.0, 25.0, 30.0, 36.0, Infinity]                                                                 |
  * |td_60month_platform_count         |[-Infinity, 17.0, 23.0, 27.0, 31.0, 38.0, 44.0, 50.0, 58.0, 72.0, Infinity]                                                               |
  * |td_6month_platform_count          |[-Infinity, 9.0, 13.0, 17.0, 20.0, 23.0, 28.0, 34.0, 40.0, 46.0, Infinity]                                                                |
  * |td_6month_platform_count_model    |[-Infinity, 9.0, 13.0, 17.0, 20.0, 23.0, 28.0, 34.0, 40.0, 46.0, Infinity]                                                                |
  * |td_7d_euquipment_num              |[-Infinity, Infinity]                                                                                                                     |
  * +----------------------------------+------------------------------------------------------------------------------------------------------------------------------------------+
    */
  //  val qd = new QuantileDiscretizer().setInputCol("f").setNumBuckets(10).setHandleInvalid("skip").fit(tempDF.select($"f".cast(DoubleType)))
//  tempDf.withColumn("bin", udf{})
//callUDF("percentile_approx", $"value", lit((0.0 to 1.0 by 1.0/10).toArray)).as("bins")

//    val tempDf = row2ColDf.groupBy("feature").agg(
//      callUDF("concat_ws", lit(","), callUDF("collect_list", $"value")).as("tValue")
//    )
//
//  var temspark: SparkSession = null
//  val binsArrayDF = tempDf.withColumn("bin", udf { str: String => {
//    val res = for (t <- str.split(",") if str.nonEmpty) yield Tuple1(t)
//    if (temspark == null) {
//      temspark = SparkSession.builder().master("local[*]").getOrCreate()
//    }
//    val tempDF = temspark.createDataFrame(res.toSeq).toDF("f").coalesce(2)
//    val qd = new QuantileDiscretizer().setInputCol("f").setNumBuckets(10).setHandleInvalid("skip").fit(tempDF.select($"f".cast(DoubleType)))
//    var interval = qd.getSplits
//    if (interval.map(_ < 0).length >= 2) {
//      var t = interval.filter(x => x > 0).toBuffer
//      t +:= Double.NegativeInfinity
//      interval = t.toArray
//    }
//    interval
//  }
//  }.apply(col("tValue"))).drop("tValue")
//
//  if (temspark != null) temspark.stop()
////  println(s"end getbinning time:${DataUtils.getNowDate}")
//
//  binsArrayDF.show(20, truncate = 0 )


  println(s"start row2coldf join binsarray time:${DateUtils.getNowDate}")
  val row2ColBinsArrayDF = row2ColDf.join(binsArrayDF, Seq("feature"), "left")
//  println(s"end row2coldf join binsarray time:${DataUtils.getNowDate}")

//  movies.withColumn("genre", explode(split($"genre", "[|]"))).show  一行切割为多行
//    fres.withColumn("newValue", explode(split($"newValue","[,]"))).show()
  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
    temp
  }

//  println(s"start focus bin join binsarray time:${DataUtils.getNowDate}")
  val binsDF = row2ColBinsArrayDF.withColumn("bin", splitBinning($"value", $"bin"))
//  println(s"end focus bin join binsarray time:${DataUtils.getNowDate}")

//  println(s"start innerbin index join binsarray time:${DataUtils.getNowDate}")
  val binsResDF = binsDF.groupBy("feature", "bin").agg(
    count("value").as("binSamples"),
    min("value").as("min"),
    max("value").as("max"),
    sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
    sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
    (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("value").as("binSamples")).as("overdueCountPercent")
  ).orderBy("feature", "bin")

//  println(s"end innerbin index join binsarray time:${DataUtils.getNowDate}")

  /**
    * +-------+--------------------+----------+------------+---------------+--------------------+
    * |feature|            bin     |binSamples|overdueCount|notOverdueCount| overdueCountPercent|
    * +-------+--------------------+----------+------------+---------------+--------------------+
    * |    d14|[-Infinity, Infin...|     29149|        2063|          27086| 0.07077429757453085|
    * |   day7|    [-Infinity, 2.0]|     15268|        1616|          13652|  0.1058422845166361|
    * |   day7|          [2.0, 3.0]|      3892|         192|           3700| 0.04933196300102775|
    * |   day7|          [3.0, 4.0]|      2580|          88|           2492|0.034108527131782945|
    * |   day7|          [4.0, 5.0]|      1935|          50|           1885|0.025839793281653745|
    * --------+--------------------+----------+------------+---------------+---------------------
    */


  //  case class Result ( date: String, usage: Double )
  def splitBinning = udf { (value: String, binsArray: Seq[Double]) =>
    if (value != null && !value.equals("null") && !value.equals("NULL")) {
      val index = searchIndex2(value.toDouble, binsArray.toArray)
      "(" + binsArray(index - 1) + "," + binsArray(index) + ")"
    } else {
      "(" + "missing-value" + ")"
    }
  }

  //   IV，分箱，最小，最大，样本量，样本占比，违约样本量，违约率，lift
//  println(s"start master index join binsarray time:${DataUtils.getNowDate}")
  val masterDf = row2ColBinsArrayDF.groupBy("feature").agg(
    count("value").as("totalSamples"),
    sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
    sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
    (sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue") / sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue")).as("totalOverduePercent")
  )
//  println(s"end master index join binsarray time:${DataUtils.getNowDate}")
  /**
    * +-------+------------+---------------+-------------------+
    * |feature|totalOverdue|totalNotOverdue|totalOverduePercent|
    * +-------+------------+---------------+-------------------+
    * |    d14|        2063|          27086|0.07616480838809717|
    * |   day7|        2063|          27086|0.07616480838809717|
    * |     m1|        2063|          27086|0.07616480838809717|
    * +-------+------------+---------------+-------------------+
    */
  masterDf.createOrReplaceTempView("master")
  binsResDF.createOrReplaceTempView("bins")

  println(s"final join index start time:${DateUtils.getNowDate}")
  val resDF = spark.sql(
    """
      |select
      |bins.feature,
      |bin,
      |min,
      |max,
      |binSamples,
      |totalSamples,
      |(binSamples / totalSamples) as binsSamplesPercent,
      |overdueCount,
      |totalOverdue,
      |totalOverduePercent,
      |notOverdueCount,
      |overdueCountPercent,
      |(overdueCountPercent / totalOverduePercent) as liftIndex,
      |(overdueCount / totalOverdue) as bin2TotalOverduePer,
      |(notOverdueCount / totalNotOverdue) as bin2TotalNotOverduePer,
      |log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as woeV,
      |((overdueCount / totalOverdue) - (notOverdueCount / totalNotOverdue)) * log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as oneIv
      |from bins left join master on bins.feature = master.feature
    """.stripMargin)

//  resDF.show(5)
  //持久化,如csv->hdfs
  //res2.coalesce(10).write.format("csv").save("")

  println(s"final join index  end time:${DateUtils.getNowDate}")

  println(s"final total index  start time:${DateUtils.getNowDate}")
  val res2 = resDF.groupBy("feature").agg(
    lit("TOTAL").as("bin"),
    max("totalSamples").as("binSamples"),
    lit(1).as("binsSamplesPercent"),
    max("totalOverdue").as("overdueCount"),
    max("totalOverduePercent").as("overdueCountPercent"),
    lit(100).as("liftIndex"),
    lit(0).as("woeV"),
    sum("oneIv").as("IV")
  )

  //持久化,如csv->hdfs
  //res2.coalesce(10).write.format("csv").save("")


  println(s"final total index  end time:${DateUtils.getNowDate}")

  spark.stop()
}
