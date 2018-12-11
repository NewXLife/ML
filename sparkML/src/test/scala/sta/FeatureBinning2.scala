package sta

import com.niuniuzcd.demo.util.DataUtils
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ArrayBuffer

object FeatureBinning2 extends App {
  val spark = SparkSession.builder().appName("test-binning").master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
  spark.conf.set("spark.sql.default.parallelism", 100)
  spark.conf.set("spark.sql.shuffle.partitions", 20)
  spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", value = true)
  spark.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions._
  import spark.implicits._

  ///user/hive/warehouse/base
  println(s"start load data time:${DataUtils.getNowDate}")
  val test = loadCSVData("csv", "file:\\D:\\NewX\\ML\\docs\\testData\\base.csv")
  println(s"end load time:${DataUtils.getNowDate}")

  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

  //  test.show()

  //d14,ad,day7,m1,m3,m6,m12,m18,m24,m60
  val cols = "d14,day7,m1,m3,m6,m12,m18,m24,m60"
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

  println(s"start row2coldf time:${DataUtils.getNowDate}")
  //day7,m1,m3,m6,m12,m18,m24,m60
  val columns = Array("day7", "m1", "m3", "m6", "m12", "m18", "m24", "m60")
  val row2ColDf = test.withColumnRenamed("d14", "label").selectExpr("label", s"${getStackParams(columns: _*)} as (feature, value)")
  println(s"end row2coldf time:${DataUtils.getNowDate}")
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

  var temspark: SparkSession = null

  println("good-----------result")
  //  val sta3Df = sta2df.except(sta2df.limit(3))
  println(s"start getbinning time:${DataUtils.getNowDate}")

  def getBinsArray(bins: Array[Double]): Array[(Double, Double)] = {
    val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
    res.toArray
  }

  val binsArrayDF = row2ColDf.groupBy("feature").agg(
    callUDF("concat_ws", lit(","), callUDF("collect_list", $"value")).as("tValue")
  ).withColumn("bin", udf { str: String => {
    val res = for (t <- str.split(",") if str.nonEmpty) yield Tuple1(t)
    if (temspark == null) {
      temspark = SparkSession.builder().master("local[*]").getOrCreate()
    }
    val tempDF = temspark.createDataFrame(res.toSeq).toDF("f").coalesce(5)
    val qd = new QuantileDiscretizer().setInputCol("f").setNumBuckets(10).setHandleInvalid("skip").fit(tempDF.select($"f".cast(DoubleType)))
    var interval = qd.getSplits
    if (interval.map(_ < 0).length >= 2) {
      var t = interval.filter(x => x > 0).toBuffer
      t +:= Double.NegativeInfinity
      interval = t.toArray
    }
    interval
  }
  }.apply(col("tValue"))).coalesce(4).cache()

  if (temspark != null) temspark.stop()
  println(s"end getbinning time:${DataUtils.getNowDate}")

  /**
    * +-------+---------------------------------------------------------------------------+
    * |feature|  bin                                   array(double)                    |
    * +-------+---------------------------------------------------------------------------+
    * |m12    |[-Infinity, 20.0, 28.0, 34.0, 40.0, 46.0, 53.0, 60.0, 69.0, 82.0, Infinity]|
    * |m3     |[-Infinity, 7.0, 11.0, 15.0, 18.0, 22.0, 26.0, 31.0, 37.0, 45.0, Infinity] |
    * |day7   |[-Infinity, -1.0, 2.0, 3.0, 4.0, 5.0, 8.0, Infinity]                       |
    * |m18    |[-Infinity, 22.0, 30.0, 37.0, 44.0, 50.0, 58.0, 66.0, 76.0, 92.0, Infinity]|
    * |m6     |[-Infinity, 12.0, 17.0, 22.0, 27.0, 31.0, 36.0, 42.0, 49.0, 58.0, Infinity]|
    * |m1     |[-Infinity, 2.0, 3.0, 5.0, 6.0, 8.0, 10.0, 12.0, 16.0, 21.0, Infinity]     |
    * +-------+---------------------------------------------------------------------------+
    */

  println(s"start row2coldf join binsarray time:${DataUtils.getNowDate}")
  val row2ColBinsArrayDF = row2ColDf.join(binsArrayDF, Seq("feature"), "left")
  println(s"end row2coldf join binsarray time:${DataUtils.getNowDate}")

  //movies.withColumn("genre", explode(split($"genre", "[|]"))).show  一行切割为多行
  //  fres.withColumn("newValue", explode(split($"newValue","[,]"))).show()
  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
    temp
  }

  println(s"start focus bin join binsarray time:${DataUtils.getNowDate}")
  val binsDF = row2ColBinsArrayDF.withColumn("bin", splitBinning($"value", $"bin"))
  println(s"end focus bin join binsarray time:${DataUtils.getNowDate}")

  println(s"start innerbin index join binsarray time:${DataUtils.getNowDate}")
  val binsResDF = binsDF.groupBy("feature", "bin").agg(
    count("value").as("binSamples"),
    min("value").as("min"),
    max("value").as("max"),
    sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
    sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
    (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("value").as("binSamples")).as("overdueCountPercent")
  ).orderBy("feature", "bin")

  println(s"end innerbin index join binsarray time:${DataUtils.getNowDate}")

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
    val index = searchIndex2(value.toDouble, binsArray.toArray)
    Array(binsArray(index - 1), binsArray(index))
  }

  //   IV，分箱，最小，最大，样本量，样本占比，违约样本量，违约率，lift
  println(s"start master index join binsarray time:${DataUtils.getNowDate}")
  val masterDf = row2ColBinsArrayDF.groupBy("feature").agg(
    count("value").as("totalSamples"),
    sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
    sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
    (sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue") / sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue")).as("totalOverduePercent")
  )
  println(s"end master index join binsarray time:${DataUtils.getNowDate}")
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

  println(s"final join index start time:${DataUtils.getNowDate}")
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
  println(s"final join index  end time:${DataUtils.getNowDate}")

  println(s"final total index  start time:${DataUtils.getNowDate}")
  resDF.groupBy("feature").agg(
    lit("TOTAL").as("bin"),
    max("totalSamples").as("binSamples"),
    lit(1).as("binsSamplesPercent"),
    max("totalOverdue").as("overdueCount"),
    max("totalOverduePercent").as("overdueCountPercent"),
    lit(100).as("liftIndex"),
    lit(0).as("woeV"),
    sum("oneIv").as("IV")
  ).show()
  println(s"final total index  end time:${DataUtils.getNowDate}")

  spark.stop()
}
