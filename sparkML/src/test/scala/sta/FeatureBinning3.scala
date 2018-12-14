package sta

import com.niuniuzcd.demo.util.{DSHandler, DataUtils}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import sta.StaTestDFGroupyByKey.{df1, spark}

object FeatureBinning3 extends App {
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
  val test = loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\tongdun1.csv").limit(10)
  println(s"total:${test.count()}")
  test.show(100, truncate = 0)

  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

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

  val labelCol = "7d"
  val featureCols = test.columns.toBuffer
  val excludeCol = Array("1d", labelCol,"etl_time","dt")
  for( col <- excludeCol) featureCols.remove(featureCols.indexOf(col))

  val row2ColDf = test.withColumnRenamed(labelCol, "label").selectExpr("label", s"${getStackParams(featureCols: _*)} as (feature, value)")

  var temspark: SparkSession = null
  def getBinsArray(bins: Array[Double]): Array[(Double, Double)] = {
    val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
    res.toArray
  }

//  val tempDf = row2ColDf.groupBy("feature").agg(
//    callUDF("concat_ws", lit(","), callUDF("collect_list", $"value")).as("tValue")
//  )

  val res = row2ColDf.selectExpr("feature", "cast(value as string)").rdd.map(x => (x.getAs[String](0), x.getAs[String](1))).groupByKey().cache().map(row =>{
    var datas = Seq[Tuple1[String]]()
    for (v <- row._2) {
      if (v != null && v!= "NULL" && v != "null") {
        datas = datas :+ Tuple1(v)
      }
    }
    val sk = SparkSession.builder().master("local[2]").getOrCreate().createDataFrame(datas).toDF("valueField")
    val bucketizer = new QuantileDiscretizer().setInputCol("valueField").setNumBuckets(10).setRelativeError(0d).setHandleInvalid("skip").fit(sk.select($"valueField".cast(DoubleType)))
    Row(row._1, bucketizer.getSplits)
  })

  //  res.collect().foreach(println(_))
  val schema = StructType(Array(StructField("feature",StringType,true),StructField("bin",DataTypes.createArrayType(DoubleType),true)))
  val binsArrayDF = spark.createDataFrame(res, schema)
  println("binsarray dataframe------------------")
  binsArrayDF.show(100, truncate = 0)
  /**
    * +----------------------------------+--------------------------------------------------------------+
    * |feature                           |bin                                                           |
    * +----------------------------------+--------------------------------------------------------------+
    * |td_7day_platform_count_for_model  |[-Infinity, 0.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, Infinity]     |
    * |td_7d_euquipment_num_m            |[-Infinity, 0.0, Infinity]                                    |
    * |td_6month_platform_count          |[-Infinity, 3.0, 14.0, 21.0, 40.0, 44.0, 47.0, 54.0, Infinity]|
    * |td_1month_platform_count_for_model|[-Infinity, 3.0, 7.0, 18.0, 19.0, 27.0, Infinity]             |
    * |overdue_days                      |[-Infinity, -1.0, 0.0, 1.0, Infinity]                         |
    * +----------------------------------+--------------------------------------------------------------+
    */
  row2ColDf.join(binsArrayDF, Seq("feature"), "left").show(10, truncate = 0)
  val row2ColBinsArrayDF = row2ColDf.join(binsArrayDF, Seq("feature"), "left")
  println("row2ColBinsArrayDF---------")
//  row2ColBinsArrayDF.show(5)

  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
    temp
  }

  val binsDF = row2ColBinsArrayDF.withColumn("bin", splitBinning($"value", $"bin"))

  val binsResDF = binsDF.groupBy("feature", "bin").agg(
    count("value").as("binSamples"),
    min("value").as("min"),
    max("value").as("max"),
    sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
    sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
    (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("value").as("binSamples")).as("overdueCountPercent")
  )


  def splitBinning = udf { (value: String, binsArray: Seq[Double]) =>
    if(value != null && !value.equals("null") && !value.equals("NULL")){
      val index = searchIndex2(value.toDouble, binsArray.toArray)
      "(" +binsArray(index - 1)+ ","+binsArray(index) + ")"
    }else{
      "("+ "missing-value" +")"
    }
  }

  println(s"start master index join binsarray time:${DataUtils.getNowDate}")
  val masterDf = row2ColBinsArrayDF.groupBy("feature").agg(
    count("value").as("totalSamples"),
    sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
    sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
    (sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue") / sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue")).as("totalOverduePercent")
  )
  println("masterdf ------------")
  masterDf.show(5)

  masterDf.createOrReplaceTempView("master")
  binsResDF.createOrReplaceTempView("bins")

  //   IV，分箱，最小，最大，样本量，样本占比，违约样本量，违约率，lift
  val resDF = spark.sql(
    """
      |select
      |bins.feature as index_name,
      |bin,
      |min,
      |max,
      |binSamples as bins_sample_count,
      |(binSamples / totalSamples) as bins_sample_percent,
      |overdueCount as overdue_count,
      |overdueCountPercent as overdue_count_percent,
      |(overdueCountPercent / totalOverduePercent) as lift,
      |log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as WOE,
      |((overdueCount / totalOverdue) - (notOverdueCount / totalNotOverdue)) * log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as IV
      |from bins left join master on bins.feature = master.feature
    """.stripMargin)
//  DSHandler.save2MysqlDb(resDF, "bins_index")
  resDF.show(10)

  val totalResDf = resDF.groupBy("index_name").agg(
    lit("TOTAL").as("bin"),
    min("min").as("min"),
    max("max").as("max"),
    sum("bins_sample_count").as("bins_sample_count"),
    lit(1).as("bins_sample_percent"),
    sum("overdue_count").as("overdue_count"),
    sum("overdue_count_percent").as("overdue_count_percent"),
    lit(100).as("lift"),
    lit(0).as("WOE"),
    sum("IV").as("IV")
  ).show()
  println(s"final total index  end time:${DataUtils.getNowDate}")

//  DSHandler.save2MysqlDb(totalResDf, "bins_index")
  spark.stop()
}
