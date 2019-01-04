package sta

import com.niuniuzcd.demo.util.{DSHandler, DateUtils}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import scala.util.{Success, Try}

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
  println(s"start load data time:${DateUtils.getNowDate}")
  val test = loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\base2.csv")
  val staCols = test.columns.toBuffer
  println("master dataframe----------")
  test.show(100)

  /**
    * +-----+-------+-----+
    * |label|feature|value|
    * +-----+-------+-----+
    * |0.0  |day7   |-1.0 |
    * |0.0  |m1     |2.0  |
    * |0.0  |m3     |6.0  |
    * |0.0  |m6     |13.0 |
    * |0.0  |m12    |42.0 |
    * |0.0  |m18    |48.0 |
    * |0.0  |m24    |54.0 |
    * |0.0  |m60    |54.0 |
    * |0.0  |day7   |4.0  |
    * |0.0  |m1     |5.0  |
    * +-----+-------+-----+
    */


  def String2Double(df: DataFrame, colsArray: Array[String]): Try[DataFrame] = {
    import org.apache.spark.sql.functions._
    Try(df.select(colsArray.map(f => col(f).cast(DoubleType)): _*))
  }

  val  ttcol = "d14,m1,m3"
  val staDf = String2Double(test, ttcol.split(",")).get
  println("------------stadf")
  staDf.show()

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

//  val labelCol = "7d"
//  val featureCols = test.columns.toBuffer
//  val excludeCol = Array("1d", labelCol,"etl_time","dt")
//  for( col <- excludeCol) featureCols.remove(featureCols.indexOf(col))

  val labelName = "d14"
  if(staCols.exists(name => labelName.contains(name)))staCols.remove(staCols.indexOf(labelName))

  val staCols0 = "day7,m1,m3,m6,m12,m18,m24,m60"
  val staCols1 = "m1,m3"
  val staCols2 = ",m12,m18,m24,m60"
  val row2ColDf = staDf.withColumnRenamed("d14", "label").selectExpr("label", s"${getStackParams(staCols1.split(","): _*)} as (feature, value)")

  row2ColDf.show(10, truncate = 0)

  def getBinsArray(bins: Array[Double]): Array[(Double, Double)] = {
    val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
    res.toArray
  }

//  val tempDf = row2ColDf.groupBy("feature").agg(
//    callUDF("concat_ws", lit(","), callUDF("collect_list", $"value")).as("tValue")
//  )

//  val res = row2ColDf.selectExpr("feature", "cast(value as string)").rdd.map(x => (x.getAs[String](0), x.getAs[String](1))).groupByKey().cache().map(row =>{
//    var datas = Seq[Tuple1[String]]()
//    for (v <- row._2) {
//      if (v != null && v!= "NULL" && v != "null") {
//        datas = datas :+ Tuple1(v)
//      }
//    }
//    val sk = SparkSession.builder().master("local[2]").getOrCreate().createDataFrame(datas).toDF("valueField")
//    val bucketizer = new QuantileDiscretizer().setInputCol("valueField").setNumBuckets(10).setRelativeError(0d).setHandleInvalid("skip").fit(sk.select($"valueField".cast(DoubleType)))
//    Row(row._1, bucketizer.getSplits)
//  })
//
//  //  res.collect().foreach(println(_))
//  val schema = StructType(Array(StructField("feature",StringType,true),StructField("bin",DataTypes.createArrayType(DoubleType),true)))
//  val binsArrayDF = spark.createDataFrame(res, schema)
  println("binsarray dataframe------------------")
  def qcut(df: DataFrame, binsNum:Int = 10):DataFrame = {
    import df.sparkSession.implicits._
    val tempDf = df.groupBy("feature").agg(
      callUDF("percentile_approx", $"value", lit((0.0 to 1.0 by 1.0/binsNum).toArray)).as("bin")
    )

    println("tempdf -------------------")
    tempDf.show(20, truncate = 0)

    val binsArrayDF = tempDf.withColumn("bin", udf{ splits:Seq[Double] =>
      if(splits != null){
        var buffer = splits.toBuffer
        buffer(0) =  Double.NegativeInfinity
        buffer(buffer.length - 1) = Double.PositiveInfinity
        buffer =  Double.NegativeInfinity +: buffer.filter(_ >0)
        buffer.distinct.toArray
      }else{
        Array(Double.NegativeInfinity, Double.PositiveInfinity)
      }

    }.apply(col("bin")))

    binsArrayDF
  }

//      val tempDf = row2ColDf.groupBy("feature").agg(
//        callUDF("concat_ws", lit(","), callUDF("collect_list", $"value".cast(StringType))).as("tValue")
//      )
//
//  var temspark: SparkSession = null
//    val binsArrayDF = tempDf.withColumn("bin", udf { str: String => {
//      val res = for (t <- str.split(",") if str.nonEmpty) yield Tuple1(t)
//      if (temspark == null) {
//        temspark = SparkSession.builder().master("local[*]").getOrCreate()
//      }
//      val tempDF = temspark.createDataFrame(res.toSeq).toDF("f").coalesce(2)
//      val qd = new QuantileDiscretizer().setInputCol("f").setNumBuckets(10).setHandleInvalid("skip").fit(tempDF.select($"f".cast(DoubleType)))
//      var interval = qd.getSplits
//      if (interval.map(_ < 0).length >= 2) {
//        var t = interval.filter(x => x > 0).toBuffer
//        t +:= Double.NegativeInfinity
//        interval = t.toArray
//      }
//      interval
//    }
//    }.apply(col("tValue"))).drop("tValue").coalesce(2)
//
//    if (temspark != null) temspark.stop()
  //  println(s"end getbinning time:${DataUtils.getNowDate}")

  val binsArrayDF = qcut(row2ColDf,10)
  println("binsArraydf------------------------")
  binsArrayDF.show(20, truncate = 0 )
  /**
    * +-------+---------------------------------------+
    * |feature|bin                                    |
    * +-------+---------------------------------------+
    * |m3     |[-Infinity, 12.0, 25.0, 33.0, Infinity]|
    * |m6     |[-Infinity, 21.0, 33.0, 36.0, Infinity]|
    * |m1     |[-Infinity, 5.0, 10.0, 16.0, Infinity] |
    * |day7   |[-Infinity, 3.0, 4.0, Infinity]        |
    * +-------+---------------------------------------+
    *
    * +-------+---------------------------------------+
    * |feature|bin                                    |
    * +-------+---------------------------------------+
    * |m3     |[-Infinity, 12.0, 25.0, 33.0, Infinity]|
    * |m6     |[-Infinity, 21.0, 33.0, 36.0, Infinity]|
    * |m1     |[-Infinity, 5.0, 10.0, 16.0, Infinity] |
    * |day7   |[-Infinity, 3.0, 4.0, Infinity]        |
    * +-------+---------------------------------------+
    *
    * +-------+---------------------------------------------------------------------------+
    * |feature|bin                                                                        |
    * +-------+---------------------------------------------------------------------------+
    * |m18    |[-Infinity, 48.0, 48.0, 48.0, 48.0, 48.0, 68.0, 68.0, 73.0, 73.0, Infinity]|
    * |ad     |[-Infinity, Infinity]                                                      |
    * |m12    |[-Infinity, 42.0, 42.0, 42.0, 42.0, 42.0, 66.0, 66.0, 67.0, 67.0, Infinity]|
    * |m3     |[-Infinity, 12.0, 12.0, 12.0, 12.0, 12.0, 25.0, 25.0, 33.0, 33.0, Infinity]|
    * |m60    |[-Infinity, 54.0, 54.0, 54.0, 54.0, 54.0, 68.0, 68.0, 80.0, 80.0, Infinity]|
    * |m6     |[-Infinity, 21.0, 21.0, 21.0, 21.0, 21.0, 33.0, 33.0, 36.0, 36.0, Infinity]|
    * |others |[-Infinity, Infinity]                                                      |
    * |m1     |[-Infinity, 5.0, 5.0, 5.0, 5.0, 5.0, 10.0, 10.0, 16.0, 16.0, Infinity]     |
    * |day7   |[-Infinity, 3.0, 3.0, 4.0, 4.0, Infinity]                                  |
    * |m24    |[-Infinity, 54.0, 54.0, 54.0, 54.0, 54.0, 68.0, 68.0, 80.0, 80.0, Infinity]|
    * +-------+---------------------------------------------------------------------------+
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
      val r1=scala.util.Try(value.toDouble)
      val result = r1 match {
        case Success(_) =>
          val index = searchIndex2(value.toDouble, binsArray.toArray)
          "(" +binsArray(index - 1)+ ","+binsArray(index) + ")"
        case _ =>
          "("+ Double.NaN + "," + Double.NaN + ")"
      }
      result
    }else{
      "("+ Double.NaN + "," + Double.NaN + ")"
    }
  }

  println(s"start master index join binsarray time:${DateUtils.getNowDate}")
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
  println(s"final total index  end time:${DateUtils.getNowDate}")

//  DSHandler.save2MysqlDb(totalResDf, "bins_index")
  spark.stop()
}
