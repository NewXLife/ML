package sta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.util.Success

object FeatureBinning3TestCut extends App {
  /**
    * 在數據量很小的時候，有些值落不到分相当中，这个时候，某些分箱就为空，这个时候， 就会漏掉分箱
    */
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
    spark.conf.set("spark.default.parallelism", 100)
    spark.conf.set("spark.sql.shuffle.partitions", 50)
    //    spark.conf.set("spark.driver.maxResultSize","2g")
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", value = true)

  ///user/hive/warehouse/base
  val test = loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\base.csv")
  println(s"total:${test.count()}")
  test.printSchema()

  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

  /**
    * ///user/hive/warehouse/base
    * println(s"start load data time:${DataUtils.getNowDate}")
    * val test = loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\tongdun1.csv").limit(10)
    * println(s"total:${test.count()}")
    *
    * def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    * if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    * else spark.read.format(csv).load(filePath)
    * }
    */

  val labelCol = "d14"
//    val featureCols = test.columns.toBuffer
//    val excludeCol = Array("1d", labelCol,"etl_time","dt")
//    for( col <- excludeCol) featureCols.remove(featureCols.indexOf(col))

val testcols = "day7,m1,m3,m6,m12,m18,m24,m60,test"
//  val testcol = Array("td_1month_platform_count","td_3month_platform_count")
    val row2ColDf = test.withColumnRenamed(labelCol, "label").selectExpr("label", s"${Tools.getStackParams(testcols.split(","): _*)} as (feature, value)").coalesce(100).cache()


    def getBinsArray(bins: Array[Double]): Array[(Double, Double)] = {
      val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
      res.toArray
    }


  /**
    * -----------------------------------------------------
    * val tempDf = row2ColDf.groupBy("feature").agg(
    * callUDF("percentile_approx", $"value", lit((0.0 to 1.0 by 1.0/10).toArray)).as("bin")
    * )
    *
    * val binsArrayDF = tempDf.withColumn("bin", udf{ splits:Seq[Double] =>
    * var buffer = splits.toBuffer
    * buffer(0) =  Double.NegativeInfinity
    * buffer(buffer.length - 1) = Double.PositiveInfinity
    * //    buffer = Double.NegativeInfinity +: buffer
    * buffer =  Double.NegativeInfinity +: buffer.filter(_ >0)
    *       buffer.toArray
    * }.apply(col("bin")))
    */


    val cutObj = new BinningMethod

    val tempDf = row2ColDf.groupBy("feature").agg(
      callUDF("concat_ws", lit(","), callUDF("collect_set", $"value")).as("bins")
    )

    val binsArrayDF = tempDf.withColumn("bins", udf{ inputStr:String => {
      val doubleArray = inputStr.split(",").map( x=> x.toDouble)
      cutObj.cut(doubleArray.min,doubleArray.max, 5)
    }}.apply(col("bins")))

  binsArrayDF.show()

  //none.get
    val row2ColBinsArrayDF = row2ColDf.join(binsArrayDF, Seq("feature"), "left")

  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
    temp
  }

  def binsInnerIndex: UserDefinedFunction = udf{(value: String, binsArray: Seq[Double]) =>
    if(value != null && !value.equals("null") && !value.equals("NULL")){
      val r1=scala.util.Try(value.toDouble)
      val result = r1 match {
        case Success(_) => searchIndex2(value.toDouble, binsArray.toArray)
        case _ => 0
      }
      result
  }else{
      0
    }
  }

  def splitBinning: UserDefinedFunction = udf { (value: String, binsArray: Seq[Double]) =>

    if(value != null && !value.equals("null") && !value.equals("NULL")){
      val r1=scala.util.Try(value.toDouble)
      val result = r1 match {
        case Success(_) =>
          val index = searchIndex2(value.toDouble, binsArray.toArray)
          "(" +binsArray(index - 1).formatted("%.4f")+ ","+binsArray(index).formatted("%.4f") + "]"
        case _ =>
          "("+ Double.NegativeInfinity + "," + Double.PositiveInfinity + ")"
      }
      result
    }else{
      "("+ Double.NegativeInfinity + "," + Double.PositiveInfinity + ")"
    }
  }
  println("row2ColBinsArrayDF-----------")
  row2ColBinsArrayDF.show(20, 500)

    val binsDF = row2ColBinsArrayDF.withColumn("bin", splitBinning($"value", $"bins")).withColumn("bin_inner_index", binsInnerIndex($"value", $"bins")).orderBy("feature","bin_inner_index")
  println("new ---------------bins     ")
  binsDF.show(100, 500)


    val binsResDF = binsDF.groupBy("feature", "bin").agg(
      max("bin_inner_index").as("bin_inner_index"),
      count("value").as("binSamples"),
      min("value").as("min"),
      max("value").as("max"),
      sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
      sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("value").as("binSamples")).as("overdueCountPercent")
    )


    val masterDf = row2ColBinsArrayDF.groupBy("feature").agg(
      count("value").as("totalSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
      sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue") / sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue")).as("totalOverduePercent")
    )

    masterDf.createOrReplaceTempView("master")
    binsResDF.createOrReplaceTempView("bins")

    //   IV，分箱，最小，最大，样本量，样本占比，违约样本量，违约率，lift
    val resDF = spark.sql(
      """
        |select
        |bins.feature as key_field_name,
        |bin,
        |bin_inner_index,
        |min as min_value,
        |max as max_value,
        |binSamples as bins_sample_count,
        |(binSamples / totalSamples) as bins_sample_ratio,
        |overdueCount as overdue_count,
        |overdueCountPercent as overdue_count_ratio,
        |(overdueCountPercent / totalOverduePercent) as lift,
        |log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as woe,
        |((overdueCount / totalOverdue) - (notOverdueCount / totalNotOverdue)) * log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as iv
        |from bins left join master on bins.feature = master.feature
      """.stripMargin)
  resDF.orderBy("key_field_name","bin_inner_index").show(100)
//    DSHandler.save2MysqlDb(resDF, "dataset_statistic_bins_continuous")

    val totalResDf = resDF.groupBy("key_field_name").agg(
      lit("TOTAL").as("bin"),
      min("min_value").as("min_value"),
      max("max_value").as("max_value"),
      sum("bins_sample_count").as("bins_sample_count"),
      lit(1).as("bins_sample_ratio"),
      sum("overdue_count").as("overdue_count"),
      sum("overdue_count_ratio").as("overdue_count_ratio"),
      lit(100).as("lift"),
      lit(0).as("woe"),
      sum("iv").as("iv")
    ).show()
//    DSHandler.save2MysqlDb(totalResDf, "dataset_statistic_bins_continuous")
    "success"
}
