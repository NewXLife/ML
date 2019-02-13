package sta

import com.niuniuzcd.demo.util.DateUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}


class StaFlow extends Serializable {
  def timeInteral2Array(startTime: String, endTime: String, timeIntenal: Int): Array[String] = {
    val dateObj = new DateUtils()
    val startTime2 = dateObj.timeFormat2(startTime)
    val endTime2 = dateObj.timeFormat2(endTime)
    dateObj.getTimeRangeArray(startTime2, endTime2, timeIntenal).toArray
  }

  def timeFormat(startTime: Any) = {
    val dateObj = new DateUtils()
    dateObj.timeFormat(startTime)
  }


  val spark = SparkSession.builder().appName("test-binning").master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
  spark.conf.set("spark.sql.default.parallelism", 100)
  spark.conf.set("spark.sql.shuffle.partitions", 20)
  spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", value = true)
  spark.sparkContext.setLogLevel("ERROR")
  spark

  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

  def row2ColDfContainsTimeCol(test: DataFrame, featureCols: Array[String], labelCol: String = "label", timeCol: String): DataFrame = {
    val tools = new Tools()
    test.withColumnRenamed(labelCol, "label").selectExpr("label", timeCol, s"${tools.getStackParams(featureCols: _*)} as (key_field_name, value)").coalesce(100).cache()
  }

  def row2ColDf(test: DataFrame, featureCols: Array[String], labelCol: String = "label"): DataFrame = {
    test.withColumnRenamed(labelCol, "label").selectExpr("label", s"${Tools.getStackParams(featureCols: _*)} as (key_field_name, value)").coalesce(100).cache()
  }

  def String2String(df: DataFrame, colsArray: Array[String]): Try[DataFrame] = {
    import org.apache.spark.sql.functions._
    Try(df.select(colsArray.map(f => col(f).cast("string")): _*))
  }


  def row2ColCrossDf(test: DataFrame, featureCols: Array[String], labelCol: String = "label"): DataFrame = {
    import test.sparkSession.implicits._
    val staCols = featureCols.map(x => x + "_bin")
    val staDf = test.select($"$labelCol", $"${staCols(0)}", $"${staCols(1)}", $"${featureCols(0)}".cast(StringType), $"${featureCols(1)}".cast(StringType))
    staDf.withColumnRenamed(labelCol, "label").selectExpr("label",staCols(0),staCols(1), s"${Tools.getStackParams(featureCols: _*)} as (key_field_name, value)").coalesce(100).cache()
  }


  def initRunningCfg(spark: SparkSession) = {
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
    spark.conf.set("spark.default.parallelism", 100)
    spark.conf.set("spark.sql.shuffle.partitions", 50)
    //    spark.conf.set("spark.driver.maxResultSize","2g")
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", value = true)
  }


  def row2ColDFAggValue(row2ColDf: DataFrame, method: String = "collect_list"): DataFrame = {
    import row2ColDf.sparkSession.implicits._
    row2ColDf.groupBy("key_field_name").agg(
      //method collect_list / collect_set
      callUDF("concat_ws", lit(","), callUDF(method, $"value")).as("tValue")
    )
  }

  def binsInnerIndex: UserDefinedFunction = udf { (value: String, binsArray: Seq[Double]) =>
    if (value != null && !value.equals("null") && !value.equals("NULL")) {
      val r1 = scala.util.Try(value.toDouble)
      val result = r1 match {
        case Success(_) => searchIndex2(value.toDouble, binsArray.toArray)
        case _ => -1
      }
      result
    } else {
      //空值子分箱索引号
      -1
    }
  }

  def cutBins(df: DataFrame): DataFrame = {
    val cutObj = new BinningMethod
    df.withColumn("bin", udf { inputStr: String => {
      val doubleArray = inputStr.split(",").map(x => x.toDouble)
      cutObj.innerCut(doubleArray.min, doubleArray.max, 10)
    }
    }.apply(col("tValue")))
  }

  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
    temp
  }

  def searchIndex2Str(v2: String, array: Array[String]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2.equals(array(i))) temp += 1 else temp
    temp
  }

  def splitBinning: UserDefinedFunction = udf { (value: String, binsArray: Seq[Double]) =>
    if (value != null && !value.equals("null") && !value.equals("NULL")) {
      val index = searchIndex2(value.toDouble, binsArray.toArray)
      "(" + binsArray(index - 1) + "," + binsArray(index) + ")"
    } else {
      "(" + "missing-value" + ")"
    }
  }

  //StabilityBinsTest$
  def splitCrossSubBin: UserDefinedFunction = udf { (value: String, binsArray: String) =>
    var res = "(" + "missing-value" + ")"
    if (value != null && !value.equals("null") && !value.equals("NULL")) {
      var resArray = ArrayBuffer[Double]()
      var resArrayStr = ArrayBuffer[String]()
      val binsStr = binsArray.split(",")
      for (subV <- binsStr) {
        Try(subV.toDouble) match {
          case Success(_) =>
            resArray += subV.toDouble
          case _ =>
            resArrayStr += subV
        }
      }

      if (resArray.nonEmpty) {
        val r1 = scala.util.Try(value.toDouble)
        res = r1 match {
          case Success(_) =>
            val index = searchIndex2(value.toDouble, resArray.toArray)
            "(" + resArray(index - 1).formatted("%.4f") + "," + resArray(index).formatted("%.4f") + "]"
          case _ =>
            "(" + Double.NegativeInfinity + "," + Double.PositiveInfinity + ")"
        }
      }

      if (resArrayStr.nonEmpty) {
        for (bins <- resArrayStr) {
          res = if (bins.contains(value)) bins else res
        }
      }
    }
    res
  }

  //StabilityBinsTest$
  def splitStabilitySubBinBinning: UserDefinedFunction = udf { (value: String, binsArray: Seq[String]) =>
    var res = "(" + "missing-value" + ")"
    if (value != null && !value.equals("null") && !value.equals("NULL")) {
      val pattern = "[()\\[\\]]".r
      for (bins <- binsArray if binsArray != null && binsArray.nonEmpty) {
        val subArray = bins.split(",")
        val start = pattern.replaceAllIn(subArray(0), "")
        res = Try(start.toDouble) match {
          case Success(_) =>
            val end = pattern.replaceAllIn(subArray(1), "")
            if (value.toDouble <= end.toDouble && value.toDouble > start.toDouble) {
              bins
            } else {
              res
            }
          case _ =>
            if (bins.contains(value)) bins else res
        }
        res
      }
    }
    res
  }


  def splitBinningString: UserDefinedFunction = udf { (value: String, binsArray: String) =>
    var res = "(" + "missing-value" + ")"
    if (value != null && !value.equals("null") && !value.equals("NULL")) {
      val timeRangeArray = binsArray.split(";")
      val dateObj = new DateUtils()
      for (subTimeRange <- timeRangeArray) {
        val times = subTimeRange.split(",")
        val startTime = times.head
        val endTime = times.last
        val startTime2 = dateObj.timeFormat2(startTime)
        val endTime2 = dateObj.timeFormat2(endTime)
        val valueDate = dateObj.timeFormat2(value)

        if (valueDate.compareTo(startTime2) >= 0 && valueDate.compareTo(endTime2) < 0)
          res = "(" + startTime + "," + endTime + ")"
        else
          res
      }
    }
    res
  }

  def getBinsDoubleArray(bins: Array[Double]) = {
    val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
    res.toArray
  }


  def getBinsArray[T](bins: Array[T]) = {
    val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
    res.toArray
  }

  def filterSpecialChar(str: String): String = {
    //    val pattern = "[`~!@#$%^&*()+=|{}':;'\\[\\]<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。、？]".r
    val pattern = "[()]".r
    pattern replaceAllIn(str, "")
  }


  def categoriesDefinedBin: UserDefinedFunction = udf { (value: String, bins: Seq[String]) =>
    if (value != null) {
      //    if (value != null && value.toLowerCase != "null") {
      val res = for (bin <- bins.toArray if bin.contains(value)) yield bin
      res.mkString(",")
    }
    else
      "others"
  }


  def categoriesBin: UserDefinedFunction = udf { (value: String, bins: String, binsCount: Int) =>
    if (binsCount <= 3 && value != null && value.toLowerCase != "null" && bins.contains(value))
      value
    else
      "others"
  }

  def binsIndexExcludeMinMaxDF(binsDF: DataFrame): DataFrame = {
    import binsDF.sparkSession.implicits._
    binsDF.groupBy("key_field_name", "bin").agg(
      count("*").as("binSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
      sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("*").as("binSamples")).as("overdueCountPercent")
    )
  }

  def binsIndexTimeRange(binsDF: DataFrame): DataFrame = {
    import binsDF.sparkSession.implicits._
    binsDF.groupBy("key_field_name", "bin", "sta_time_range").agg(
      count("value").as("binSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
      sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("value").as("binSamples")).as("overdueCountPercent")
    )
  }

  def binsIndexDF(binsDF: DataFrame): DataFrame = {
    import binsDF.sparkSession.implicits._
    binsDF.groupBy("key_field_name", "bin").agg(
      count("value").as("binSamples"),
      min("value").as("min"),
      max("value").as("max"),
      sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
      sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("value").as("binSamples")).as("overdueCountPercent")
    )
  }

  def totalIndexTimeRangeDF(row2ColBinsArrayDF: DataFrame): DataFrame = {
    import row2ColBinsArrayDF.sparkSession.implicits._
    row2ColBinsArrayDF.groupBy("key_field_name", "sta_time_range").agg(
      count("value").as("totalSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
      sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue") / sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue")).as("totalOverduePercent")
    )
  }

  def binsIndexCrossDF(binsDF: DataFrame,groupyCols:Array[Column]): DataFrame = {
    import binsDF.sparkSession.implicits._
    binsDF.groupBy(groupyCols:_*).agg(
      count("value").as("binSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount"),
      sum(when($"label" === 0, 1).otherwise(0)).as("notOverdueCount"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("overdueCount") / count("value").as("binSamples")).as("overdueCountPercent")
    )
  }

  def totalIndexCross(row2ColBinsArrayDF: DataFrame, cols:Array[Column]): DataFrame = {
    import row2ColBinsArrayDF.sparkSession.implicits._
    row2ColBinsArrayDF.groupBy(cols:_*).agg(
      count("value").as("totalSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
      sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue") / sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue")).as("totalOverduePercent")
    )
  }


  def totalIndexDF(row2ColBinsArrayDF: DataFrame): DataFrame = {
    import row2ColBinsArrayDF.sparkSession.implicits._
    row2ColBinsArrayDF.groupBy("key_field_name").agg(
      count("*").as("totalSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
      sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
      (sum(when($"label" > 0, 1).otherwise(0)) / count("*")).as("totalOverduePercent")
    )
  }

  def binsExcludeMinMaxTimeRangeIndex(binsDF: DataFrame, masterDF: DataFrame): DataFrame = {
    binsDF.createOrReplaceTempView("bins")
    masterDF.createOrReplaceTempView("master")
    spark.sql(
      """
        |select
        |bins.key_field_name as key_field_name,
        |bin,
        |bins.sta_time_range as sta_time_range,
        |binSamples as bins_sample_count,
        |(binSamples / totalSamples) as bins_sample_ratio,
        |overdueCount as overdue_count,
        |overdueCountPercent as overdue_count_ratio,
        |(overdueCountPercent / totalOverduePercent) as lift
        |from bins left join master on bins.key_field_name = master.key_field_name
      """.stripMargin)
  }

  def binsStabilityExcludeMinMaxTimeRangeIndex(binsDF: DataFrame, masterDF: DataFrame): DataFrame = {
    binsDF.createOrReplaceTempView("bins")
    masterDF.createOrReplaceTempView("master")
    spark.sql(
      """
        |select
        |bins.key_field_name as key_field_name,
        |bin,
        |bins.sta_time_range as sta_time_range,
        |binSamples as bins_sample_count,
        |(binSamples / totalSamples) as bins_sample_ratio,
        |overdueCount as overdue_count,
        |overdueCountPercent as overdue_count_ratio,
        |(overdueCountPercent / totalOverduePercent) as lift
        |from bins left join master on bins.key_field_name = master.key_field_name and bins.sta_time_range = master.sta_time_range
      """.stripMargin)
  }

  def binsExcludeMinMaxIndex(binsDF: DataFrame, masterDF: DataFrame): DataFrame = {
    binsDF.createOrReplaceTempView("bins")
    masterDF.createOrReplaceTempView("master")
    spark.sql(
      """
        |select
        |bins.key_field_name as key_field_name,
        |bin,
        |binSamples as bins_sample_count,
        |(binSamples / totalSamples) as bins_sample_ratio,
        |overdueCount as overdue_count,
        |overdueCountPercent as overdue_count_ratio,
        |(overdueCountPercent / totalOverduePercent) as lift,
        |log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as woe,
        |((overdueCount / totalOverdue) - (notOverdueCount / totalNotOverdue)) * log((overdueCount / totalOverdue) / (notOverdueCount / totalNotOverdue)) as iv
        |from bins left join master on bins.key_field_name = master.key_field_name
      """.stripMargin)
  }


  def binsIndex(binsDF: DataFrame, masterDF: DataFrame): DataFrame = {
    binsDF.createOrReplaceTempView("bins")
    masterDF.createOrReplaceTempView("master")
    spark.sql(
      """
        |select
        |bins.key_field_name as key_field_name,
        |bin,
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
  }

  def totalCategoriesIndex(aggDF: DataFrame): DataFrame = {
    aggDF.groupBy("key_field_name").agg(
      //0定义为自定义总特征的分箱索引号
      lit(0).as("bin_inner_index"),
      sum("bins_sample_count").as("bins_sample_count"),
      lit(1).as("bins_sample_ratio"),
      sum("overdue_count").as("overdue_count"),
      (sum("overdue_count") / sum("bins_sample_count")).as("overdue_count_ratio"),
      lit(100).as("lift"),
      lit(0).as("woe"),
      sum("iv").as("iv")
    )
  }

  def totalIndex(aggDF: DataFrame): DataFrame = {
    aggDF.groupBy("key_field_name").agg(
      //0定义为自定义总特征的分箱索引号
      lit(0).as("bin_inner_index"),
      min("min_value").as("min_value"),
      max("max_value").as("max_value"),
      sum("bins_sample_count").as("bins_sample_count"),
      lit(1).as("bins_sample_ratio"),
      sum("overdue_count").as("overdue_count"),
      sum("overdue_count_ratio").as("overdue_count_ratio"),
      lit(100).as("lift"),
      lit(0).as("woe"),
      sum("iv").as("iv")
    )
  }

  def useBinsContinueTemplate(df: DataFrame, binsArray: Map[String, Array[Double]], newCol: String = "bins", applyCol: String = "feature"): DataFrame = {
    df.withColumn(newCol, udf { f: String =>
      binsArray.filter { case (key, _) => key.equals(f) }.map { case (_, v) => v }.toSeq.flatten.toArray
    }.apply(col(applyCol)))
  }

  def useBinsCategoriesTemplate(df: DataFrame, binsMap: Map[String, Array[String]], newCol: String = "bins", applyCol: String = "key_field_name"): DataFrame = {
    df.withColumn(newCol, udf { f: String =>
      binsMap.filter { case (key, _) => key.equals(f) }.map { case (_, v) => v}.toSeq.flatten.toArray
    }.apply(col(applyCol)))
  }

  def useBinsDefinedBinsTemplate(df: DataFrame, binsMap: Map[String, String], newCol: String = "bins", applyCol: String = "key_field_name"): DataFrame = {
    df.withColumn(newCol, udf { f: String =>
      binsMap.filter { case (key, _) => key.equals(f) }.map { case (_, v) => v }.toSeq.toArray
    }.apply(col(applyCol)))
  }
}

object StaFlow extends StaFlow
