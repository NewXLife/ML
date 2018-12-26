package sta

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object StaFlow {
  def timeInteral2Array(startTime: String, endTime: String, timeIntenal:Int) = {
    
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

  def row2ColDfContainsTimeCol(test: DataFrame, featureCols: Array[String], labelCol: String = "label", timeCol:String): DataFrame = {
      test.withColumnRenamed(labelCol, "label").selectExpr("label",timeCol, s"${Tools.getStackParams(featureCols: _*)} as (key_field_name, value)").coalesce(100).cache()
  }

  def row2ColDf(test: DataFrame, featureCols: Array[String], labelCol: String = "label"): DataFrame = {
      test.withColumnRenamed(labelCol, "label").selectExpr("label", s"${Tools.getStackParams(featureCols: _*)} as (key_field_name, value)").coalesce(100).cache()
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

  def cutBins(df: DataFrame): DataFrame = {
    val cutObj = new BinningMethod
    df.withColumn("bin", udf { inputStr: String => {
      val doubleArray = inputStr.split(",").map(x => x.toDouble)
      cutObj.cut(doubleArray.min, doubleArray.max, 10)
    }
    }.apply(col("tValue")))
  }

  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
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

  def categoriesDefinedBin: UserDefinedFunction = udf { (value: String, bins: Seq[String]) =>
    if(value!= null && value.toLowerCase != "null"){
    val res =   for(bin <- bins.toArray if bin.contains(value)) yield bin
      res.mkString(",")
    }
    else
      "others"
  }


  def categoriesBin: UserDefinedFunction = udf { (value: String, bins: String, binsCount:Int) =>
   if(binsCount <= 3 && value!= null && value.toLowerCase != "null" && bins.contains(value))
     value
    else
     "others"
  }

  def binsIndexExcludeMinMaxDF(binsDF: DataFrame): DataFrame = {
    import binsDF.sparkSession.implicits._
    binsDF.groupBy("key_field_name", "bin").agg(
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

  def totalIndexDF(row2ColBinsArrayDF: DataFrame): DataFrame = {
    import row2ColBinsArrayDF.sparkSession.implicits._
    row2ColBinsArrayDF.groupBy("key_field_name").agg(
      count("value").as("totalSamples"),
      sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue"),
      sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue"),
      (sum(when($"label" > 0, 1).otherwise(0)).as("totalOverdue") / sum(when($"label" === 0, 1).otherwise(0)).as("totalNotOverdue")).as("totalOverduePercent")
    )
  }

  def binsExcludeMinMaxIndex(binsDF:DataFrame, masterDF:DataFrame): DataFrame = {
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


  def binsIndex(binsDF:DataFrame, masterDF:DataFrame): DataFrame = {
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
      sum("overdue_count_ratio").as("overdue_count_ratio"),
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

  def useBinsContinueTemplate(df: DataFrame, binsArray: Map[String, Array[Double]], newCol:String = "bins", applyCol:String = "feature"): DataFrame = {
    df.withColumn(newCol, udf { f: String =>
      binsArray.filter { case (key, _) => key.equals(f) }.map { case (_, v) => v }.toSeq.flatten.toArray
    }.apply(col(applyCol)))
  }

  def useBinsCategoriesTemplate(df: DataFrame, binsMap: Map[String, String], newCol:String = "bins", applyCol:String = "key_field_name"): DataFrame = {
    df.withColumn(newCol, udf { f: String =>
      binsMap.filter { case (key, _) => key.equals(f) }.map { case (_, v) => v.split(",") }.toSeq.flatten.toArray
    }.apply(col(applyCol)))
  }
  }
