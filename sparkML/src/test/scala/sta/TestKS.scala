package sta

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataTypes, DoubleType, StringType}

import scala.collection.mutable.ArrayBuffer
case class Ks(index_name: String, KS: Double)

case class AggregateResultModel(lable: String,feature:String, value:String)
private[sta] object TestKS extends App{

  val spark = SparkSession.builder().appName("test-binning").master("local[*]").getOrCreate()
  import spark.implicits._
  spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
  spark.conf.set("spark.sql.default.parallelism", 100)
  spark.conf.set("spark.sql.shuffle.partitions", 20)
  spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", value = true)
  spark.sparkContext.setLogLevel("ERROR")

  val test = loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\base.csv")

  val featureCols = Array("day7","m1","m3","m6","m12","m18","m24","m60")


  val row2ColDf = test.withColumnRenamed("d14", "label").selectExpr("label", s"${Utils.getStackParams(featureCols: _*)} as (feature, value)")

  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }

  row2ColDf.show()
//  row2ColDf.as[AggregateResultModel].groupByKey(x => (x.lable, x.feature)).reduceGroups((x, y) => getMinHealthScore(x,y)).map(_._2).show()
  // the binary function used in the reduceGroups
  def getMinHealthScore(x : AggregateResultModel, y : AggregateResultModel): AggregateResultModel = {
    AggregateResultModel("1",x.feature, x.value + y.value )
  }

//  row2ColDf.as[AggregateResultModel].groupByKey(x => (x.lable, x.feature)).
  import org.apache.spark.sql.functions._
  val res = row2ColDf.groupBy("label", "feature").agg(
    callUDF("concat_ws", lit(","), callUDF("collect_list", $"value")).as("tValue")
  )

  //df.select(struct("id", "name").as("a-b")).show()
  import spark.implicits._
  val res1= res.select($"feature",struct("label", "tValue").as("label_value"))
  res1.printSchema()
  /**
    * /**
    * * root
    * * |-- feature: string (nullable = true)
    * * |-- label_value: struct (nullable = false)
    * * |    |-- label: string (nullable = true)
    * * |    |-- tValue: string (nullable = false)
    **/
    */
  /**
    * +-------+--------------------+
    * |feature|        label-tValue|
    * +-------+--------------------+
    * |    m12|[1,78.0,12.0,42.0...|
    * |     m6|[1,60.0,12.0,39.0...|
    * |   day7|[1,9.0,-1.0,3.0,3...|
    * |     m3|[1,44.0,12.0,30.0...|
    * |    m60|[0,54.0,80.0,68.0...|
    * |    m12|[0,42.0,67.0,66.0...|
    * ------------------------------
    */
  val res2 = res1.groupBy("feature").agg(
    callUDF("concat_ws", lit(";"), callUDF("collect_list", struct($"label_value").cast(StringType))).as("ks")
  )
  // [[1,2,3]];[[0,3,5,6]]

  res2.printSchema()
  /**
    * root
    * |-- feature: string (nullable = true)
    * |-- AValue: string (nullable = false)
    */
  def filterSpecialChar(str:String): String = {
    val pattern = "[\\[\\])]".r
    pattern replaceAllIn(str, "")
  }

  res2.withColumn("ks", udf{str: String => {
    //[[1,2,3]];[[0,3,5,6]]
   val res = str.split(";")
    val r1 = filterSpecialChar(res(0)).split(",").toBuffer
    val r2 = filterSpecialChar(res(1)).split(",").toBuffer
    var p = Array[Double]()
    var n = Array[Double]()
    if(r1.head.toInt == 0){
      p = r1.tail.toArray.map(_.toDouble)
      n = r2.tail.toArray.map(_.toDouble)
    }else{
      p = r2.tail.toArray.map(_.toDouble)
      n = r1.tail.toArray.map(_.toDouble)
    }
    ks2Samp(p, n)
  }}.apply(col("ks"))).show()

  res2.show()


  /**
    * @return ArrayBuffer
    */
  def test_2samp(positive: DataFrame, negative: DataFrame, cols: Array[String]): Option[DataFrame] = {
    import org.apache.spark.sql.functions._
    import positive.sparkSession.implicits._

    val columns = positive.columns
    def getDfData: (DataFrame,String) => Array[Double] =
      (df, c) =>
        df.select(col(c).cast(DoubleType)).filter(col(c).isNotNull).map(x => x.getDouble(0)).collect()

    var ksV: Double = 0
    var resArray = new ArrayBuffer[Ks]()
    for (staCol <- cols if cols.nonEmpty) {
      val c = staCol.trim
      if(columns.contains(c)) {
        println(s"ks-index column:$c")
        val p = getDfData(positive, c)
        val n = getDfData(negative, c)

        if (p.length == 0) {
          ksV = 0
        } else if (n.length == 0) {
          ksV = 1
        } else {
          ksV = ks2Samp(getDfData(positive, c), getDfData(negative, c))
        }
        resArray +:= Ks(index_name = c, KS = ksV)
      }else{
        println(s"column not contains df:$c")
      }
      }
    val res = positive.sparkSession.createDataFrame(resArray)
    println("ks index dataframe show:")
    if (resArray.nonEmpty) Some(res) else None
  }



  /**
    * Computes the Kolmogorov-Smirnov statistic on 2 samples.
    * This is a two-sided test for the null hypothesis that 2 independent samples
    * are drawn from the same continuous distribution.
    * sequence of 1-D ndarrays,two arrays of sample observations assumed to be drawn from a continuous,
    * distribution, sample sizes can be different
    *
    * @param data1
    * @param data2
    * @return KS statistic
    */
  def ks2Samp(data1: Array[Double], data2: Array[Double]): Double = {
    val s1 = data1.sorted
    val s2 = data2.sorted
    val n1 = s1.length
    val n2 = s2.length

    val dataAll = s1 ++ s2

    val cdf1 = searchSorted(s1, dataAll).map(x => x / (1.0 * n1))
    val cdf2 = searchSorted(s2, dataAll).map(x => x / (1.0 * n2))

    val statistic = cdf1.zip(cdf2).map(x => x._2 - x._1).map(x => math.abs(x)).max
    statistic
  }

  private def searchSorted(data1: Array[Double], data2: Array[Double], side: String = "right"): ArrayBuffer[Int] = {
    require(data1.length > 0 && data2.length > 0, "Array size must be bigger than zero")
    val len1 = data1.length
    val len2 = data2.length

    var resIndexArray: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val r1 = (for (i <- data1.indices) yield i.toInt).toArray

    def searchIndex(v2: Double, array: Array[Double]): Int = {
      var temp = 0
      for (i <- array.indices) if (v2 >= array(i)) temp += 1 else return temp
      temp
    }

    for (j <- 0 until len2) {
      val v2 = data2(j)
      resIndexArray += searchIndex(v2, data1)
    }
    resIndexArray
  }

}


