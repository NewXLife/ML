import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * create by colin on 2018/7/12
  */
object FeatureCutPcutDTLikePython extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()
  //          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.createDataFrame(Seq((0, "1", "2", 3, 0.3), (1, "", "2", 13, 0.2), (2, "1", "2", 13, 0.2), (3, "1", "2", 23, 0.2), (4, "1", "2", 31, 0.5), (5, "1", "2", 35, 0.7), (0, "1", "2", 36, 0.5), (0, "1", "2", 39, 0.9)))
    .toDF("id", "age", "name", "score", "mu")

  //  val data = Array((0, "1"), (1,"3"), (2, "2"), (3, "4"),(4, "5"))
  //  val df1 = spark.createDataFrame(data).toDF("id","features")


  //  The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`
  //  val t = cut(df, "id")
  //
  //  for (i <- t) println(i)

  qcut(df, 5)

  // -0.001
  //1.0
  //2.0
  //3.0
  //4.0
  //5.0
  def cut(df: DataFrame, col: String) = {
    val cut_num = 5
    val right = true

    var mn = min(df, col).asInstanceOf[Double]
    var mx = max(df, col).asInstanceOf[Double]

    if (mn == mx) {
      mn = mn - (if (mn != 0).001 * math.abs(mn) else .001)
      mx = mx + (if (mx != 0).001 * math.abs(mx) else .001)
      //      bins = linspace(mn, mx, cut_num + 1)
      linspace(mn, mx, cut_num + 1).toBuffer
    } else {
      val bins = linspace(mn, mx, cut_num + 1).toBuffer
      val adj = (mx - mn) * 0.001
      if (right) bins(0) -= adj else bins(bins.length - 1) += adj
      bins
    }
  }

  def linspace2(start: Double, stop: Double, num: Int = 10, endpoint: Boolean = true) = {
    require(num > 0, "Number of samples, %s, must be non-negative.")

    val div = if (endpoint) num - 1 else num

    val delta = stop * 1.0 - start * 1.0
    val d = delta / div

    val ab = ArrayBuffer[Double]()
    var cut_point = start
    for (_ <- 0 until div) {
      ab += cut_point
      cut_point = cut_point + d
    }
    ab
  }

  def linspace(start: Double, stop: Double, num: Int = 10, endpoint: Boolean = true) = {
    require(num > 0, "Number of samples, %s, must be non-negative.")

    val div = if (endpoint) num - 1 else num

    val delta = stop * 1.0 - start * 1.0

    val d = delta / div

    val y = Range(0, div) // 0 until div

    if (num > 1) {
      val step = delta / (div - 1)
      if (step == 0) {
        val t = for (i <- y) yield i / div
        for (i <- t) yield i * delta
      } else {
        for (i <- y) yield i * step
      }
    } else {
      val step = None
      for (i <- y) yield i * delta
    }
  }


  def qcut(df: DataFrame, q: Int = 10) = {
    val quantiles = linspace2(0, 1, 5 + 1)
    quantiles.foreach(println(_))
  }


  def min(df: DataFrame, col: String) = {
    df.agg(s"$col" -> "min").first().get(0).asInstanceOf[Int]
  }


  def max(df: DataFrame, col: String) = {
    df.agg(s"$col" -> "max").first().get(0).asInstanceOf[Int]
  }

}

