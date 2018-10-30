package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}

/**
  * 将连续型变量转化为离散型
  *
  * 仅适用于cont， 支持缺失值
  *
  * Parameters
  * ----------
  * diff_thr : int, default: 20
  * 不同取值数高于该值才进行离散化处理，不然原样返回
  *
  * binning_method : str, default: 'dt', {'dt', 'qcut', 'cut'}
  * 分箱方法,:
  * 'dt' which uses decision tree.
  * 'cut' which cuts data by the equal intervals.
  * 'qcut' which cuts data by the equal quantity.
  * default is 'dt'. if y is None, default auto changes to 'qcut'.
  *
  * bins : int, default: 10
  * 分箱数目， 当binning_method='dt'时，该参数失效
  *
  * 决策树分箱方法使用的决策树参数
  */
class ContinueEncoder {
  private val DEFAULT_BINS = 10

  def bins2cuts(df: DataFrame, bins: mutable.Buffer[Double], right:Boolean = true): Unit = {
    val unique_bins = bins.toArray.distinct
    require(unique_bins.length == bins.length && bins.length !=2, "Bin edges must be unique.")

    val side =  if (right) "left" else "right"

  }

  def getCatalog(df: DataFrame, col: String, binsNum: Int = DEFAULT_BINS): immutable.IndexedSeq[(String, String)] = {
    val bins = cut(df, col, binsNum)
    for( i <- 0 until bins.length -1) yield (bins(i).formatted("%.3f"), bins(i+1).formatted("%.3f"))
  }

  def getCatalogDouble(df: DataFrame, col: String, binsNum: Int = DEFAULT_BINS): Array[(Double, Double)] = {
    val bins = cut(df, col, binsNum)
    val res = for( i <- 0 until bins.length -1) yield (bins(i), bins(i+1))
    res.toArray
  }

  def getBinsArray(bins:Array[Double]):Array[(Double, Double)] = {
    val res = for (i <- 0 until bins.length - 1) yield (bins(i), bins(i + 1))
    res.toArray
  }

  def cut(df: DataFrame, col: String, binsNum: Int = DEFAULT_BINS, right: Boolean = true): mutable.Buffer[Double] = {
    var mn = min(df, col)
    var mx = max(df, col)

    if (mn == mx) {
      mn = mn - (if (mn != 0).001 * math.abs(mn) else .001)
      mx = mx + (if (mx != 0).001 * math.abs(mx) else .001)
      linspace(mn, mx, binsNum + 1)
    } else {
      val t_bins = linspace(mn, mx, binsNum + 1)
      val adj = (mx - mn) * 0.001
      if (right) t_bins(0) -= adj else t_bins(t_bins.length - 1) += adj
      t_bins
    }
  }



  /**
    * the other way
    * @param start
    * @param stop
    * @param num
    * @param endpoint
    * @return
    */
  def linspace2(start: Double, stop: Double, num: Int = DEFAULT_BINS, endpoint: Boolean = true): ArrayBuffer[Double] = {
    require(num > 0, "Number of samples, %s, must be non-negative.")

    val div = if (endpoint) num - 1 else num

    val delta = stop - start
    val d = delta / div

    val ab = ArrayBuffer[Double]()
    var cut_point = start
    for (_ <- 0 until div) {
      ab += cut_point + start
      cut_point = cut_point + d
    }
    ab += stop
    ab
  }


  /**
    *
    * @param start
    * @param stop
    * @param num
    * @param endpoint
    * @return
    */
  def linspace(start: Double, stop: Double, num: Int = DEFAULT_BINS, endpoint: Boolean = true): mutable.Buffer[Double] = {
    require(num > 0, "Number of samples, %s, must be non-negative.")

    val div = if (endpoint) num - 1 else num

    val delta = stop.toDouble - start.toDouble

    var y = (0 to div).map(_*1.0).toBuffer // 0 until div

    if (num > 1) {
      val step = delta / div
      if (step == 0) {
        val t = for (i <- y) yield i / div
        y = for (i <- t) yield i * delta
      } else {
        y = for (i <- y) yield i * step
      }
    } else {
      val step = None
      y = for (i <- y) yield i * delta
    }

    y = y.map(x => x + start)

    if (endpoint && num > 1) {
      y(div) = stop.toDouble
    }
    y
  }

  private def min(df: DataFrame, col: String): Double = {
    df.selectExpr(s"CAST($col as Double)").agg(s"$col" -> "min").first().get(0).asInstanceOf[Double]
  }


  private def max(df: DataFrame, col: String): Double = {
    df.selectExpr(s"CAST($col as Double)").agg(s"$col" -> "max").first().get(0).asInstanceOf[Double]
  }

}

object ContinueEncoder {
  def apply: ContinueEncoder = new ContinueEncoder()
}
