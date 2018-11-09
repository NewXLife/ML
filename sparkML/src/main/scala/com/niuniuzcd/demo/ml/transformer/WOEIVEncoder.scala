package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.sql.DataFrame

/**
  * woe（weight of evidence)变换
  *
  * 适用于cont和cate，但对多取值cont无效，支持缺失值
  *
  * Parameters
  * ----------
  * diff_thr : int, default: 20
  * 不同取值数小于等于该值的才进行woe变换，不然原样返回
  *
  * woe_min : int, default: -20
  * woe的截断最小值
  *
  * woe_max : int, default: 20
  * woe的截断最大值
  *
  * nan_thr : float, default: 0.01
  * 对缺失值采用平滑方法计算woe值，nan_thr为平滑参数
  */
class WOEIVEncoder(diff_thr: Int, woe_min: Int, woe_max: Int, nan_thr: Double, inplace: Boolean) {
  var dmap: scala.collection.mutable.Map[Any, Any] = _

  /**
    * OVERDUE_THREAD >0  overdue
    *
    * @return
    */
  val OVERDUE_THREAD = 0

  /**
    * 样本的好坏是分场景的，对于当前场景，未逾期就认为是好的样本， 逾期就是坏样本
    */
  final val GOOD_SAMPLE = "good"
  final val BAD_SAMPLE = "bad"

  final val DEFAULT_LABEL_NAME = "label"

  /**
    * 违约门限  >14 days为违约
    */
  final val DEFAULT_THREAD = 14

  def totalPercent(df: DataFrame, col: String, labelName: String = DEFAULT_LABEL_NAME): Double = {
    import df.sparkSession.implicits._
    val totalGood = df.select(col).where($"$labelName" <= OVERDUE_THREAD).count().asInstanceOf[Int]
    val totalBad = df.select(col).where($"$labelName" > OVERDUE_THREAD).count().asInstanceOf[Int]
    //Double.NegativeInfinity
    if (totalBad == 0) Double.MaxValue else totalGood.toDouble / totalBad.toDouble
  }


  /**
    * return Array(((0-1),woe,IV))
    *
    * @param df
    * @param col
    * @param bins
    * @param labelName
    * @return
    */
  def woeIv(df: DataFrame, col: String, bins: Array[(Double, Double)], labelName: String = DEFAULT_LABEL_NAME): Array[((Double, Double), Double, Double)] = {
    import df.sparkSession.implicits._
    val totalP = totalPercent(df, col, labelName)

    //val newDf = df.selectExpr(s"CAST($col as Double)")
    def innerWoe(t: (Double, Double), flag: String): Int = {
      flag match {
        case GOOD_SAMPLE => df.selectExpr(s"CAST($col as Double)").filter(df(s"$col") > t._1 && df(s"$col") <= t._2).where($"$labelName" <= OVERDUE_THREAD).count().asInstanceOf[Int]
        case BAD_SAMPLE => df.selectExpr(s"CAST($col as Double)").filter(df(s"$col") > t._1 && df(s"$col") <= t._2).where($"$labelName" > OVERDUE_THREAD).count().asInstanceOf[Int]
      }
    }

    //double i = Double.POSITIVE_INFINITY
    def woePer(goodSample: Double, badSample: Double) = {
      if (badSample == 0) Double.MaxValue else goodSample / badSample
    }

    // bins，woePercent
    val bins_per = for (i <- bins) yield (i, woePer(innerWoe(i, GOOD_SAMPLE).toDouble, innerWoe(i, BAD_SAMPLE).toDouble))

    //(min,max], binsPercent, bins-woe
    val woeV = bins_per.map(x => (x._1, x._2, math.log(x._2 / totalP)))

    //iv=(binsPercent - p_total)*bins_woe  return (bins, woe, iv)
    val bins_woe_iv = woeV.map(x => (x._1, x._3, (x._2 - totalP) * x._3))
    val IV  =   woeV.map(x =>(x._2 - totalP) * x._3)
    bins_woe_iv
  }


  @deprecated
  def woe(x: DataFrame, y: DataFrame, woe_min: Int = 0, woe_max: Int) = {
    import x.sparkSession.implicits._
    val pos = y.where($"label" === 1).count()
    val neg = y.where($"label" === 0).count()

    val c = x.schema.fieldNames(0)

    for (k <- x.distinct()) {
      val pos_r = x.select(x(s"$c")).where(x(s"$c") === k && $"label" === 1).agg(Map("label" -> "sum")).first().get(0)
      val neg_r = x.select(x(s"$c")).where(x(s"$c") === k && $"label" === 0).agg(Map("label" -> "sum")).first().get(0)

      var woe1: Double = 0
      if (pos_r equals 0) {
        woe1 = woe_min
      } else if (neg_r equals 0) {
        woe1 = woe_max
      } else {
        woe1 = math.log(pos_r.asInstanceOf[Long] / neg_r.asInstanceOf[Long])
      }
      dmap += (k -> woe1)
    }
  }

  @deprecated
  def fit(df: DataFrame, y: Any): Unit = {
    val columns = df.schema.fields
    val woecols = df.schema.fieldNames.map(f => (f, df.select(s"$f").distinct().count())).filter { case (_, num) => num.asInstanceOf[Long] >= 2 }.map { case (f, _) => f }
    val label = "label"

    for (c <- woecols) {
      var tmp = df.withColumn("label", df(s"$c"))
      tmp = tmp.na.drop()
      val dmap = woe(tmp.select(tmp(c)), tmp.select(tmp("label")), woe_min, woe_max)
    }
  }
}

object WOEIVEncoder {
  def apply(diff_thr: Int = 20, woe_min: Int = -20, woe_max: Int = 20, nan_thr: Double = 0.01, inplace: Boolean = true): WOEIVEncoder = new WOEIVEncoder(diff_thr, woe_min, woe_max, nan_thr, inplace)
}
