package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.sql.DataFrame

/**
  * 用于剔除缺失值严重列，同值严重列，不同值严重cate列（字符串列如果取值太过于分散，则信息量过低）。
  *
  * 适用于cont和cate，支持缺失值, 建议放置在encoder序列第一位次
  *
  * Parameters
  * ----------
  * missing_thr: 0.8, 缺失率高于该值的列会被剔除
  *
  * same_thr: 0.8, 同值率高于该值的列会被剔除
  *
  * cate_thr: 0.9， 取值分散率高于该值的字符串列会被剔除
  *
  * Attributes
  * ----------
  * missing_cols: list, 被剔除的缺失值列
  *
  * same_cols: list, 被剔除的同值列
  *
  * cate_cols: list, 被剔除的取值分散字符串列
  *
  * exclude_cols: list, 被剔除的列名
  */
class BaseEncoder(val missing_thr: Double = 0.8, val same_thr: Double = 0.8, val cate_thr: Double = 0.9) {
  var exclude_cols: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]()

  def fit(df: DataFrame): this.type = {
    exclude_cols ++= getCateCols(df)
    exclude_cols ++= getSameValueRatio(df)
    exclude_cols ++= getMissingValueRatio(df)
    this
  }

  /**
    * 被剔除的取值分散字符串列 default = 0.9
    * @return
    */
  def getCateCols(df: DataFrame): Array[String] ={
    val total = df.count().asInstanceOf[Double]
    val cateFeatures = df.dtypes.filter { case (_, t) => t == "StringType" }.map { case (f, _) => f }

    if (cateFeatures.length > 0)
        cateFeatures.map(fName => (fName,df.select(fName).distinct().count()))
          .filter{case(_, p) => (p.asInstanceOf[Long] / total) > cate_thr}
          .map{case(f,_) => f}
    else
      Array[String]()
  }


  /**
    * 被剔除的缺失值列 default=0.8
    * @param df
    * @return
    */
  def getMissingValueRatio(df: DataFrame): Array[String] = {
    val tmp_count = df.schema.fields.map(f => (f.name, df.where(s"${f.name} is null").count()))
    for (i <- tmp_count if i._2 > this.missing_thr) yield i._1
  }


  /**
    * 被剔除的同值列 default = 0.8
    * @param df
    * @return
    */
  def getSameValueRatio(df: DataFrame): Array[String] = {
    val total = df.count().asInstanceOf[Double]
    df.schema.fieldNames.map(fName =>
      (fName, df.groupBy(s"$fName").count()))
      .filter { case (_, cdf) => cdf.sort(cdf("count").desc).first().get(1).asInstanceOf[Long] / total > this.same_thr }
      .map { case (f, _) => f }

  }

  def transform(df: DataFrame): DataFrame = {
    df.drop(exclude_cols.toArray: _*)
  }

  /**
    * Returns a new `DataFrame` that drops rows containing any null or NaN values
    * @param df
    * @param fillValue
    * @return
    */
  def fillNa(df:DataFrame, fillValue:Any):DataFrame={
    fillValue match {
      case x:Int => df.na.fill(x.asInstanceOf[Int])
      case x:Double => df.na.fill(x.asInstanceOf[Double])
      case x:String => df.na.fill(x.asInstanceOf[String])
      case x:Long => df.na.fill(x.asInstanceOf[Long])
      case _ => df.na.fill(-999)
    }
  }

}


