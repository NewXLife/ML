package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer

/**
  * 字符串变量将字符转换成它对应的频数 是否用ml.StringIndexer
  * count encoding: Replace categorical variables with count in the train set.
  * replace unseen variables with 1.
  * Can use log-transform to be avoid to sensitive to outliers.
  */

object CategoryEncoder {
  def apply(unseen_value: Int = 1, log_transform: Boolean = true, smoothing: Int = 1, inplace: Boolean = true): CategoryEncoder =
    new CategoryEncoder(unseen_value, log_transform, smoothing, inplace)
}

class CategoryEncoder(val unseen_value: Int, val log_transform: Boolean, val smoothing: Int, val inplace: Boolean) {
  var columns: Array[String] = _
  var dfTemp : DataFrame = _
  var dfTempList: ListBuffer[DataFrame] = _

  /**
    * group by each field use count(), and combine each DataFrame
    * if log_transform is true ,addIndexDf() will invoke math.log() to every number
    * @param df
    */
  def fit(df: DataFrame): Unit = {
    columns = df.schema.fieldNames
    val r1 = df.schema.fieldNames.map(f => (f, df.groupBy(s"$f").count())).map { case (f, tmp_df) => (f, tmp_df.select(tmp_df(s"$f"), (tmp_df("count") + unseen_value).as(s"${f}_count"))) }
    val r2 = for (i <- r1) yield df.select(df(s"${i._1}")).join(i._2, df(s"${i._1}") === i._2(s"${i._1}"), "left").select(s"${i._1}_count")

    for ( i <- r2.indices) dfTempList += addIndexDf(r2(i),r2(i).schema.fieldNames(0))

    for (i <- dfTempList.indices){
      if (dfTemp != null){
        dfTemp = dfTemp.join(dfTempList(i), dfTempList(i)("index") === dfTemp("index")).drop(dfTempList(i)("index"))
      }else{
        dfTemp = dfTempList(i)
      }
    }
  }


  def transform(df: DataFrame) :DataFrame = {
    dfTemp.drop(dfTemp("index"))
  }


  /**
    * parsed DataFrame has no primary key ,add index and use it  join function
    * @param df
    * @param f
    * @return
    */
  def addIndexDf(df:DataFrame, f:String):DataFrame = {
    val mRdd = df.na.fill(1).rdd
    val newRdd = mRdd.mapPartitions(par => par.map(x => x.toString.substring(1, x.toString.length -1))).zipWithIndex()
    //transfer rdd use math.log
    var rowRdd = newRdd.mapPartitions(par => par.map(a => Row(math.log(a._1.toInt), a._2.toInt)))
    if (!log_transform) rowRdd = newRdd.mapPartitions(par => par.map(a => Row(a._1.toInt, a._2.toInt)))
    var sf = StructField(f, DoubleType, nullable = true)
    if (!log_transform) sf = StructField(f, IntegerType, nullable = true)
    val schema = StructType(
      Array(
        sf,
        StructField("index", IntegerType, nullable = true)
      )
    )
    val resDf = df.sparkSession.createDataFrame(rowRdd, schema)
    resDf
  }


}

