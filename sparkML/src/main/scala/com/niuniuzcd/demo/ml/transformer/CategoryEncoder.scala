package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer

/**
  * 字符串变量将字符转换成它对应的频数 是否用ml.StringIndexer
  * count encoding: Replace categorical variables with count in the train set.
  * replace unseen variables with 1.
  * Can use log-transform to be avoid to sensitive to outliers.
  */

class CategoryEncoder(var df: DataFrame, val unseen_value: Int = 1, val log_transform: Boolean = true, val smoothing: Int = 1, val inplace: Boolean = true) {
  var columns: Array[String] = _
  var dfTemp : DataFrame = _
  var dfTempList: ListBuffer[DataFrame] = _
  var strIndexerModel: StringIndexerModel = _

  /**
    * ml.StringIndexer
    */
  def fit(colName: String):this.type = {
    strIndexerModel = new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "_inx")
      .fit(df)
    this
  }

  def transform(df: DataFrame) :DataFrame = {
    strIndexerModel.transform(df)
  }
}

