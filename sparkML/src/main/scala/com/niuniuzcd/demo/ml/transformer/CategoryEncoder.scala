package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 字符串变量将字符转换成它对应的频数 是否用ml.StringIndexer
  * count encoding: Replace categorical variables with count in the train set.
  * replace unseen variables with 1.
  * Can use log-transform to be avoid to sensitive to outliers.
  */

class CategoryEncoder(val unseen_value: Int = 1, val log_transform: Boolean = true, val smoothing: Int = 1, val inplace: Boolean = true) {
  var columns: Array[String] = _
  var dfTemp : DataFrame = _
  var resTemp : DataFrame = _
  var dfTempList: ListBuffer[DataFrame] = _
  var strIndexerModel: StringIndexerModel = _
  private var newCol = ""
  private var inputCol = ""
  var colsArray: ArrayBuffer[String] = ArrayBuffer()
  private var outputCol = ""

  def setInputCol(colName: String):this.type = {
    inputCol = colName
    newCol = "new_"+ inputCol
    this
  }
  def setOutputCol(colName: String):this.type  = {
    outputCol = colName
    this
  }

  /**
    * ml.StringIndexer
    * input Seq('a','b','a','c')
    * result code as follows:
    * +---+--------+
    * |n  |ad_index|
    * +---+--------+
    * |a  |0.0     |
    * |b  |1.0     |
    * |a  |0.0     |
    * |c  |2.0     |
    * +---+--------+
    */
  def fit(df: DataFrame):this.type = {
    strIndexerModel = new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .fit(df)
    this
  }

  val pfunc: DataFrame => DataFrame = (df: DataFrame) => {
    println("before category encoder dataframe show:")
    df.show(5)
    if (colsArray.nonEmpty ){
      resTemp = df
      for(col <- colsArray){
        inputCol = col
        outputCol = col
        resTemp =pfit(resTemp).ptransform(resTemp)
      }
      println("after category encoder dataframe show:")
      resTemp.show(5)
      resTemp
    }else{
      df
    }

  }

  val func: DataFrame => DataFrame = (df: DataFrame) => {
    fit(df).transform(df)
  }

  def transform(df: DataFrame) :DataFrame = {
    strIndexerModel.transform(df)
  }

  import org.apache.spark.sql.functions.udf
  val mathLog: UserDefinedFunction = udf{ f: Double => math.log(f)}
  /**
    * the same with python p-> python alias
    * step1: group by  sta-colName
    * step2: all of smooth 1
    * step3: use math.log smooth
    * @param df
    */
  def pfit(df:DataFrame):this.type = {
    dfTemp = df.groupBy(inputCol).count().toDF(newCol, "count")
    this
  }

  def ptransform(df: DataFrame): DataFrame = {
    val t2 = if(inplace) df.join(dfTemp, dfTemp(newCol).equalTo(df(inputCol)), "left").drop(inputCol, newCol) else df.join(dfTemp, dfTemp(newCol).equalTo(df(inputCol)), "left")
    val t3 = if(smoothing > 0) t2.withColumn("count_1", t2("count")+ smoothing).drop("count").withColumnRenamed("count_1", "count") else t2
    if(log_transform) t3.withColumn("log_count", mathLog(t3("count"))).drop("count").withColumnRenamed("log_count", outputCol)
    else t3.withColumnRenamed("count", outputCol)
  }
}

