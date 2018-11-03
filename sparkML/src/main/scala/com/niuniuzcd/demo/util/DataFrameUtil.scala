package com.niuniuzcd.demo.util

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
/**
  * create by colin on 2018/7/30
  */
object DataFrameUtil {
  /**
    * parsed DataFrame has no primary key ,add index and use it  join function
    *
    * @param df
    * @param f
    * @return
    */
  def addIndexDf(df: DataFrame, f: String): DataFrame = {
    val mRdd = df.na.drop().rdd
    val newRdd = mRdd.mapPartitions(par => par.map(x => x.toString.substring(1, x.toString.length - 1))).zipWithIndex()

    //value-index
    val rowRdd = newRdd.mapPartitions(par => par.map(a => Row(a._1.toDouble, a._2.toInt)))

    val sf = StructField(f, DoubleType, nullable = true)
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
