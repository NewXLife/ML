package com.niuniuzcd.demo.util

import org.apache.spark.sql.SparkSession

object SparkS {
  @transient private var instance: SparkSession = _

  def getInstance(): SparkSession = {
    if (instance == null) instance = SparkSession.builder().getOrCreate()
    instance
  }
}
