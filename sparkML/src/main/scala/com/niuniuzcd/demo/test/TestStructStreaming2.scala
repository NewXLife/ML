package com.niuniuzcd.demo.test

import org.apache.spark.sql.SparkSession

/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object TestStructStreaming2 extends App {
  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()
//  spark.listenerManager.register()

//  Thread.currentThread().join()
}
