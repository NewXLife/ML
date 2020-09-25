package com.niuniuzcd.demo.test

import org.apache.spark.sql.SparkSession

/**
  * 1-在终端启动：nc -lk 9999
  * 2-启动TestStructStreamingSocket程序
  * 3-在终端输入字符串进行统计，打印输出在console
  */
object TestStructStreamingSocket extends App {
  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words

  lines.createTempView("socket")

  val output=spark.sql("select count(*) from  socket")


//  val words = lines.as[String].flatMap(_.split(" "))
//
//  // Generate running word count
//  val wordCounts = words.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = output.writeStream
    .outputMode("complete") //Append,Update
    .format("console")//.trigger(Trigger.ProcessingTime(300))
    //.option("checkpointLocation", "/xx")设置checkpoint
    .start()

  query.awaitTermination()

}
