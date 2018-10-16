package util

import org.apache.spark.sql.SparkSession

/**
  * create by colin on 2018/8/23
  */
trait SparkTools extends App{
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()


  def loadCSVData(csv:String,filePath:String, hasHeader:Boolean=true) ={
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }
}
