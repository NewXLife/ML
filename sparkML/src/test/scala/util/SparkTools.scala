package util

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
  * create by colin on 2018/8/23
  */
trait SparkTools extends App{
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import org.apache.spark.sql.functions._
  import spark.implicits._

  val tempDf = loadCSVData("csv", "C:\\NewX\\newX\\MachineLearning\\docs\\testData\\base.csv")
  val tempLaoke = loadCSVData("csv", "C:\\NewX\\newX\\MachineLearning\\docs\\testData\\laoke_online.csv")

  val columns = tempDf.columns.toBuffer

  //exclude date field
  columns.remove(columns.indexOf("ad"))

  val baseDf = tempDf.select($"ad" +: columns.toArray.map(f => col(f).cast(DoubleType)):_*)

  val df = spark.createDataFrame(Seq(
    (0, Vectors.dense(1.0, 0.5, -1.0)),
    (1, Vectors.dense(2.0, 1.0, 1.0)),
    (2, Vectors.dense(4.0, 10.0, 2.0))
  )).toDF("id", "features")

  def loadCSVData(csv:String,filePath:String, hasHeader:Boolean=true) ={
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }
}
