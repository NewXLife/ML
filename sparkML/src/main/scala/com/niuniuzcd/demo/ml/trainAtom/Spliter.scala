package com.niuniuzcd.demo.ml.trainAtom

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.DataFrame

private[trainAtom] sealed class Spliter(str: String) extends TrainProtocol[String] {
  val (st, ds, out) = parseTrainJson(str)
  val jsonSt: JSONObject = JSON.parseObject(st)

  var test: DataFrame = _

  var label_col = ""
  var testSize:Double = _
  var timeCol = ""
  var indexCol = ""

  def getDs: String = {
    val json = JSON.parseObject(ds)
    ""
  }

  val spliter: DataFrame => DataFrame = (df: DataFrame) => {
    getSt
    import df.sparkSession.implicits._
    val counts = df.count()
    val ootNumber = (counts * testSize).toInt
    val trainNumber = (counts * (1 - testSize)).toInt

    jsonSt.getString("method") match {
      case "oot" =>
        require(!timeCol.isEmpty,"input time column must be not empty")
        test = df.sort(-$"$timeCol".desc).limit(ootNumber).toDF()
        println("oot test show:")
        test.show(5)
        val train = df.sort($"$timeCol").limit(trainNumber)
        println("oot train show:")
        train.show(5)

        train.toDF()
      case "random" =>
        val res = df.randomSplit(Array(1 - testSize, testSize))
        test = res(0).toDF()

        println("random test show:")
        test.show(5)
        val train = res(1)

        println("random train show:")
        train.show(5)
        train.toDF()
      case _ => println("mismatch, do nothing return input dataframe"); df
    }
  }

  def getSt: String = {
    val json = JSON.parseObject(st)
    val method = json.getString("method")
    testSize = json.getDouble("test_size")
    val random_state = json.getString("random_state")
    timeCol = json.getString("time_col")
    indexCol = json.getString("index_col")
    label_col = json.getString("label_col")
    ""
  }

  def getOut: String = {
    val json = JSON.parseObject(out)
    val dst = json.getString("dst")
    dst
  }
}
