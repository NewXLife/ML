package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.DataFrame

private[trainAtom] sealed class Spliter(str: String) extends TrainProtocol[String]{
  val (st, ds, out) = parseTrainJson(str)
  val jsonSt: JSONObject = JSON.parseObject(st)

  def getDs: String = {
    val json = JSON.parseObject(ds)
    ""
  }

  def trainTestSplit(df: DataFrame):(DataFrame, DataFrame) = {
    import df.sparkSession.implicits._
    val testSize = jsonSt.getDouble("test_size")
    val time_col = jsonSt.getString("time_col")
    val counts = df.count()
    val ootNumber = (counts * testSize).toInt
    val trainNumber = (counts * (1- testSize)).toInt

    jsonSt.getString("method") match {
      case "oot" =>
        val test = df.sort(-$"$time_col".desc).limit(ootNumber)
        val train =  df.sort($"$time_col").limit(trainNumber)
        (train, test)
      case "random" =>
        val res = df.randomSplit(Array(1 - testSize, testSize))
        val test = res(0)
        val train = res(1)
        (train, test)
    }
  }

  def getSt: String = {
    val json = JSON.parseObject(st)
    val method = json.getString("method")
    val test_size = json.getString("test_size")
    val random_state = json.getString("random_state")
    val time_col = json.getString("time_col")
    val index_col = json.getString("index_col")
    val label_col = json.getString("label_col")
    test_size
  }

  def getOut: String = {
    val json = JSON.parseObject(out)
    val dst = json.getString("dst")
    dst
  }
}
