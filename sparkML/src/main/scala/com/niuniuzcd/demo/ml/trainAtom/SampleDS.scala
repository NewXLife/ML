package com.niuniuzcd.demo.ml.trainAtom

import com.alibaba.fastjson.{JSON, JSONObject}

 class SampleDS(str: String) extends TrainProtocol[String]{
  val (st, ds, out) = parseTrainJson(str)
  val dsJsonStr: JSONObject = JSON.parseObject(ds)

   def getLabel: String = {
    val labelItem = JSON.parseObject(dsJsonStr.getString("label"))
    labelItem.getString("name")
  }

  def getDsExtend: (String, String) = {
    (dsJsonStr.getString("fileType"), dsJsonStr.getString("target"))
  }

   def getDs: String = ""


  def getStrategyStr:(String, String) ={
    val dsJsonStr = JSON.parseObject(st)
    val para = JSON.parseObject(dsJsonStr.getString("params"))
    (para.getString("include"), para.getString("condition"))
  }

  def getSt: String = {
    val dsJsonStr = JSON.parseObject(st)
    val para = JSON.parseObject(dsJsonStr.getString("params"))
    val sqlStr = StringBuilder.newBuilder
    sqlStr ++= para.getString("include")
    sqlStr ++= ";"
    sqlStr ++= para.getString("condition")
    sqlStr.toString()
  }

  def getOut: String = ""
}
