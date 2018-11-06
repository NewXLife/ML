package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.{JSON, JSONObject}

private[trainAtom] class Filter(str: String) extends TrainProtocol[String] {
  val (st, ds, out) = parseTrainJson(str)
  val stJsonStr: JSONObject = JSON.parseObject(st)

  def getDs: String = ""

  def getSt: String = {
    val method = stJsonStr.getString("method")
    val params = stJsonStr.getString("params")
  }

  def getOut: String = ""
}
