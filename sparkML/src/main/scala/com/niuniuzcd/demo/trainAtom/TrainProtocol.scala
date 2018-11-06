package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.JSON
case class P(st: String, ds: String, out: String)
private[trainAtom] trait TrainProtocol[T]{
  def getDs: T
  def getSt : T
  def getOut: T

  def parseTrainJson(jsonStr: String): (String, String, String) = {
    val obj = JSON.parseObject(jsonStr, classOf[P])
    (obj.st, obj.ds, obj.out)
  }
}
