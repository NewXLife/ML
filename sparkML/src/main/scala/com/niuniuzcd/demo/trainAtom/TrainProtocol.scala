package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.DataFrame
case class P(st: String, ds: String, out: String)
private[trainAtom] trait TrainProtocol[T] extends Serializable{
  def getDs: T
  def getSt : T
  def getOut: T

//  val fit: DataFrame => DataFrame = (df: DataFrame) => df
  def fit(f: DataFrame => DataFrame): this.type= {
    this
  }

  def parseTrainJson(jsonStr: String): (String, String, String) = {
    val obj = JSON.parseObject(jsonStr, classOf[P])
    (obj.st, obj.ds, obj.out)
  }
}
