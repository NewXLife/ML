package com.niuniuzcd.demo.ml.mlHandler

import com.alibaba.fastjson.{JSON, JSONObject}
import com.kuainiu.beidou.util.JsonUtil
import com.kuainiu.beidou.ml.trainAtom.TrainUnit._
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


object MLEngine {
  type func = DataFrame => DataFrame
  private var executorFlow: ArrayBuffer[func] = ArrayBuffer[func]()
  private var featureFlow: ArrayBuffer[func] = ArrayBuffer[func]()

  private var jsonP: JSONObject  = _
  private var protocolKey: scala.collection.Set[String]  = _
  private var trainSequenceList: List[String] = _
  private val cps = new Components

  private var pipeline: func = _

  private var model: XGBoostClassificationModel = _

  def start(df: DataFrame)(protocolStr: String): DataFrame = {
    setProtocolStr(protocolStr).fit(df)
    val testDF = getTestDF
    predict(testDF)
  }

  def getTestDF: DataFrame = {
    cps.get(TrainComponent.spliter).get.test //split test-data
  }

  def setProtocolStr(protocolStr: String):this.type ={
    jsonP = JSON.parseObject(protocolStr)
    protocolKey = JsonUtil.getKeySet(protocolStr)
    trainSequenceList = protocolKey.toList.drop(1)
    this
  }

  def fit(df: DataFrame) = {
    //List(FEA_SAMPLE_SPLIT, FEA_TRANSFORM, TRAIN_FILTER, TRAIN_TRAINER))
    for (key <- trainSequenceList) {
      println("input keys: ", key)
      //FEA_SAMPLE_SPLIT
      key match {
        case "FEA_SAMPLE_SPLIT" =>
          val so = spliterObj(jsonP.getString("FEA_SAMPLE_SPLIT"))
          cps.set(TrainComponent.spliter)(so)
          featureFlow += so.spliter
        case "FEA_TRANSFORM" =>
          val fto = featureTransformObj(jsonP.getString("FEA_TRANSFORM"))
          cps.set(TrainComponent.featureTransform)(fto)
          featureFlow += fto.transform
        case "TRAIN_FILTER" =>
          val to = filterObj(jsonP.getString("TRAIN_FILTER"))
          cps.set(TrainComponent.filter)(to)
          featureFlow += to.filter
        case "TRAIN_TRAINER" =>
          val to = trainerObj(jsonP.getString("TRAIN_TRAINER"))
          cps.set(TrainComponent.trainer)(to)
          executorFlow ++= featureFlow
          executorFlow += to.trainer
        case _ => println("mismatch")
      }
    }

    pipeline = Function.chain(executorFlow)
    pipeline(df)
    model = cps.get(TrainComponent.trainer).get.Model
  }

  //input test dataframe
  def predict(df: DataFrame): DataFrame = {
    Function.chain(featureFlow)(df)
    // like as train-data; need same encoder and transforms
    val transformsDf = cps.get(TrainComponent.filter).get.dataset
    model.transform(transformsDf)
  }
}
