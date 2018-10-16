package com.niuniuzcd.demo.util

/**
  * create by colin on 2018/7/13
  */
object FlowEnum extends Enumeration {

  type FlowEnum = Value

  val  FEA_DS ,FEA_SAMPLING,FEA_SAMPLE_SPLIT, FEA_TRANSFORM, TRAIN_FILTER, TRAIN_TRAINER = Value

  def isContain(flow : FlowEnum):Boolean = {
    Seq(FEA_DS ,FEA_SAMPLING,FEA_SAMPLE_SPLIT, FEA_TRANSFORM, TRAIN_FILTER, TRAIN_TRAINER).contains(flow)
  }

  def checkExists(flowName:String):Boolean = this.values.exists(_.toString == flowName)

  def showAll:Unit = this.values.foreach(println)
}
