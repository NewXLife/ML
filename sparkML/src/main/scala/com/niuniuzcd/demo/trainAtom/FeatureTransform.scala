package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.JSON
import com.niuniuzcd.demo.ml.transformer.{BaseEncoder, CategoryEncoder, ContinueEncoder}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

case class Encoder(method: String, params:String)
case class EncoderWrap(cols:Array[Any], encoders: Array[Encoder])
case class ST(cate: Array[EncoderWrap], cont: Array[EncoderWrap], custom: Array[EncoderWrap], method: String, params: String, verbose: Boolean)

private[trainAtom] class FeatureTransform(str: String) extends TrainProtocol[String]{
  val (st, ds, out) = parseTrainJson(str)
  var cate: Array[EncoderWrap]= _
  var cont: Array[EncoderWrap]= _
  var custom: Array[EncoderWrap]= _
  var method: String = _
  var params: String = _

  var be: BaseEncoder = _

  var funcSeq: ArrayBuffer[DataFrame => DataFrame] = ArrayBuffer[DataFrame => DataFrame]()

  val transform: DataFrame => DataFrame = (df: DataFrame) => {
    getSt
    if (cate.length >0 ){
      for (encoderWrap <- cate){
        val cols = encoderWrap.cols
        for(encoder <- encoderWrap.encoders){
          val p = JSON.parseObject(encoder.params)
          encoder.method match {
            case "BaseEncoder" =>
              println("add baseEncoder")
              be = new BaseEncoder(cate_thr = p.getDouble("cate_thr"), missing_thr = p.getDouble("missing_thr"), same_thr =p.getDouble("same_thr"))
              funcSeq += be.func
            case "CountEncoder" =>
              println("add CountEncoder")
              val ce = new CategoryEncoder(log_transform = p.getBoolean("log_transform"), unseen_value= p.getInteger("unseen_value"), smoothing =p.getInteger("smoothing"))
             if (be.cateCols.length >0 ) ce.colsArray ++= be.cateCols else   ce.colsArray ++= be.stringFeatureArray
              ce.colsArray += "apply_date"
              funcSeq += ce.pfunc
            case "ContinueEncoder" =>println("add ContinueEncoder")
              val cone =  new ContinueEncoder(diff_thr = p.getInteger("diff_thr"), bins = p.getInteger("bins"), binningMethod = p.getString("binning_method"))
              funcSeq += cone.func
            case _ => println("mismatch encoder")
          }
        }
      }
    }
    val pipelineFunc = Function.chain(funcSeq)
    pipelineFunc(df)
  }

  def getDs: String = {
    ""
  }

  def getSt: String = {
    val stObj = JSON.parseObject(st, classOf[ST])
    cate = stObj.cate
    cont = stObj.cont
    custom = stObj.custom
    method = stObj.method
    params = stObj.params
    ""
  }

  def getOut: String = {
    ""
  }
}