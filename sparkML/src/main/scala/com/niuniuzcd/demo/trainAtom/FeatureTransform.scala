package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.DataFrame

case class Encoder(method: String, params:String)
case class EncoderWrap(cols:Array[Any], encoders: Array[Encoder])
case class ST(cate: Array[EncoderWrap], cont: Array[EncoderWrap], custom: Array[EncoderWrap], method: String, params: String, verbose: Boolean)

private[trainAtom] class FeatureTransform(str: String) extends TrainProtocol[String]{
  val (st, ds, out) = parseTrainJson(str)
  val  featureTransform: DataFrame => DataFrame = (input: DataFrame) => input
  var cate: Array[EncoderWrap]= _
  var cont: Array[EncoderWrap]= _
  var custom: Array[EncoderWrap]= _
  var method: String = _
  var params: String = _

  def transform(df: DataFrame): DataFrame = {
    if (cate.length >0 ){
      for (encoderWrap <- cate){
        val cols = encoderWrap.cols
        for(encoder <- encoderWrap.encoders){
          val p = JSON.parseObject(encoder.params)
          encoder.method match {
            case "BaseEncoder" => new BaseEncoder(cate_thr = p.getDouble("cate_thr"), missing_thr = p.getDouble("missing_thr"), same_thr =p.getDouble("same_thr")).fit(df).transform(df)
            case "CountEncoder" => new CategoryEncoder(log_transform = p.getBoolean("log_transform"), unseen_value= p.getInteger("unseen_value"), smoothing =p.getInteger("smoothing")).fit(df).transform(df)
            case "ContinueEncoder" =>  new ContinueEncoder(diff_thr = p.getInteger("diff_thr"), bins = p.getInteger("bins"), binningMethod = p.getString("binning_method")).fit(df).transform(df)
          }
        }
      }
    }
    null
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