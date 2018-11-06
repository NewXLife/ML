package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.DataFrame

private[trainAtom] class Trainer(str: String) extends TrainProtocol[String]{
  val (st, ds, out) = parseTrainJson(str)

  def trainFilter(df: DataFrame): DataFrame = {
    null
  }

  def getDs: String = {
    val json = JSON.parseObject(ds)
    ""
  }

  def getSt: String = {
    case class XGBClassifierP(colsample_bytree: Double,
                              reg_lambda: Long,
                              silent: Boolean,
                              base_score: Double,
                              scale_pos_weight:Int,
                              eval_metric: String,
                              max_depth: Int,
                              n_jobs: Int,
                              early_stopping_rounds: Int,
                              n_estimators:Long,
                              random_state:Int,
                              reg_alpha:Int,
                              booster:String,
                              objective: String,
                              verbose: Boolean,
                              colsample_bylevel: Double,
                              subsample: Double,
                              learning_rate: Double,
                              gamma: Double,
                              max_delta_step: Int,
                              min_child_weight:Int
                             )
    val json = JSON.parseObject(st)
    val method = json.getString("method")
    val test_size = json.getString("test_size")
    val oversample = json.getString("oversample")
    val n_folds = json.getString("n_folds")
    val random_state = json.getString("random_state")
    val verbose = json.getString("verbose")

    val params = json.getString("params")
    val xgbcObj = JSON.parseObject(params, classOf[XGBClassifierP])

    ""
  }

  def getOut: String = {
    val json = JSON.parseObject(out)
    json.getString("dst")
    ""
  }
}
