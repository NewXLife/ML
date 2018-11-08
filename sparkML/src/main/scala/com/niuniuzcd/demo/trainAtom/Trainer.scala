package com.niuniuzcd.demo.trainAtom

import com.alibaba.fastjson.JSON
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.sql.DataFrame

private[trainAtom] class Trainer(str: String) extends TrainProtocol[String]{
  val (st, ds, out) = parseTrainJson(str)
  var XGBModel: XGBoostModel = _

  def getDs: String = {
    val json = JSON.parseObject(ds)
    ""
  }

  val XGBtrainer = (df: DataFrame) => {
        val numRound = 800
        require(numRound > 10, "numRound must bigger than ten")
        val paramMap = List(
          "eta" -> 0.1f,
          "max_depth" -> 6, //数的最大深度。缺省值为6 ,取值范围为：[1,∞]
          "silent" -> 0, //取0时表示打印出运行时信息，取1时表示以缄默方式运行，不打印运行时信息。缺省值为0
          "objective" -> "reg:linear", //定义学习任务及相应的学习目标
          "eval_metric" -> "rmse", //校验数据所需要的评价指标
          "num_round" -> numRound
          //"nthread"->  1  //XGBoost运行时的线程数。缺省值是当前系统可以获得的最大线程数
        ).toMap

    //    val trainDF = spark.sqlContext.read.format("libsvm").load(inputTrainPath)

    XGBModel = XGBoost.trainWithDataFrame(df, paramMap, numRound, nWorkers = 2)
    df
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
