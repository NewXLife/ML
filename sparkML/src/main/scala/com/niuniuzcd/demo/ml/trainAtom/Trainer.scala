package com.niuniuzcd.demo.ml.trainAtom

import com.alibaba.fastjson.JSON
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.sql.DataFrame

private[trainAtom] class Trainer(str: String) extends TrainProtocol[String] {
  private val (st, ds, out) = parseTrainJson(str)
  var Model: XGBoostClassificationModel = _

  private val stJSONStr = JSON.parseObject(st)

  private var method: String = _
  private var test_size: Double = _
  private var oversample: String = _
  private var n_folds: String = _
  private var random_state: Int = _
  private var verbose: String = _
  private var params: String = _

  def getDs: String = {
    val json = JSON.parseObject(ds)
    ""
  }

  def LinearRegression(df: DataFrame) = {
    df
  }

  def LogisticRegression(df: DataFrame) = {
    df
  }

  /** Booster-params
    *
    * eta
    * step size shrinkage used in update to prevents overfitting. After each boosting step, we
    * can directly get the weights of new features and eta actually shrinks the feature weights
    * to make the boosting process more conservative. [default=0.3] range: [0,1]
    *
    * gamma
    * minimum loss reduction required to make a further partition on a leaf node of the tr
    * the larger, the more conservative the algorithm will be. [default=0] range: [0,Double.MaxValue]
    *
    * maxDepth
    * maximum depth of a tree, increase this value will make model more complex / likely to be overfitting. [default=6] range: [1, Int.MaxValue]
    *
    * minChildWeight
    * minimum sum of instance weight(hessian) needed in a child. If the tree partition step results
    * in a leaf node with the sum of instance weight less than min_child_weight, then the building
    * process will give up further partitioning. In linear regression mode, this simply corresponds
    * to minimum number of instances needed to be in each node. The larger, the more conservative
    * the algorithm will be. [default=1] range: [0, Double.MaxValue]
    *
    * maxDeltaStep
    * Maximum delta step we allow each tree's weight estimation to be. If the value is set to 0, it
    * means there is no constraint. If it is set to a positive value, it can help making the update
    * step more conservative. Usually this parameter is not needed, but it might help in logistic
    * regression when class is extremely imbalanced. Set it to value of 1-10 might help control the
    * update. [default=0] range: [0, Double.MaxValue]
    *
    * subsample
    * subsample ratio of the training instance. Setting it to 0.5 means that XGBoost randoml
    * collected half of the data instances to grow trees and this will prevent overfitting.
    * [default=1] range:(0,1]
    *
    * colsampleBytree
    * subsample ratio of columns when constructing each tree. [default=1] range: (0,1]
    *
    * colsampleBylevel
    * subsample ratio of columns for each split, in each level. [default=1] range: (0,1]
    *
    * lambda
    * L2 regularization term on weights, increase this value will make model more conservative.[default=1]
    *
    * alpha
    * L1 regularization term on weights, increase this value will make model more conservative.[default=0]
    *
    * treeMethod
    * The tree construction algorithm used in XGBoost. options: {'auto', 'exact', 'approx'}, [default='auto']
    *
    * growPolicy
    * growth policy for fast histogram algorithm
    *
    * maxBin
    * maximum number of bins in histogram
    *
    * sketchEps
    * This is only used for approximate greedy algorithm.
    * This roughly translated into O(1 / sketch_eps) number of bins. Compared to directly select
    * number of bins, this comes with theoretical guarantee with sketch accuracy.[default=0.03] range: (0, 1)
    *
    * scalePosWeight
    * Control the balance of positive and negative weights, useful for unbalanced classes. A typical
    * value to consider: sum(negative cases) / sum(positive cases).   [default=1]
    *
    * sampleType
    * Parameter for Dart booster.
    * Type of sampling algorithm. "uniform": dropped trees are selected uniformly.
    * "weighted": dropped trees are selected in proportion to weight. [default="uniform"]
    *
    * normalizeType
    * Parameter of Dart booster.
    * type of normalization algorithm, options: {'tree', 'forest'}. [default="tree"]
    *
    * rateDrop
    * Parameter of Dart booster.
    * dropout rate. [default=0.0] range: [0.0, 1.0]
    *
    * skipDrop
    * Parameter of Dart booster.
    * probability of skip dropout. If a dropout is skipped, new trees are added in the same manner as gbtree. [default=0.0] range: [0.0, 1.0]
    *
    * lambdaBias
    * Parameter of linear booster
    * L2 regularization term on bias, default 0(no L1 reg on bias because it is not important)
    *
    * treeLimit
    * number of trees used in the prediction; defaults to 0 (use all trees)
    */

  val trainer = (df: DataFrame) => {
    import com.kuainiu.beidou.ml.evaluation.ModelS._
    getSt
    method match {
      case XGBClassifier => XGB(df)
      case _ => df
    }
  }

  def DecisionTreeClassifier(df: DataFrame) = {
    df
  }

  def KMeans(df: DataFrame): DataFrame = {
    df
  }

  def XGB(df: DataFrame) = {
    case class XGBClassifierP(colsample_bytree: Double,
                              reg_lambda: Long,
                              silent: Boolean,
                              base_score: Double,
                              scale_pos_weight: Int,
                              eval_metric: String,
                              max_depth: Int,
                              n_jobs: Int,
                              early_stopping_rounds: Int,
                              n_estimators: Long,
                              random_state: Int,
                              reg_alpha: Int,
                              booster: String,
                              objective: String,
                              verbose: Boolean,
                              colsample_bylevel: Double,
                              subsample: Double,
                              learning_rate: Double,
                              gamma: Double,
                              max_delta_step: Int,
                              min_child_weight: Int
                             )
    val xgbcObj = JSON.parseObject(params, classOf[XGBClassifierP])
    val numRound = 800
    require(numRound > 10, "numRound must bigger than ten")
    val paramMap = Map(
      "colsampleBytree" -> xgbcObj.colsample_bytree,
      "lambda" -> xgbcObj.reg_lambda,
      "silent" -> 0,
      "baseScore" -> xgbcObj.base_score,
      "scalePosWeight" -> xgbcObj.scale_pos_weight,
      "evalMetric" -> xgbcObj.eval_metric, //校验数据所需要的评价指标
      "maxDepth" -> xgbcObj.max_depth, //数的最大深度。缺省值为6 ,取值范围为：[1,∞]
      //      "n_jobs" -> 1,
      "numEarlyStoppingRounds" -> xgbcObj.early_stopping_rounds,
      //      "n_estimators" -> 1000,
      //      "random_state" -> 0,
      "numWorkers" -> 4,
      "alpha" -> xgbcObj.reg_alpha,
      "booster" -> xgbcObj.booster, //spark 目前只支持 gbtree（默认也是这个参数），设置其它参数会抛异常
      "objective" -> xgbcObj.objective, //定义学习任务及相应的学习目标  "reg:linear"
      //      "verbose" -> false,
      "colsampleBylevel" -> xgbcObj.colsample_bylevel,
      "subsample" -> xgbcObj.subsample,
      "eta" -> xgbcObj.learning_rate, // learning_rate
      "gamma" -> xgbcObj.gamma,
      "maxDeltaStep" -> xgbcObj.max_delta_step,
      "minChildWeight" -> xgbcObj.min_child_weight,
      "nthread" -> 4 //XGBoost运行时的线程数。缺省值是当前系统可以获得的最大线程数

    )
    //version 0.72
    //XGBModel = XGBoost.trainWithDataFrame(df, paramMap, numRound, nWorkers = 2)

    //version 0.80
    Model = new XGBoostClassifier(paramMap).setFeaturesCol("features").setLabelCol("label").fit(df)
    df
  }

  /**
    * general-params
    *
    * "numRound" ->  Round, //  The number of rounds for boosting
    * "numWorkers" -> 2, //number of workers used to train xgboost model. default: 1
    * "nthread" -> 2, //number of threads used by per worker. default 1
    * "useExternalMemory" -> false, //whether to use external memory as cache. default: false
    * "silent" -> 0, //取0时表示打印出运行时信息，取1时表示以缄默方式运行，不打印运行时信息。缺省值为0（0 means printing running messages, 1 means silent mode. default: 0）
    * "customObj" -> null, //customized objective function provided by user. default: null
    * "customEval" -> null, //customized evaluation function provided by user. default: null
    * "missing" -> Float.NaN, // the value treated as missing. default: Float.NaN
    * "trackerConf"-> //"Rabit tracker configurations"
    * "seed" -> 0, // random seed
    * "timeoutRequestWorkers" -> 30 * 60 * 1000L, //the maximum time to wait for the job requesting new workers. default: 30 minutes
    * "checkpointPath" -> "", //The hdfs folder to load and save checkpoint boosters. default: `empty_string`
    * "checkpointInterval" -> "", //Param for set checkpoint interval (&gt;= 1) or disable checkpoint (-1).  E.g. 10 means that the trained model will get checkpointed every 10 iterations
    * //Note: `checkpoint_path` must also be set if the checkpoint interval is greater than 0.
    * "trankerConf" -> "rabit tracker configurations"
    * "seed" -> "random seed"
    * "leafPredictionCol" -> "Param for leaf prediction column name"
    * "contribPredictionCol" -> "Param for contribution prediction column name."
    * "baseMarginCol" -> "Param for initial prediction (aka base margin) column name."
    * "groupCol" -> "Param for group column name."
    * "numClass" -> "number of classes"
    *
    * @return
    */

  /**
    * learning-task-prams
    *
    * objective
    * Specify the learning task and the corresponding learning objective.
    * options: reg:linear, reg:logistic, binary:logistic, binary:logitraw, count:poisson,
    * multi:softmax, multi:softprob, rank:pairwise, reg:gamma. default: reg:linear
    *
    * objectiveType
    * The learning objective type of the specified custom objective and eval.
    * Corresponding type will be assigned if custom objective is defined
    * options: regression, classification. default: null
    *
    * baseScore
    * the initial prediction score of all instances, global bias. default=0.5
    *
    * evalMetric
    * evaluation metrics for validation data, a default metric will be assigned according to
    * objective(rmse for regression, and error for classification, mean average precision for ranking).
    * options: rmse, mae, logloss, error, merror, mlogloss, auc, aucpr, ndcg, map, gamma-deviance
    *
    * trainTestRatio
    * Fraction of training points to use for testing.
    *
    * numEarlyStoppingRounds
    * If non-zero, the training will be stopped after a specified number of consecutive increases in any evaluation metric.
    *
    * maximizeEvaluationMetrics
    * define the expected optimization to the evaluation metrics, true to maximize otherwis minimize it
    *
    */
  def getSt: String = {
    method = stJSONStr.getString("method")
    test_size = stJSONStr.getDouble("test_size")
    oversample = stJSONStr.getString("oversample")
    n_folds = stJSONStr.getString("n_folds")
    random_state = stJSONStr.getIntValue("random_state")
    verbose = stJSONStr.getString("verbose")
    params = stJSONStr.getString("params")
    ""
  }

  def getOut: String = {
    val json = JSON.parseObject(out)
    json.getString("dst")
    ""
  }
}
