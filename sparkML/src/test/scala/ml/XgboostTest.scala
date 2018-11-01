/**
  * create by colin on 2018/7/23
  */
package ml
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostClassificationModel}
import org.apache.spark.SparkConf
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.SparkTools

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

object XgboostTest extends SparkTools {

  import spark.implicits._
  import org.apache.spark.sql.functions._
  import spark.sql

  val flowMap = TreeMap(
    "FEA_DS" -> "",
    "FEA_SAMPLE_SPLIT" -> "",
    "FEA_TRANSFORM" -> "",
    "TRAIN_FILTER" -> "",
    "TRAIN_TRAINER" -> ""
  )

  //step 1
  def getFeaDS(str: String):DataFrame = {
    baseDf
  }

  import org.apache.spark.sql.functions._
  //step 2  return train/test
  def getSampleSplit(df:DataFrame,
                     label:String,
                     report_dst:String,
                     time_col: String="biz_report_expect_at",
                     index_col:String="apply_risk_id",
                     label_col:String="overdue_days",
                     test_size: Double=0.2,
                     method: String="oot", random_state: Int=7) = {

    val counts = df.count()
    val ootNumber = (counts * test_size).toInt
    val trainNumber = (counts * (1- test_size)).toInt

    val (train, test) = method match {
      case "oot" =>
        val test = df.sort(-$"$time_col".desc).limit(ootNumber)
        val train =  df.sort($"$time_col").limit(trainNumber)
        (train, test)
      case "random" =>
        val res = df.randomSplit(Array(1 - test_size, test_size))
        val test = res(0)
        val train = res(1)
        (train, test)
      case _ =>
        println("mismatch spliter method....return None")
        (None, None)
    }
  }

  // step 3 FEA_TRANSFORM
  def featureTransform() = {

  }

  //step 4 TRAIN_FILTER
  def featureFilter() = {

  }

  // setp 5 train data



  // from fea_ds get Dataframe
//  val sourceDf = getSrouce(flowMap("FEA_DS"))

  def getSplitP(str: String) = {
    0.2
  }

  // get split parameters
  val splitPara = getSplitP(flowMap("FEA_DS"))

  //getDataFrame from FEA_DS
//  val data = tempLaoke
//  data.createOrReplaceTempView("data")
//
//  val labelWhere = "case when affairs=0 then 0 else cast(1 as double) end as label"
//  val genderWhere = "case when gender='female' then 0 else cast(1 as double) end as gender"
//  val childrenWhere = "case when children='no' then 0 else cast(1 as double) end as children"
//
//  val newDf = sql(s"select $labelWhere, $genderWhere,age,yearsmarried,$childrenWhere,religiousness,education,occupation,rating from data")


  val labelCol = "d14"
  //字段转为特征向量
  // "d14" ,"ad","day7","m1","m3","m6","m12","m18","m24","m60"
  val featuresArray = Array("day7","m1","m3","m6","m12","m18","m24","m60")

  val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")
  val vecDf = assembler.transform(baseDf).withColumnRenamed("d14", "label")

  vecDf.show(10, truncate = false)


  def dfSpliter(df: String, label:String, report_dst: String, time_col: String="biz_report_expect_at", index_col:String ="apply_risk_id", label_col:String="overdue_days", test_size:Double=0.2, method:String="oot", random_state:Int=7)={

  }

  //spliter parameters
  val Array(trainData, testData) = vecDf.randomSplit(Array( 1- splitPara, splitPara))

  //feature transform

  val paramMap = List(
    "eta" -> 0.1f,
    "max_depth" -> 6, //数的最大深度。缺省值为6 ,取值范围为：[1,∞]
    "silent" -> 0, //取0时表示打印出运行时信息，取1时表示以缄默方式运行，不打印运行时信息。缺省值为0
    "objective" -> "reg:linear", //定义学习任务及相应的学习目标
    "eval_metric" -> "rmse", //校验数据所需要的评价指标
    "num_round" -> 10
    //"nthread"->  1  //XGBoost运行时的线程数。缺省值是当前系统可以获得的最大线程数

  ).toMap

  //    val trainDF = spark.sqlContext.read.format("libsvm").load(inputTrainPath)
  val xgboostModel = XGBoost.trainWithDataFrame(trainData, paramMap, 10, nWorkers = 2)
  xgboostModel.transform(testData).show()
}


