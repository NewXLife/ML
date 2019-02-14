package sta

import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.feature.VectorAssembler
import sta.DTBinsTest.{loadCSVData, spark, vectorAssembler}

object XGBBinsTest extends App{
  import spark.implicits._
  import org.apache.spark.sql.functions._
  val label = "d14"
  var df = loadCSVData("csv", "C:\\NewX\\newX\\ML\\docs\\testData\\base3.csv").withColumnRenamed(label, "label").withColumn("label", $"label".cast("int"))
  df.show()

  /**
    * +-----+----+----+----+----+----+----+----+----+---+---------+
    * |label|day7|  m1|  m3|  m6| m12| m18| m24| m60|age|       ad|
    * +-----+----+----+----+----+----+----+----+----+---+---------+
    * |    0|-1.0| 2.0| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/19|
    * |    0|-1.0| 2.0| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/20|
    */

  /**
    * use dt-tree all feature must be number
    */
  //dt统计特征数组
  val features = Array("m1","m3","m6")

  val paramMap = Map(
    "maxDepth" -> 4, //数的最大深度。缺省值为6 ,取值范围为：[1,∞]
    //      "n_jobs" -> 1,
    "numWorkers" -> 1,
    "booster" -> "gbtree", //spark 目前只支持 gbtree（默认也是这个参数），设置其它参数会抛异常 //General Balanced Trees
    "nthread" -> 1 //XGBoost运行时的线程数。缺省值是当前系统可以获得的最大线程数

  )
  //version 0.72
  //XGBModel = XGBoost.trainWithDataFrame(df, paramMap, numRound, nWorkers = 2)

  //version 0.80
  val vectorAssembler = new VectorAssembler()
  val f = "m1"
  val staDF = df.withColumn(f, $"$f".cast("double")).where($"$f".notEqual(Double.NaN))
  val singleDf =vectorAssembler.setInputCols(Array(f)).setOutputCol("features").transform(staDF)
  val model = new XGBoostClassifier(paramMap).setFeaturesCol("features").setLabelCol("label").fit(singleDf)
  println(model.getThresholds.mkString(","))
}
