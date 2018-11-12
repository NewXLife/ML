package com.niuniuzcd.demo.ml.trainAtom

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}

case class Estimator(name: String, params: String)

private[trainAtom] class Filter(str: String) extends TrainProtocol[String] {
  val (st, ds, out) = parseTrainJson(str)
  val stJsonStr: JSONObject = JSON.parseObject(st)

  def getDs: String = ""

  var dataset: DataFrame = _

  def getSt: String = {
    case class Model(estimator: Estimator, threshold: String)
    val method = stJsonStr.getString("method")
    val estimatorStr = stJsonStr.getString("params")
    val esObj = JSON.parseObject(estimatorStr, classOf[Model])
    val estimator = esObj.estimator.name
    val threshold = esObj.threshold
    ""
  }

  def getOut: String = ""

  val filter: DataFrame => DataFrame = (df: DataFrame) =>  {
    println("input filter dataframe as flows:")
    df.show(5)
    getSt

    val vf = new VectorAssembler().setInputCols(df.schema.fields.filter(f => !f.name.equals("label")).map(x => x.name)).setOutputCol("features").transform(df)

    val selector = new ChiSqSelector()
      .setNumTopFeatures(20)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    val result = selector.fit(vf).transform(vf)
    println("after filter dataframe as flows:")
    result.show(5)

    /**
      * +-----+----+------+------+------+-------+-------+-------+-------+-----------------+--------------------+--------------------+
      * |label|7day|1month|3month|6month|12month|18month|24month|60month|       apply_date|            features|    selectedFeatures|
      * +-----+----+------+------+------+-------+-------+-------+-------+-----------------+--------------------+--------------------+
      * |  0.0|-1.0|  -1.0|  -1.0|  -1.0|    2.0|    6.0|    6.0|    6.0|7.239214973779806|[-1.0,-1.0,-1.0,-...|[-1.0,-1.0,-1.0,-...|
      * |  0.0|-1.0|  -1.0|  -1.0|   2.0|    4.0|    5.0|    5.0|    9.0|7.239214973779806|[-1.0,-1.0,-1.0,2...|[-1.0,-1.0,-1.0,2...|
      * |  0.0|-1.0|  -1.0|  -1.0|   2.0|   21.0|   22.0|   22.0|   22.0|7.239214973779806|[-1.0,-1.0,-1.0,2...|[-1.0,-1.0,-1.0,2...|
      * |  0.0|-1.0|  -1.0|  -1.0|   7.0|    8.0|    9.0|    9.0|    9.0|7.239214973779806|[-1.0,-1.0,-1.0,7...|[-1.0,-1.0,-1.0,7...|
      * |  0.0|-1.0|  -1.0|  -1.0|   8.0|   14.0|   20.0|   20.0|   20.0|7.239214973779806|[-1.0,-1.0,-1.0,8...|[-1.0,-1.0,-1.0,8...|
      * +-----+----+------+------+------+-------+-------+-------+-------+-----------------+--------------------+--------------------+
      */
    dataset = result
    dataset
  }
}
