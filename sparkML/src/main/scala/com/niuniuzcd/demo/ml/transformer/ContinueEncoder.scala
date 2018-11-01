package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{QuantileDiscretizer, VectorAssembler}
import org.apache.spark.rdd.ParallelCollectionRDD
import org.apache.spark.sql.DataFrame

/**
  * 将连续型变量转化为离散型
  *
  * 仅适用于cont， 支持缺失值
  *
  * Parameters
  * ----------
  * diff_thr : int, default: 20
  * 不同取值数高于该值才进行离散化处理，不然原样返回
  *
  * binning_method : str, default: 'dt', {'dt', 'qcut', 'cut'}
  * 分箱方法,:
  * 'dt' which uses decision tree.
  * 'cut' which cuts data by the equal intervals.
  * 'qcut' which cuts data by the equal quantity.
  * default is 'dt'. if y is None, default auto changes to 'qcut'.
  *
  * bins : int, default: 10
  * 分箱数目， 当binning_method='dt'时，该参数失效
  *
  * 决策树分箱方法使用的决策树参数
  */
class ContinueEncoder(var df: DataFrame, val bins:Int = 10) {
  private final val spark = df.sparkSession
  import spark.implicits._


  /**
    * 等距离分箱,等同pandas 的cut
    * 桶必须是排好序的，并且不包含重复元素，至少有两个元素
    */


  def cut(colName: String) = {
    val rd = df.groupBy("C1").count().rdd.map(x => x.getDouble(0)).histogram(3)
//
//    // 入参是一个数/或者是一个数组
//    // 先用排好序的数组的边界值来得出两个桶之间的间距
//    // 如果是一个数组 表示： [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
//    // 返回一个元组， 分箱边界和分箱内的统计数目
//    val (bucket, ct )  = rd.histogram(bins)
//    bucket.foreach(x => println(f"$x%2.2f"))
  }

  /**
    * 等频分箱，等同pandas 的pcut
    */
  def qcut = {
    val qd = new QuantileDiscretizer().setInputCol("m1").setOutputCol("q7day")
      .setNumBuckets(10)      //设置分箱数
      .setRelativeError(0) //设置precision-控制相对误差,设置为0时，将会计算精确的分位点（计算代价较高）。
      .setHandleInvalid("keep") //对于空值的处理 handleInvalid = ‘keep’时，可以将空值单独分到一箱,  "skip" 不要空值
      .fit(df)

    qd.getSplits.foreach(println(_))//分箱区别
    qd.transform(df).show() //左闭，右开区间，离散化数据

  }

  /**
    * 决策树分箱, 该决策树和python的sk-learn 出的结果不一致，需要再研究
    * 训练决策树模型，单特征分箱
    */
  def dt = {
    val dt = new DecisionTreeClassifier().setLabelCol("d14")
      .setFeaturesCol("feature")
      .setImpurity("gini") // gini系数 or entropy [ˈentrəpi](熵)
      //    .setMaxBins(100) //离散化"连续特征"的最大划分数
      .setMaxDepth(4) //树的最大深度
      //    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(1) //每个节点包含的最小样本数
      .setSeed(7)

    df.printSchema()

    val sm = new VectorAssembler()
      .setInputCols(Array("day7"))
      .setOutputCol("feature")

    val data = sm.transform(df)
    data.show(5, truncate = 0)

    //input fit must be vector
    val model = dt.fit(data)

    //  val mp = """\d|\d+\.\d+""".r
    //  val res = mp.findAllMatchIn(model.toDebugString).toArray
    //  res.foreach(println(_))

    println(model.toDebugString)
    println(model.featureImportances)
    println(model.numClasses)
    println(model.numFeatures)
    println(model.numNodes)
    println(model.rootNode.impurity)
  }



}
