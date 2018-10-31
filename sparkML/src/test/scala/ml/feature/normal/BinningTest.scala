package ml.feature.normal

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{QuantileDiscretizer, VectorAssembler}
import util.SparkTools

object BinningTest extends SparkTools{
  baseDf.show(5, truncate = false)

//  等频分箱，等同pandas 的pcut
  val qd = new QuantileDiscretizer().setInputCol("m1").setOutputCol("q7day")
    .setNumBuckets(10)      //设置分箱数
    .setRelativeError(0) //设置precision-控制相对误差,设置为0时，将会计算精确的分位点（计算代价较高）。
    .setHandleInvalid("keep") //对于空值的处理 handleInvalid = ‘keep’时，可以将空值单独分到一箱,  "skip" 不要空值
    .fit(baseDf)

  qd.getSplits.foreach(println(_))//分箱区别
  qd.transform(baseDf).show() //左闭，右开区间，离散化数据

  /**
    * 等距离分箱,等同pandas 的cut
    * 桶必须是排好序的，并且不包含重复元素，至少有两个元素
    */
  val a = spark.sparkContext.parallelize(List(1.1,1.2,1.3,2.0,2.1,7.4,7.5,7.6,8.8,9.0),3)
  // 入参是一个数/或者是一个数组
  // 先用排好序的数组的边界值来得出两个桶之间的间距
  // 如果是一个数组 表示： [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
  // 返回一个元组， 分箱边界和分箱内的统计数目
  val (bucket, ct )  = a.histogram(5)
  bucket.foreach(x => println(f"$x%2.2f"))
  /**
    * 1.1
    * 2.68
    * 4.26
    * 5.84
    * 7.42
    * 9.0
    */

  // 决策树分箱, 该决策树和python的sk-learn 出的结果不一致，需要再研究
  // 训练决策树模型，单特征分箱
  val dt = new DecisionTreeClassifier().setLabelCol("d14")
    .setFeaturesCol("feature")
    .setImpurity("gini") // gini系数 or entropy [ˈentrəpi](熵)
//    .setMaxBins(100) //离散化"连续特征"的最大划分数
    .setMaxDepth(4) //树的最大深度
//    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
    .setMinInstancesPerNode(1) //每个节点包含的最小样本数
    .setSeed(123456)

  baseDf.printSchema()

  val sm = new VectorAssembler()
    .setInputCols(Array("day7"))
    .setOutputCol("feature")

  val data = sm.transform(baseDf)
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

  spark.stop()
}
