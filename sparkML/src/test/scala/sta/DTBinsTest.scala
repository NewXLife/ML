package sta

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tree.DTUtils.{recursiveExtraInfo, recursiveExtraSplits}
import org.apache.spark.ml.tree._

import scala.collection.mutable

object DTBinsTest extends StaFlow with App {
  val label = "d14"
  var testDf = loadCSVData("csv","C:\\NewX\\newX\\ML\\docs\\testData\\base3.csv").withColumnRenamed(label, "label")
  testDf.show()

  import spark.implicits._
  import org.apache.spark.sql.functions._


  testDf = testDf.withColumn("m1", $"m1".cast("int")).withColumn("label", $"label".cast("int"))

  /**
    * use dt-tree all feature must be number
    */
  //字段转为特征向量
  val features = Array("m1")
  val assembler = new VectorAssembler().setInputCols(Array("label")).setOutputCol("features")
  val vecDf = assembler.transform(testDf.na.fill(-99))
  println("assembler-------------------")
  vecDf.show()

  //索引标签，将元数据添加到标签中
  val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(vecDf)
  println("labelIndexer-------------------")
  labelIndexer.transform(vecDf)

  //对特征进行索引
  val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(5).fit(vecDf)
  println("featureIndexer------------------")
  featureIndexer.transform(vecDf)

  //训练数据集划分
//  val Array(trainData, testData) = vecDf.randomSplit(Array(0.7, 0.3))


  // 训练决策树模型
  val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel")
    .setFeaturesCol("features")
    //    .setImpurity("entropy") //
    .setImpurity("gini") //
    //    .setMaxBins(100) //离散化"连续特征"的最大划分数
    .setMaxDepth(4) //树的最大深度
    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
    //    .setMinInstancesPerNode(10) //每个节点包含的最小样本数
    .setSeed(7)

  // 将索引标签转换回原始标签
  val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
  val pipeLine = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter)).fit(vecDf)
  val treeModel = pipeLine.stages(2).asInstanceOf[DecisionTreeClassificationModel]

  println(treeModel.toDebugString)
  /**
    * If (feature 0 <= 5.0)
    * Predict: 0.0
    * Else (feature 0 > 5.0)
    * If (feature 0 <= 10.0)
    * Predict: 1.0
    * Else (feature 0 > 10.0)
    * Predict: 0.0
    */

  val res = DTUtils.extractBinInfo(treeModel)
  res.foreach(x => println(x.range, x.hitInfo.mkString(",")))

  /**
    * 分箱信息
    */
  val binsArray = DTUtils.extractConBins(treeModel)
  val binsCateArray = DTUtils.extractCateBins(treeModel)

  println(binsArray.mkString(","))
  println(binsCateArray.mkString(","))
//  val pipelineModel = pipeLine.fit(vecDf)

  //val predictions =  pipelineModel.transform(vecDf)
}
