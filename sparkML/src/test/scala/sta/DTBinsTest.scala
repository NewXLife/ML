package sta

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tree.DTUtils.{recursiveExtraInfo, recursiveExtraSplits}
import org.apache.spark.ml.tree._

import scala.collection.mutable

object DTBinsTest extends StaFlow with App {
  import spark.implicits._
  import org.apache.spark.sql.functions._
  val label = "d14"
  var testDf = loadCSVData("csv", "C:\\NewX\\newX\\ML\\docs\\testData\\base3.csv").withColumnRenamed(label, "label").na.drop().withColumn("label", $"label".cast("int"))
  testDf.show()

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
  //字段转为特征向量
  val features = Array("m60")
//  val features = Array("m1", "m60")
  /**
    * +-----+----+---+----+----+----+----+----+----+---+---------+--------+
    * |label|day7| m1|  m3|  m6| m12| m18| m24| m60|age|       ad|features|
    * +-----+----+---+----+----+----+----+----+----+---+---------+--------+
    * |    0|-1.0|  2| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/19|   [0.0]|
    * |    0|-1.0|  2| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/20|   [0.0]|
    */
  // 训练决策树模型
  val dt = new DecisionTreeClassifier().setLabelCol("label")
    .setFeaturesCol("features")
    //    .setImpurity("entropy") //
    .setImpurity("gini") //
    //    .setMaxBins(100) //离散化"连续特征"的最大划分数
    .setMaxDepth(4) //树的最大深度

    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
    //    .setMinInstancesPerNode(10) //每个节点包含的最小样本数
    .setSeed(7)

  val singleDf = new VectorAssembler().setInputCols(Array("m1")).setOutputCol("features").transform(testDf.withColumn("m1", $"m1".cast("int")))
  println(DTUtils.extractConBins(dt.fit(singleDf)).mkString(","))

  // Index labels, adding metadata to the label column.
  // Fit on whole dataset to include all labels in index.
  for (f <- features) {
    println("feature============", f)
    //离散特征转为索引
    val cateFeatureIndexer = new StringIndexer()
      .setInputCol(f)
      .setOutputCol(f + "indexer").fit(testDf)

    //特征索引包装为vector，作为决策树输入特征
    val assembler = new VectorAssembler().setInputCols(Array(f + "indexer")).setOutputCol("features")

    // 将索引标签转换回原始标签
    val featureConverter = new IndexToString().setInputCol(f + "indexer").setOutputCol(f+"_new").setLabels(cateFeatureIndexer.labels)

    val pipeLine = new Pipeline().setStages(Array(cateFeatureIndexer,assembler, dt, featureConverter)).fit(testDf)
    pipeLine.transform(testDf).show()
    val treeModel = pipeLine.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    println("features number = ",treeModel.numFeatures)
    println(treeModel.toDebugString)

//    val res = DTUtils.extractBinInfo(treeModel)
//    res.foreach(x => println(x.range, x.hitInfo.mkString(",")))

    /**
      * 分箱信息
      */
    val binsArray = DTUtils.extractConBins(treeModel)
    val binsCateArray = DTUtils.extractCateBins(treeModel)

    println(binsArray.mkString(","))
    println(binsCateArray.mkString(","))
  }

  //训练数据集划分
  //  val Array(trainData, testData) = vecDf.randomSplit(Array(0.7, 0.3))
}
