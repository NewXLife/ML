package sta

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tree._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object DTBinsTestCategories extends StaFlow with App {
  import spark.implicits._
  val label = "d14"
  var testDf = loadCSVData("csv", "C:\\NewX\\newX\\ML\\docs\\testData\\base4.csv").withColumnRenamed(label, "label").withColumn("label", $"label".cast("int")).na.fill("NULL")
  testDf.show(20)

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
  val features = Array("extr","age")
//  testDf.describe(features:_*).show()

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
    //    .setImpurity("entropy") //树节点选择的不存度指标，取值为entroy/gini
    .setImpurity("gini") //
    //    .setMaxBins(100) //离散化"连续特征"的最大划分数，默认32，理论上分箱树越大粒度越细
    .setMaxDepth(5) //树的最大深度
    //numTrees 随机盛林需要训练的树的个数

//    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
    //    .setMinInstancesPerNode(10) //每个节点包含的最小样本数
    .setSeed(7)


  // Index labels, adding metadata to the label column.
  // Fit on whole dataset to include all labels in index.
  var binsMap: mutable.Map[String, Array[String]] = mutable.Map()
  for (f <- features) {
    println("feature============", f)
    //离散特征转为索引
    val cateFeatureIndexer = new StringIndexer()
      .setInputCol(f)
      .setOutputCol(f + "indexer").fit(testDf)

    //特征索引包装为vector，作为决策树输入特征
    val assembler = new VectorAssembler().setInputCols(Array(f + "indexer")).setOutputCol("features")

    // 将索引标签转换回原始标签
//    val featureConverter = new IndexToString().setInputCol(f + "indexer").setOutputCol(f+"_new").setLabels(cateFeatureIndexer.labels)

    val pipeLine = new Pipeline().setStages(Array(cateFeatureIndexer,assembler, dt)).fit(testDf)
    println("pipeline----------")
    val res = pipeLine.transform(testDf)
    res.show()
    val treeModel = pipeLine.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    println("features number = ",treeModel.numFeatures)
    println(treeModel.toDebugString)

    /**
      * 分箱信息
      */
//    val binsArray = DTUtils.extractConBins(treeModel)
//Array[Array[Double]]
    val binsCateArray = DTUtils.extractCateBins(treeModel)
    val binsCateArray2 = DTUtils.extractCateBins2(treeModel)
    //extractCateBins2
    println("------------------11111111111111")
    println(binsCateArray.map(x => x.mkString(",")).mkString(";"))
    println("------------------2222222222222")
    println(binsCateArray2.map(x => x.mkString(",")).mkString(";"))

    var binsMapStr: mutable.Map[Int, ArrayBuffer[String]] = mutable.Map()
    for(x <- binsCateArray.indices) binsMapStr += (x -> ArrayBuffer[String]())

//    val resBins = binsCateA transfer index2value
    val resFinal = res.select(f, f+"indexer").where($"${f+"indexer"}".isin(binsCateArray.flatten:_*)).distinct()
    resFinal.show()
   val fitV = resFinal.select(f+"indexer", f).collect().map(x => (x.getAs[Double](0), x.getAs[String](1)))


    //(1.0,初中),(8.0,大),(3.0,高中),(7.0,中),(11.0,NULL),(6.0,小学),(5.0,研究生),(0.0,博士),(10.0,究生),(9.0,研),(4.0,高)

    for(subStr <- fitV){
      for(idx <- binsCateArray.indices if binsCateArray(idx).contains(subStr._1)) binsMapStr(idx) += subStr._2
    }
    println(binsMapStr.mkString(","))
    //1 -> ArrayBuffer(大),0 -> ArrayBuffer(初中, 高中, 中, NULL, 小学, 研究生, 博士, 究生, 研, 高)
//    val resArray = ArrayBuffer[String]()
  val resArray =   for(subMap <- binsMapStr) yield subMap._2.mkString(",")
    resArray.foreach(println(_))
    binsMap += (f -> resArray.toArray)
  }
  //训练数据集划分
  //  val Array(trainData, testData) = vecDf.randomSplit(Array(0.7, 0.3))
  println(binsMap.mapValues(x=>x.mkString("[", ";", "]")))
}
