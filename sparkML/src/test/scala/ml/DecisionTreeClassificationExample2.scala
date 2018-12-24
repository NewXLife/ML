package ml

// $example on$
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tree.DTUtils
// $example off$
import org.apache.spark.sql.SparkSession

object DecisionTreeClassificationExample2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("DecisionTreeClassificationExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // $example on$
    val df = spark.createDataFrame(Seq((0, "1", "2", 3, 0.0), (0, "", "2", 13, 0.4), (0, "1", "2", 13, 0.5), (0, "1", "2", 23, 0.2), (1, "1", "2", 31, 0.7), (1, "1", "2", 35, 0.1)))
      .toDF("label", "age", "name", "score", "test")
    df.show(10, truncate = 0)


    val se = new VectorAssembler().setInputCols(Array("test")).setOutputCol("features")
    val data = se.transform(df)
    println("data-----------vector----")
    data.show(10, truncate = 0)

    // Train a DecisionTree model.
    //DecisionTreeClassificationModel
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setImpurity("gini")
      .setMaxDepth(4)
      .setSeed(7)



    val model = dt.fit(data)

    val mp = """\d|\d+\.\d+""".r
    val res = mp.findAllMatchIn(model.toDebugString).toArray
    println("resutls-----------")
    println(res.mkString(","))

    println("model debug---------")
   println(model.toDebugString)

    println(" Estimate of the importance of each feature.")
    println(model.featureImportances)
    println(model.rootNode.prediction)

    println("number numClasses--------------")
    println(model.numClasses)

    println("number features------------")
    println(model.numFeatures)

    println("number nodes---------------")
    println(model.numNodes)

    println("Impurity measure at this node")
    println(model.rootNode.impurity)

    println(DTUtils.extractConBins(model).mkString(","))
    println(DTUtils.extractBinInfo(model).mkString(","))
    spark.stop()
  }
}