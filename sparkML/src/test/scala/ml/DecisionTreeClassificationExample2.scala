package ml

// $example on$
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
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


    val se = new VectorAssembler().setInputCols(Array("test")).setOutputCol("features")
    val data = se.transform(df)

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(4)
      .setSeed(7)



    val model = dt.fit(data)

    val mp = """\d|\d+\.\d+""".r
    val res = mp.findAllMatchIn(model.toDebugString).toArray
    res.foreach(println(_))

   println(model.toDebugString)

    println(model.featureImportances)
    println(model.numClasses)
    println(model.numFeatures)
    println(model.numNodes)
    println(model.rootNode.impurity)

    spark.stop()
  }
}