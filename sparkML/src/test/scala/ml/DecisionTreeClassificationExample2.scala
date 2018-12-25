package ml

// $example on$
import com.niuniuzcd.demo.util.Tools
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tree.DTUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

object DecisionTreeClassificationExample2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("DecisionTreeClassificationExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // $example on$

    val array1 = (0, "1", "2", 3, 0.0)
    val df = spark.createDataFrame(Seq(array1, (0, "1", "2", 13, 0.4), (0, "1", "2", 13, 0.5), (0, "1", "2", 23, 0.2), (1, "1", "2", 31, 0.7), (1, "1", "2", 35, 0.1)))
      .toDF("label", "age", "name", "score", "test")
    df.show(10, truncate = 0)
    /**
      * +-----+---+----+-----+----+
      * |label|age|name|score|test|
      * +-----+---+----+-----+----+
      * |0    |1  |2   |3    |0.0 |
      * |0    |   |2   |13   |0.4 |
      * |0    |1  |2   |13   |0.5 |
      * |0    |1  |2   |23   |0.2 |
      * |1    |1  |2   |31   |0.7 |
      * |1    |1  |2   |35   |0.1 |
      * +-----+---+----+-----+----+
      */
    def  String2String(df: DataFrame, colsArray: Array[String]):DataFrame ={
      import org.apache.spark.sql.functions._
      df.select(colsArray.map(f => col(f).cast("double")):_*)
    }
    val cols = "label,age,name,score,test"
    val se = new VectorAssembler().setInputCols(cols.split(",")).setOutputCol("features")

    val inputDf = String2String(df, cols.split(","))
    val data = se.transform(inputDf)
    println("data-----------vector----")
    data.show(10, truncate = 0)

    /**
      * +-----+---+----+-----+----+----------------------+
      * |label|age|name|score|test|features              |
      * +-----+---+----+-----+----+----------------------+
      * |0.0  |1.0|2.0 |3.0  |0.0 |[0.0,1.0,2.0,3.0,0.0] |
      * |0.0  |1.0|2.0 |13.0 |0.4 |[0.0,1.0,2.0,13.0,0.4]|
      * |0.0  |1.0|2.0 |13.0 |0.5 |[0.0,1.0,2.0,13.0,0.5]|
      * |0.0  |1.0|2.0 |23.0 |0.2 |[0.0,1.0,2.0,23.0,0.2]|
      * |1.0  |1.0|2.0 |31.0 |0.7 |[1.0,1.0,2.0,31.0,0.7]|
      * |1.0  |1.0|2.0 |35.0 |0.1 |[1.0,1.0,2.0,35.0,0.1]|
      * +-----+---+----+-----+----+----------------------+
      */

    val cols1 = "age,name,score,test"
    val staDf = inputDf.selectExpr("label", s"${Tools.getStackParams(cols1.split(","):_*)} as (feature, value)")
    staDf.show()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val aggDf = staDf.groupBy("feature").agg(
      callUDF("concat_ws", lit(","), callUDF("collect_list", $"value".cast("string"))).as("tvalue"),
      callUDF("concat_ws", lit(","), callUDF("collect_list", $"label".cast("string"))).as("lv")
    )
    println("agg dataframe-----------")
    aggDf.show()
    /**
      * +-------+--------------------+-------+
      * |feature|              tvalue|     lv|
      * +-------+--------------------+-------+
      * |   name|2.0,2.0,2.0,2.0,2...|0.0,1.0|
      * |    age|1.0,1.0,1.0,1.0,1...|0.0,1.0|
      * |  score|3.0,13.0,13.0,23....|0.0,1.0|
      * |   test|0.0,0.4,0.5,0.2,0...|0.0,1.0|
      * +-------+--------------------+-------+
      */
//        aggDf.withColumn("bin", dtBins($"tvalue",$"lv"))


//    val localVector = new MLDenseVector(Array(1, 0, 0, 0, 0))

//    def dtBins: UserDefinedFunction  = udf{(tvalue: String, lv:String) =>{
//      val tf = tvalue.split(",").map(x=> x.toDouble)
//      val tlable = lv.split(",").map(x=> x.toDouble)
//      spark.createDataFrame(Seq(tf, tlable))
//    }
//    }

    val dt = new DecisionTreeClassifier()
      .setLabelCol("lv")
      .setFeaturesCol("tvalue")
      .setImpurity("gini")
      .setMaxDepth(4)
      .setSeed(7)


    val model = dt.fit(aggDf)

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