package ml

import ml.DecisionTreeClassiferTest._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tree.DTUtils
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object DecisionTreeClassificationBinsTest {
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

    val cols = "label,age,name,score,test"


    def String2Double(df: DataFrame, colsArray: Array[String]): Try[DataFrame] = {
      import org.apache.spark.sql.functions._
      Try(df.select(colsArray.map(f => col(f).cast(DoubleType)): _*))
    }

    val tDf = String2Double(df, cols.split(",")).get

    val binsMap: scala.collection.mutable.Map[String, Array[Double]] = scala.collection.mutable.Map()
    //    binsMap += ("1" -> Array(2.0))

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setImpurity("gini")
      .setMaxDepth(2)
      .setSeed(7)


    for (col <- cols.split(",")) {
      if(!col.equals("label")){
        val se = new VectorAssembler().setInputCols(Array(col)).setOutputCol("features")
        val ttdf = se.transform(tDf)
        ttdf.show()
        val model = dt.fit(ttdf)
        binsMap += ("col" -> DTUtils.extractConBins(model))
      }
    }
    println(binsMap.keySet.mkString(","))
    println(binsMap.values.mkString(","))
    spark.stop()
  }
}