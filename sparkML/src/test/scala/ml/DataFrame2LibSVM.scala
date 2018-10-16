package ml

import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object DataFrame2LibSVM  extends App{
  val pos = LabeledPoint(1.0 , Vectors.dense(1.0, 0.0,3.0))
  val neg = LabeledPoint(0.0 , Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))

  val spark = SparkSession.builder().appName("test").getOrCreate()
  val df = spark.createDataFrame(Seq(
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
  )).toDF("id", "category")

  val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")
    .fit(df)
  val indexed = indexer.transform(df)

  val encoder = new OneHotEncoder()
    .setInputCol("categoryIndex")
    .setOutputCol("categoryVec")

  val encoded = encoder.transform(indexed)
  encoded.show()
}
