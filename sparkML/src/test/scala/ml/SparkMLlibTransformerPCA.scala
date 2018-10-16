package ml

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * PCA is a statistical procedure that uses an orthogonal transformation
  * to convert a set of observations of possibly correlated variables
  * into a set of values of linearly uncorrelated variables called principal components.
  * A PCA class trains a model to project vectors to a low-dimensional space using PCA
  */
object SparkMLlibTransformerPCA extends  App{
  val data = Array(
    Vectors.sparse(5, Seq((1,3.0),(3, 3.0))),
    Vectors.dense(1,2,3,4,5),
    Vectors.dense(2.0, 3.0, 4.0, 5.0,2.3)
  )

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

  //"k", "the number of principal components (> 0)"
  val pca = new PCA().setInputCol("features").setOutputCol("pac_features").setK(3).fit(df)


  val testData = Array(Vectors.dense(10,20,30,40,50))
  val testDat = spark.createDataFrame(testData.map(Tuple1.apply)).toDF("features")

  val res = pca.transform(testDat)
  res.show(false)
}
