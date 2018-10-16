package ml

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Polynomial expansion is the process of expanding your features into a polynomial space,
  * which is formulated by an n-degree combination of original dimensions.
  * A PolynomialExpansion class provides this functionality.
  * The example below shows how to expand your features into a 3-degree polynomial space.
  */
object SparkMLlibTransformerPolynomialExpansion extends App{
  val data = Array((0, 0.1), (1,1.0), (2, 0.8), (3, 0.5),(4, 0.2))


  val data1 = Array(
    Vectors.dense(2.0, 1.0),
    Vectors.dense(1.0, 1.0),
    Vectors.dense(0.0, 1.0),
    Vectors.dense(3.0, 1.0),
    Vectors.dense(4.0, 1.0)
  )

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(data1.map(Tuple1.apply)).toDF("features")

  df.show()
//  +---+--------+
//  | id|features|
//  +---+--------+
//  |  0|     0.1|
//  |  1|     1.0|
//  |  2|     0.8|
//  |  3|     0.5|
//  |  4|     0.2|
//  +---+--------+

  val polyExpansion = new PolynomialExpansion().setInputCol("features").setOutputCol("polyfeatures").setDegree(3)

  val pdf = polyExpansion.transform(df)
  pdf.show(false)

//  +---------+----------------------------------------+
//  |features |polyfeatures                            |
//  +---------+----------------------------------------+
//  |[2.0,1.0]|[2.0,4.0,8.0,1.0,2.0,4.0,1.0,2.0,1.0]   |
//  |[1.0,1.0]|[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]   |
//  |[0.0,1.0]|[0.0,0.0,0.0,1.0,0.0,0.0,1.0,0.0,1.0]   |
//  |[3.0,1.0]|[3.0,9.0,27.0,1.0,3.0,9.0,1.0,3.0,1.0]  |
//  |[4.0,1.0]|[4.0,16.0,64.0,1.0,4.0,16.0,1.0,4.0,1.0]|
//  +---------+----------------------------------------+
}
