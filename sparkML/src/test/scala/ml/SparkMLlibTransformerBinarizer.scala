package ml

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

/**
  * Binarization is the process of thresholding numerical features to binary (0/1) features.
  *
  * Binarizer takes the common parameters inputCol and outputCol,as well as the threshold for binarization.
  * Feature values greater than the threshold are binarized to 1.0;
  * values equal to or less than the threshold are binarized to 0.0.
  * Both Vector and Double types are supported for inputCol.
  */
object SparkMLlibTransformerBinarizer extends App{
  val data = Array((0, 0.1), (1,1.0), (2, 0.8), (3, 0.5),(4, 0.2))

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(data).toDF("id","features")

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

  val binarizer = new Binarizer().setInputCol("features").setOutputCol("b_features").setThreshold(0.5)


  println("threshold:", binarizer.getThreshold)
  binarizer.transform(df).show()

//0.5 为划分阈值， 小于等于0.5 为0类

//  +---+--------+----------+
//  | id|features|b_features|
//  +---+--------+----------+
//  |  0|     0.1|       0.0|
//  |  1|     1.0|       1.0|
//  |  2|     0.8|       1.0|
//  |  3|     0.5|       0.0|
//  |  4|     0.2|       0.0|
//  +---+--------+----------+
}
