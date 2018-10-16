package ml

import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * The Discrete Cosine Transform transforms a length N real-valued sequence in the time domain
  * into another length N real-valued sequence in the frequency domain.
  * A DCT class provides this functionality,
  * implementing the DCT-II and scaling the result by 1/2‾√ such that the representing matrix for the transform is unitary.
  * No shift is applied to the transformed sequence (e.g. the 0th element of the transformed sequence is the 0th DCT coefficient and not the N/2th).
  */
object SparkMLlibTransformerDCT extends App{

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

  //Indicates whether to perform the inverse DCT (true) or forward DCT (false).
  //Default: false
  val dct = new DCT().setInputCol("features").setOutputCol("dctfeatures").setInverse(false)

  val res = dct.transform(df)
  res.show(false)

//  +---------+----------------------------------------+
//  |features |dctfeatures                             |
//  +---------+----------------------------------------+
//  |[2.0,1.0]|[2.1213203435596424,0.7071067811865476] |
//  |[1.0,1.0]|[1.414213562373095,0.0]                 |
//  |[0.0,1.0]|[0.7071067811865475,-0.7071067811865476]|
//  |[3.0,1.0]|[2.82842712474619,1.4142135623730951]   |
//  |[4.0,1.0]|[3.5355339059327373,2.121320343559643]  |
//  +---------+----------------------------------------+

}
