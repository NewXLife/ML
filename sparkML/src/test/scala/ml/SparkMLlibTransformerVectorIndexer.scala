package ml

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**主要作用：提高决策树或随机森林等ML方法的分类效果。
VectorIndexer是对数据集特征向量中的类别（离散值）特征（index categorical features categorical features ）进行编号。
它能够自动判断那些特征是离散值型的特征，并对他们进行编号，具体做法是通过设置一个maxCategories，
特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。
某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）
  */
object SparkMLlibTransformerVectorIndexer extends App{

  val data1 = Array(
    Vectors.sparse(3,Array(0,1,2),Array(2.0,5.0,7.0)),
    Vectors.sparse(3,Array(0,1,2),Array(3.0,5.0,9.0)),
    Vectors.sparse(3,Array(0,1,2),Array(4.0,7.0,9.0)),
    Vectors.sparse(3,Array(0,1,2),Array(2.0,4.0,9.0)),
    Vectors.sparse(3,Array(0,1,2),Array(9.0,5.0,7.0)),
    Vectors.sparse(3,Array(0,1,2),Array(2.0,5.0,9.0)),
    Vectors.sparse(3,Array(0,1,2),Array(3.0,4.0,7.0)),
    Vectors.sparse(3,Array(0,1,2),Array(8.0,5.0,7.0)),
    Vectors.sparse(3,Array(0,1,2),Array(3.0,6.0,2.0)),
    Vectors.sparse(3,Array(0,1,2),Array(5.0,9.0,2.0))
  )

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(data1.map(Tuple1.apply)).toDF("features")

  df.show()


  //Indicates whether to perform the inverse DCT (true) or forward DCT (false).
  //Default: false
  //具有大于5个不同值的特征被视为连续
  val vf = new VectorIndexer().setInputCol("features").setOutputCol("indexfeatures").setMaxCategories(5).fit(df)

  val res = vf.transform(df)
  res.show(false)
  /**
  +-------------------------+-------------------------+
  |features                 |indexfeatures            |
  +-------------------------+-------------------------+
  |(3,[0,1,2],[2.0,5.0,7.0])|(3,[0,1,2],[2.0,1.0,1.0])|
  |(3,[0,1,2],[3.0,5.0,9.0])|(3,[0,1,2],[3.0,1.0,2.0])|
  |(3,[0,1,2],[4.0,7.0,9.0])|(3,[0,1,2],[4.0,3.0,2.0])|
  |(3,[0,1,2],[2.0,4.0,9.0])|(3,[0,1,2],[2.0,0.0,2.0])|
  |(3,[0,1,2],[9.0,5.0,7.0])|(3,[0,1,2],[9.0,1.0,1.0])|
  |(3,[0,1,2],[2.0,5.0,9.0])|(3,[0,1,2],[2.0,1.0,2.0])|
  |(3,[0,1,2],[3.0,4.0,7.0])|(3,[0,1,2],[3.0,0.0,1.0])|
  |(3,[0,1,2],[8.0,5.0,7.0])|(3,[0,1,2],[8.0,1.0,1.0])|
  |(3,[0,1,2],[3.0,6.0,2.0])|(3,[0,1,2],[3.0,2.0,0.0])|
  |(3,[0,1,2],[5.0,9.0,2.0])|(3,[0,1,2],[5.0,4.0,0.0])|
  +-------------------------+-------------------------+ */

  /*
  结果分析：特征向量包含3个特征，即特征0，特征1，特征2。如Row=1,对应的特征分别是2.0,5.0,7.0.被转换为2.0,1.0,1.0。
  我们发现只有特征1，特征2被转换了，特征0没有被转换。这是因为特征0有6种取值（2，3，4，5，8，9），多于前面的设置setMaxCategories(5)
  ，因此被视为连续值了，不会被转换。
  特征1中，列向量（4，5，6，7，9）有5个数，被视为离散值，需要重新编码为-->(0,1,2,3,4)，所以5 的索引是 1
  特征2中, 列向量 (2,7,9)-->(0,1,2)所以7 的索引是 1

  特征1：
  5.0  -> 1.0
  5.0  -> 1.0
  7.0  -> 3.0
  4.0  -----
  5.0
  5.0
  4.0
  5.0
  6.0
  9.0
   */
}
