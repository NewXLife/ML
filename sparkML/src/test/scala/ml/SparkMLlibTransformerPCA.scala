package ml

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 1.多重共线性--预测变量之间相互关联。多重共线性会导致解空间的不稳定，从而可能导致结果的不连贯。
  * 2.高维空间本身具有稀疏性。一维正态分布有68%的值落于正负标准差之间，而在十维空间上只有0.02%。
  * 3.过多的变量会妨碍查找规律的建立。
  * 4.仅在变量层面上分析可能会忽略变量之间的潜在联系。例如几个预测变量可能落入仅反映数据某一方面特征的一个组内。
  * PCA is a statistical procedure that uses an orthogonal transformation
  * to convert a set of observations of possibly correlated variables
  * into a set of values of linearly uncorrelated variables called principal components.
  * A PCA class trains a model to project vectors to a low-dimensional space using PCA
  */

/**
  * 降维的目的：
  * 1.减少预测变量的个数
  * 2.确保这些变量是相互独立的
  * 3.提供一个框架来解释结果
  * 降维的方法有：主成分分析、因子分析、用户自定义复合等。
  * PCA（Principal Component Analysis）不仅仅是对高维数据进行降维，更重要的是经过降维去除了噪声，发现了数据中的模式。
  * PCA把原先的n个特征用数目更少的m个特征取代，新特征是旧特征的线性组合，这些线性组合最大化样本方差，尽量使新的m个特征互不相关。
  * 从旧特征到新特征的映射捕获数据中的固有变异性。
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
