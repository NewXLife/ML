package ml.feature.normal

import org.apache.spark.ml.feature.Normalizer
import util.SparkTools

/**
  * 归一化处理，将所有数据映射到统一尺度
  * * 该方法是线性的变换，当某一维特征上具有非线性的分布时，还需要配合其它的特征预处理方法。
  * (normalization)适用于分布有明显边界的情况； 最值归一化的缺点：受outlier影响较大
  */
object NormalizerTest extends SparkTools {
  spark.sparkContext.setLogLevel("ERROR")
//  baseDf.show(5, truncate = false)
  /**
    * 若为L1时，样本各个特征值除以各个特征值的绝对值之和
    * 若为L2时，样本各个特征值除以各个特征值的平方之和
    * 若为max时，样本各个特征值除以样本中特征值最大的值
    */
  val normal = new Normalizer()
    .setInputCol("features")
    .setOutputCol("normalFeature")
    .setP(1.0) //正则化每个向量到1阶范数, 1阶范数即所有值绝对值之和。

  val res = normal.transform(df)
  res.show(10, truncate = false)
  /**
    * +---+--------------+--------------+
    * |id |features      |normalFeature |
    * +---+--------------+--------------+
    * |0  |[1.0,0.5,-1.0]|[1.0,0.5,-1.0]|
    * |1  |[2.0,1.0,1.0] |[1.0,0.5,0.5] |
    * |2  |[4.0,10.0,2.0]|[0.4,1.0,0.2] |
    * +---+--------------+--------------+
    */

  //正则化每个向量到无穷阶范数,即max,样本各个特征值除以样本中特征值最大的值
  val res1 = normal.transform(df, normal.p -> Double.PositiveInfinity)
  res1.show(10, truncate =  false)
  /**
    * +---+--------------+------------------+
    * |id |features      |normalFeature     |
    * +---+--------------+------------------+
    * |0  |[1.0,0.5,-1.0]|[0.4,0.2,-0.4]    |
    * |1  |[2.0,1.0,1.0] |[0.5,0.25,0.25]   |
    * |2  |[4.0,10.0,2.0]|[0.25,0.625,0.125]|
    * +---+--------------+------------------+
    */
}
