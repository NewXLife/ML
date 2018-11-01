package ml

import org.apache.spark.ml.feature.Bucketizer
import util.SparkTools

/**
  * 分箱（分段处理）：将连续数值转换为离散类别
  * 比如特征是年龄，是一个连续数值，需要将其转换为离散类别(未成年人、青年人、中年人、老年人），就要用到Bucketizer了。
  * 分类的标准是自己定义的，在Spark中为split参数,定义如下：
  * double[] splits = {0, 18, 35,50， Double.PositiveInfinity}
  * 将数值年龄分为四类0-18，18-35，35-50，55+四个段。
  * 如果左右边界拿不准，就设置为，Double.NegativeInfinity， Double.PositiveInfinity，不会有错的
  */
object SparkMLlibTransformerBucketizer extends SparkTools{
  // [-inf, 0.5, 2.5, 3.5, 4.5, 5.5, inf]
  val splits = Array[Double](Double.NegativeInfinity,0.5, 2.5, 3.5, 4.5, 5.5, Double.PositiveInfinity)
  val bucketizer = new Bucketizer().setInputCol("day7").setOutputCol("7day_new").setSplits(splits)
  val res = bucketizer.transform(baseDf)

  res.show()
}
