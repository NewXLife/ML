package ml

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/**
  * 分箱（分段处理）：将连续数值转换为离散类别
  * 比如特征是年龄，是一个连续数值，需要将其转换为离散类别(未成年人、青年人、中年人、老年人），就要用到Bucketizer了。
  * 分类的标准是自己定义的，在Spark中为split参数,定义如下：
  * double[] splits = {0, 18, 35,50， Double.PositiveInfinity}
  * 将数值年龄分为四类0-18，18-35，35-50，55+四个段。
  * 如果左右边界拿不准，就设置为，Double.NegativeInfinity， Double.PositiveInfinity，不会有错的
  */
object SparkMLlibTransformerBucketizer extends App{
  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(Seq(
    (0, 30.0),
    (1, 40.0),
    (2, 10.0),
    (3, 50.0),
    (4, 60.0),
    (5, 70.0),
    (6, 80.0),
    (7, 77.0)
  )).toDF("id","age")


  df.show()

  /*
   0-20):0
   [20-40):1
   [40-60):2
   [60-70):3
   [70+ ):4
   */
  val splits = Array[Double](Double.NegativeInfinity,20, 40, 60, 70, Double.PositiveInfinity)

  val bucketizer = new Bucketizer().setInputCol("age").setOutputCol("age_seg").setSplits(splits)

  val res = bucketizer.transform(df)

  res.show()
}
