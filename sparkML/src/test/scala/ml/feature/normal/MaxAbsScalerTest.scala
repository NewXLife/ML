package ml.feature.normal

import org.apache.spark.ml.feature.MaxAbsScaler
import util.SparkTools

/**
  * 该方法是线性的变换，当某一维特征上具有非线性的分布时，还需要配合其它的特征预处理方法。
  * MaxAbsScaler将每一维的特征变换到[-1, 1]闭区间上
  * 通过除以每一维特征上的最大的绝对值，它不会平移整个分布，也不会破坏原来每一个特征向量的稀疏性。
  */
object MaxAbsScalerTest extends SparkTools{
  val maxabs = new MaxAbsScaler()
    .setInputCol("features")
    .setOutputCol("maxabsFeatures")

  val maxabsModel = maxabs.fit(df)

  val res = maxabsModel.transform(df)

  res.show(10, truncate = false)
  /**
    * +---+--------------+----------------+
    * |id |features      |maxabsFeatures  |
    * +---+--------------+----------------+
    * |0  |[1.0,0.5,-1.0]|[0.25,0.05,-0.5]|
    * |1  |[2.0,1.0,1.0] |[0.5,0.1,0.5]   |
    * |2  |[4.0,10.0,2.0]|[1.0,1.0,1.0]   |
    * +---+--------------+----------------+
    */

}
