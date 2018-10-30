package ml.feature.normal

import org.apache.spark.ml.feature.MinMaxScaler
import util.SparkTools

/**
  * 该方法是线性的变换，当某一维特征上具有非线性的分布时，还需要配合其它的特征预处理方法。
  * MinMaxScaler作用同样是每一列，即每一维特征。将每一维特征线性地映射到指定的区间，通常是[0, 1]
  */
object MinMaxScalerTest extends SparkTools{
  val scaler = new MinMaxScaler()
    .setInputCol("features")
    .setOutputCol("minMaxFeatures")
    .setMin(0)
    .setMax(1)

  val scalerModel =  scaler.fit(df)

  val res = scalerModel.transform(df)

  res.show(10, truncate = false)
  // 每维特征线性地映射，最小值映射到0，最大值映射到1。
  /**
    * +---+--------------+-----------------------------------------------------------+
    * |id |features      |minMaxFeatures                                             |
    * +---+--------------+-----------------------------------------------------------+
    * |0  |[1.0,0.5,-1.0]|[0.0,0.0,0.0]                                              |
    * |1  |[2.0,1.0,1.0] |[0.3333333333333333,0.05263157894736842,0.6666666666666666]|
    * |2  |[4.0,10.0,2.0]|[1.0,1.0,1.0]                                              |
    * +---+--------------+-----------------------------------------------------------+
    */
}
