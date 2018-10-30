package ml.feature.normal

import org.apache.spark.ml.feature.QuantileDiscretizer
import util.SparkTools

object BinningTest extends SparkTools{
  baseDf.show(5, truncate = false)

  //等频分箱
//  val qd = new QuantileDiscretizer().setInputCol("m1").setOutputCol("q7day")
//    .setNumBuckets(10)      //设置分箱数
//    .setRelativeError(0) //设置precision-控制相对误差,设置为0时，将会计算精确的分位点（计算代价较高）。
//    .setHandleInvalid("keep") //对于空值的处理 handleInvalid = ‘keep’时，可以将空值单独分到一箱,  "skip" 不要空值
//    .fit(baseDf)
//
//  qd.getSplits.foreach(println(_))//分箱区别
//
//  qd.transform(baseDf).show() //左闭，右开区间，离散化数据
//
//  spark.stop()

  //等距离分箱
  val a = spark.sparkContext.parallelize(List(1.1,1.2,1.3,2.0,2.1,7.4,7.5,7.6,8.8,9.0),3)
  val (bucket, ct )  = a.histogram(5)
  bucket.foreach(println(_))


  //决策树分箱

}
