import util.SparkTools

import scala.collection.mutable.ArrayBuffer

object TestKS2 extends SparkTools {
  val base1 = loadCSVData("csv", "E:\\NewX\\newX\\bd-engine\\docs\\20180814_old_tdbase.csv")
//  base1.show(10)
  println("base count number:", base1.count())

  val pre = loadCSVData("csv", "E:\\NewX\\newX\\bd-engine\\docs\\20180907_old_td.csv")
//  pre.show(10)

  val baseFeatureList = ArrayBuffer("d14","apply_date","7day","1month","3month","6month","12month","18month","24month","60month")
  //remove label
  baseFeatureList.remove(baseFeatureList.indexOf("d14"))
  //remove time column
  baseFeatureList.remove(baseFeatureList.indexOf("apply_date"))
  //KSIndex(stId, dsId, labelItem.getName, baseDf, baseFeatureList, "statistic_ks_base_index")
//  StatisticCenter.apply.KSIndex(1,2,"d14", base1, baseFeatureList.toArray, "statistic_ks_base_index")
//  StatisticCenter.apply.KSIndex(1,2,"d7", pre, baseFeatureList.toArray, "statistic_ks_index")

  //ds_id: Long, base_id: Long, st_id: Long, df: DataFrame, baseDf: DataFrame, cols: Array[String]
//  StatisticCenter.apply.PSI(1,2,11, pre,base1, baseFeatureList.toArray)


  // //input must double data type
  //  val qd = new QuantileDiscretizer().setInputCol("age").setOutputCol("gage")
  //    .setNumBuckets(3)      //设置分箱数
  //    .setRelativeError(0) //设置precision-控制相对误差,设置为0时，将会计算精确的分位点（计算代价较高）。
  //    .fit(df)
  //
  //  qd.getSplits.foreach(println(_))//分箱区别
  //
  //  qd.transform(df).show() //左闭，右开区间，离散化数据

  //input must double data type
//  val baseDf = base1.selectExpr("cast(1month as double)", "cast(d14 as double)")
  val baseDf = base1.selectExpr("cast(1month as double)")
//  val testDf = pre.selectExpr("cast(1month as double)", "cast(d7 as double)")
  val testDf = pre.selectExpr("cast(1month as double)")

  //ds_id: Long, base_id: Long, st_id: Long, df: DataFrame, baseDf: DataFrame, cols: Array[String]
//  StatisticCenter.apply.PSI(11,11,111, pre, base1, baseFeatureList.toArray )

  //base

//  val qd = new QuantileDiscretizer().setInputCol("1month").setOutputCol("new")
//    .setNumBuckets(10)      //设置分箱数
//    .setRelativeError(0) //设置precision-控制相对误差,设置为0时，将会计算精确的分位点（计算代价较高）。
//    .setHandleInvalid("skip")
//    .fit(baseDf)
//
////  qd.getSplits.foreach(println(_))//
//  println(qd.getSplits)
//  val interval = qd.getSplits
//  val ttt = interval.map(_ > 0)

/**
  var t = interval.filter(x => x >0).toBuffer
  t +:=  Double.NegativeInfinity
  t
  */

//  //transform 数据分类
//  qd.transform(baseDf).show() //左闭，右开区间，离散化数据
//
//  val tcount = baseDf.count()
//  println("totalcount:", tcount)
  //获取每个分箱的数据
  //直方图的取值边界不一样
//  val baseCouts = baseDf.rdd.mapPartitions(par => par.map(v => v.getDouble(0))).histogram(interval)
//  val testCouts = testDf.rdd.mapPartitions(par => par.map(v => v.getDouble(0))).histogram(interval)

  //df:DataFrame, col:String, bins:Array[(Double, Double)


//  val bins_temp = ContinueEncoder.apply.getBinsArray(interval)


//  spark.createDataFrame(StatisticCenter.apply.binsCount(baseDf, "1month", bins_temp, "d14")).show()

//  println("base----------counts")
//  baseCouts.foreach(println(_))
//
//  println("test-------counts")
//  testCouts.foreach(println(_))
//
//  //求分箱中的占比
//  val basePer = baseCouts.map(x => x.doubleValue() / tcount)
//  println("base -------------per")
//  basePer.foreach(println(_))
//
//  println("test------------per")
//  val testPer = testCouts.map(x => x.doubleValue() / tcount)
//  testPer.foreach(println(_))
//
////  basePer.foreach(println(_))
//  val psi = testPer.zip(basePer).map(x =>  (x._1 - x._2) * Math.log(x._1 / x._2)).sum

//  println(psi)

  //val psi = predPrecent.zip(basePrecent).map(x => if (x._1 == x._2) 0 else (x._1 - x._2) * Math.log(x._1 / x._2)).sum

  //val predCounts = predictSelectedCol.rdd.mapPartitions(par => par.map(v => v.getDouble(0))).histogram(baseStartValues)

}
