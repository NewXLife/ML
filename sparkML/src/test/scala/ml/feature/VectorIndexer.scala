package ml.feature

import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, VectorSlicer}
import org.apache.spark.sql.types.DoubleType
import util.SparkTools

/**
  * 自动识别分类特征，并进行索引
  */
object VectorIndexer extends SparkTools {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  //获取dataframe
  val testDf = baseDf

  val fildsCos = testDf.columns.toBuffer

  fildsCos.remove(fildsCos.indexOf("ad"))

  //创建向量装配器
  val va = new VectorAssembler()
    .setInputCols(fildsCos.toArray) //那些字段需要放入特征向量中，参数是一个数组
    .setOutputCol("features") //输出的名称

  //向量装配器 transform 转换类型需要 数字类型
  val vcdf = testDf.select($"ad" +: fildsCos.map(f => col(f).cast(DoubleType)): _*)

  //转换
  val vDf = va.transform(vcdf)

  //display limit 5 and not truncate
  vDf.show(5, truncate = false)
  /**
    * +----------+---+----+----+----+----+----+----+----+----+---------------------------------------------+
    * |ad        |d14|day7|m1  |m3  |m6  |m12 |m18 |m24 |m60 |features                                     |
    * +----------+---+----+----+----+----+----+----+----+----+---------------------------------------------+
    * |2018-06-24|0.0|-1.0|2.0 |6.0 |13.0|42.0|48.0|54.0|54.0|[0.0,-1.0,2.0,6.0,13.0,42.0,48.0,54.0,54.0]  |
    * |2018-06-24|0.0|4.0 |5.0 |12.0|21.0|67.0|73.0|80.0|80.0|[0.0,4.0,5.0,12.0,21.0,67.0,73.0,80.0,80.0]  |
    * |2018-06-24|0.0|3.0 |10.0|25.0|36.0|66.0|68.0|68.0|68.0|[0.0,3.0,10.0,25.0,36.0,66.0,68.0,68.0,68.0] |
    * |2018-06-24|0.0|-1.0|16.0|33.0|33.0|33.0|33.0|35.0|35.0|[0.0,-1.0,16.0,33.0,33.0,33.0,33.0,35.0,35.0]|
    * |2018-06-24|0.0|-1.0|2.0 |7.0 |30.0|33.0|36.0|36.0|36.0|[0.0,-1.0,2.0,7.0,30.0,33.0,36.0,36.0,36.0]  |
    * +----------+---+----+----+----+----+----+----+----+----+---------------------------------------------+
    */

  //  val str = Array("ad", "d14", "m1", "m3", "m6", "m12", "m18", "m24", "m60")
  /** 主要作用：提高决策树或随机森林等ML方法的分类效果。
    * VectorIndexer是对数据集特征向量中的类别（离散值）特征（index categorical features categorical features ）进行编号。
    * 它能够自动判断那些特征是离散值型的特征，并对他们进行编号，具体做法是通过设置一个maxCategories，
    * 特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。
    * 某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）
    */
  val featureIndexer = new VectorIndexer()
    .setInputCol("features") // 输入特征向量名称
    .setOutputCol("indexFeatures") // 输出特征向量名称
    .setMaxCategories(50)
    .fit(vDf)

  val categoricalFeature = featureIndexer.categoryMaps.keys.toSet
  println(s"get categorical ${categoricalFeature.size} features:", categoricalFeature.mkString(","))  // 0, 1 表示d14|day7 是离散特征，进行重新编码

  val indexDate = featureIndexer.transform(vDf)
  indexDate.show(10, truncate = false)

  /**
    * VectorSlicer是一个切片工具，和VectorAssembler相对应，它的作用就是将一个向量切片，选择向量的子集。它提供了两种切片方式
    * setIndices(), 按照向量的下标索引切片（从0开始）
    * setNames()，按照列名切片
    * slicer.setIndices(Array(1)).setNames(Array("f3"))
    */
  val slicer = new VectorSlicer()
    .setInputCol("indexFeatures")
    .setOutputCol("slicerFeatures")

  //设置需要切割的向量索引或者是特征名称
  slicer.setIndices(Array(1)) //|d14|day7|m1  |m3  |m6  |m12 |m18 |m24 |m60    表示特征数组中的 ‘day7’
//  slicer.setNames(Array("7day", "m1"))

  val out = slicer.transform(indexDate)
  out.select("indexFeatures", "slicerFeatures", "day7").limit(10).show(10, truncate = false)

  /**
    * +--------------------------------------------+--------------+----+
    * |indexFeatures                               |slicerFeatures|day7|
    * +--------------------------------------------+--------------+----+
    * |[0.0,0.0,2.0,6.0,13.0,42.0,48.0,54.0,54.0]  |[0.0]         |-1.0|
    * |[0.0,3.0,5.0,12.0,21.0,67.0,73.0,80.0,80.0] |[3.0]         |4.0 |
    * |[0.0,2.0,10.0,25.0,36.0,66.0,68.0,68.0,68.0]|[2.0]         |3.0 |
    * |[0.0,0.0,16.0,33.0,33.0,33.0,33.0,35.0,35.0]|[0.0]         |-1.0|
    * |[0.0,0.0,2.0,7.0,30.0,33.0,36.0,36.0,36.0]  |[0.0]         |-1.0|
    * |[0.0,2.0,7.0,10.0,24.0,36.0,41.0,41.0,41.0] |[2.0]         |3.0 |
    * |[0.0,2.0,5.0,25.0,28.0,35.0,35.0,35.0,35.0] |[2.0]         |3.0 |
    * |[0.0,4.0,20.0,32.0,39.0,40.0,40.0,40.0,40.0]|[4.0]         |5.0 |
    * |[0.0,4.0,15.0,19.0,30.0,54.0,89.0,93.0,97.0]|[4.0]         |5.0 |
    * |[0.0,0.0,6.0,19.0,37.0,52.0,64.0,65.0,65.0] |[0.0]         |-1.0|
    * +--------------------------------------------+--------------+----+
    */

}
