package sta

import com.google.gson.Gson
import com.niuniuzcd.demo.util.DSHandler

import scala.collection.mutable.ArrayBuffer

object CrossFeatureAnalysisTest extends App {
  val spark = StaFlow.spark

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val test = StaFlow.loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\base3.csv")
  //  val test = StaFlow.loadCSVData("csv", "file:\\D:\\NewX\\ML\\docs\\testData\\base3.csv").orderBy("ad")
  test.show(10, truncate = 0)

  /**
    * feature cross
    * f1: "-Infinity,14.0,29.0,44.0,59.0,Infinity"
    * f2: "-Infinity,14.0,29.0,44.0,59.0,Infinity"
    * f3: "(初中), (初中,大学), (大学,高中)"
    * f4: "binThread:3"
    * f5: categories: "f1"
    * f6: continue:"f2"
    */

  case class CrossFeature(name: String, isUseTemplate: Boolean, threshold: Int, t: Map[String, Array[String]])

  //  case class TestFeatures(name: String, threshold: Int, )
  import scala.collection.JavaConversions._
  val labelCol = "d14"

  val crossFeatureList:List[CrossAnalysisModel] = Map2Json.getCrossList.toList

  val featureCols = ArrayBuffer[String]()

  //cross features buffer
  val featureBuffer = ArrayBuffer[CrossFeature]()

  for (obj <- crossFeatureList) {
    val featureName = obj.getFeatureName

    var threshold = 0
    if(obj.getBinningThreshold != null)
    threshold = obj.getBinningThreshold.toInt

    val t = obj.getBinsTemplate.mapValues(a => a.map(x=>x).toArray).toMap
    if (threshold > 0) {
      val disCount = test.select(featureName).distinct().count()
      if (disCount > threshold) {
        throw new CrossAnaException(s"cross analysis failed, binThread = $threshold, but records discount= $disCount")
      }
      featureBuffer += CrossFeature(featureName, isUseTemplate = false, threshold, t)
    } else {
      featureBuffer += CrossFeature(featureName, isUseTemplate = true, threshold, t)
    }
    featureCols += featureName
  }

  val (featureName1, featureName2) = (featureCols(0), featureCols(1))

  val featureBinNameArray = featureCols.map(name => name + "_bin").toArray

  require(featureCols.nonEmpty ,"feature length must bigger than zero,please checked.")
  var tempDF = test.select(labelCol, featureCols.toSet.toArray: _*)

  println("tempDF------------------")
  tempDF.show(10, truncate = 0)
  /**
    * tempDF------------------
    * +---+---+----+
    * |d14|m60|m1  |
    * +---+---+----+
    * |0  |大学 |2.0 |
    * |0  |大学 |2.0 |
    * |0  |初中 |2.0 |
    * |0  |初中 |5.0 |
    * |0  |初中 |5.0 |
    * |0  |初中 |5.0 |
    * |1  |大学 |10.0|
    * |1  |大学 |10.0|
    * |1  |大学 |10.0|
    * |0  |博士 |16.0|
    * +---+---+----+
    */

  if (featureBuffer.nonEmpty) {
    for (obj <- featureBuffer) {
      if (obj.isUseTemplate)
      //模板需要用， 需要指定那些特征用的模板
        tempDF = tempDF.withColumn(obj.name + "_bin", lit(obj.t(obj.name)))
      else
        tempDF = tempDF.withColumn(obj.name + "_bin", udf { str: String => {
          str
        }
        }.apply(col(obj.name)))
    }
  } else {
    throw new CrossFeaturesEmptyException(s"cross analysis failed, input features is empty, featureSize=${featureBuffer.length}")
  }

  println("tempDF before--------")
  tempDF.show(10, truncate = 0)
  /**
    * +---+---+----+-----------------+---------------+
    * |d14|m60|m1  |m60_bin          |m1_bin         |
    * +---+---+----+-----------------+---------------+
    * |0  |大学 |2.0 |[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |0  |大学 |2.0 |[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |0  |初中 |2.0 |[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |0  |初中 |5.0 |[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |0  |初中 |5.0 |[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |0  |初中 |5.0 |[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |1  |大学 |10.0|[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |1  |大学 |10.0|[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |1  |大学 |10.0|[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    * |0  |博士 |16.0|[(小学), 初中, 大学,博士]|[(2,5], (5,11]]|
    */

  //模板需要计算具体分箱
  //val combine3 = combine2.withColumn("f1_bin", StaFlow.splitCrossSubBin($"m24", $"f1_bin"))

  for (obj <- featureBuffer) {
    if (obj.isUseTemplate)
    //模板需要用， 需要指定那些特征用的模板
      tempDF = tempDF.withColumn(obj.name + "_bin", StaFlow.splitCrossSubBin($"${obj.name}", $"${obj.name + "_bin"}"))
  }

  println("tempDF---new------------")
  tempDF.show(10, truncate = 0)
  /**
    * +---+---+----+----------+----------------------+
    * |d14|m60|m1  |m60_bin   |m1_bin                |
    * +---+---+----+----------+----------------------+
    * |0  |大学 |2.0 |[3, 大学,博士]|[1, (2,5]]            |
    * |0  |大学 |2.0 |[3, 大学,博士]|[1, (2,5]]            |
    * |0  |初中 |2.0 |[2, 初中]   |[1, (2,5]]            |
    * |0  |初中 |5.0 |[2, 初中]   |[2, (5,11]]           |
    */

  val row2ColDF = StaFlow.row2ColCrossDf(tempDF, featureCols.toArray, labelCol)
  println("row2ColDF--------------")
  row2ColDF.show(10, truncate = 0)
  /**
    * +-----+----------+-----------+--------------+-----+
    * |label|m60_bin   |m1_bin     |key_field_name|value|
    * +-----+----------+-----------+--------------+-----+
    * |0    |[3, 大学,博士]|[1, (2,5]] |m60           |大学   |
    * |0    |[3, 大学,博士]|[1, (2,5]] |m1            |2.0  |
    * |0    |[3, 大学,博士]|[1, (2,5]] |m60           |大学   |
    * |0    |[3, 大学,博士]|[1, (2,5]] |m1            |2.0  |
    */

  val groupByCols = featureCols.map(name => row2ColDF(name + "_bin"))

  val binDF = StaFlow.binsIndexCrossDF(row2ColDF, groupByCols.toArray)
  println("binDF-----------------")
  binDF.show(10, truncate = 0)
  /**
    * +----------+----------------------+----------+------------+---------------+-------------------+
    * |m60_bin   |m1_bin                |binSamples|overdueCount|notOverdueCount|overdueCountPercent|
    * +----------+----------------------+----------+------------+---------------+-------------------+
    * |[3, 大学,博士]|[1, (2,5]]            |4         |0           |4              |0.0                |
    * |[2, 初中]   |[1, (2,5]]            |2         |0           |2              |0.0                |
    * |[3, 大学,博士]|[-99, (missing-value)]|14        |6           |8              |0.42857142857142855|
    * |[2, 初中]   |[2, (5,11]]           |6         |0           |6              |0.0                |
    * +----------+----------------------+----------+------------+---------------+-------------------+
    */

  val masterDF = StaFlow.totalIndexCross(row2ColDF)
  println("masterDF-----------------")
  masterDF.show(10, truncate = 0)
  /**
    * masterDF-----------------
    * +--------------+------------+------------+---------------+-------------------+
    * |key_field_name|totalSamples|totalOverdue|totalNotOverdue|totalOverduePercent|
    * +--------------+------------+------------+---------------+-------------------+
    * |m60           |13          |3           |10             |0.3                |
    * |m1            |13          |3           |10             |0.3                |
    * +--------------+------------+------------+---------------+-------------------+
    */

  val resDF = StaFlow.binsDFJoinMasterDFCross(binDF, masterDF, featureBinNameArray)

  /**
    * +----------+----------------------+----------+------------+---------------+-------------------+
    * |m60_bin   |m1_bin                |binSamples|overdueCount|notOverdueCount|overdueCountPercent|
    * +----------+----------------------+----------+------------+---------------+-------------------+
    * |[3, 大学,博士]|[1, (2,5]]            |4         |0           |4              |0.0                |
    * |[2, 初中]   |[1, (2,5]]            |2         |0           |2              |0.0                |
    * |[3, 大学,博士]|[-99, (missing-value)]|14        |6           |8              |0.42857142857142855|
    * |[2, 初中]   |[2, (5,11]]           |6         |0           |6              |0.0                |
    * +----------+----------------------+----------+------------+---------------+-------------------+
    */

  /**
    * combine f1_bin and f2_bin
    * cross_bin
    */
  val map = new java.util.HashMap[String, Object]()
  //  map.put("abc", List(Bin(0, "(0,1]"),Bin(0,"(1,2]")).toArray)
  val gson = new Gson()
  //  println( gson.toJson(map) )

  case class Bin(index:Int, bin:String)
  val finalDF = resDF.withColumn("cross_bin", udf{(x:Seq[String], y:Seq[String]) =>{
    map.put(featureName1, Bin(x.head.toInt, x.last))
    map.put(featureName2, Bin(y.head.toInt, y.last))
    //    var map = Map("f1" -> Bin(0, x), "f2"->Bin(0,y))
    gson.toJson(map)
  }}.apply($"${featureName1+"_bin"}", $"${featureName2+"_bin"}")).drop($"${featureName1+"_bin"}").drop($"${featureName2+"_bin"}")
  finalDF.show(10, truncate = 0)
  /**
    * +----------+------------+---------------+-------------------+----------------------------------------------------------------------------+
    * |binSamples|overdueCount|notOverdueCount|overdueCountPercent|cross_bin                                                                   |
    * +----------+------------+---------------+-------------------+----------------------------------------------------------------------------+
    * |4         |0           |4              |0.0                |{"m1":{"index":1,"bin":"(2,5]"},"m60":{"index":3,"bin":"大学,博士"}}            |
    * |2         |0           |2              |0.0                |{"m1":{"index":1,"bin":"(2,5]"},"m60":{"index":2,"bin":"初中"}}               |
    * |14        |6           |8              |0.42857142857142855|{"m1":{"index":-99,"bin":"(missing-value)"},"m60":{"index":3,"bin":"大学,博士"}}|
    * |6         |0           |6              |0.0                |{"m1":{"index":2,"bin":"(5,11]"},"m60":{"index":2,"bin":"初中"}}              |
    * +----------+------------+---------------+-------------------+----------------------------------------------------------------------------+
    */
  DSHandler.save2MysqlDb(finalDF.withColumn("statistic_id", lit(3)).withColumn("statistic_uuid", lit("abc")), "dataset_statistic_bins_cross")
}

class CrossFeaturesEmptyException(message: String, cause: Throwable)
  extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}

class CrossAnaException(message: String, cause: Throwable)
  extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}
