package sta

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tree._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DTBinsTest extends StaFlow with App {
  import spark.implicits._
  import org.apache.spark.sql.functions._
  val label = "d14"
  var testDf = loadCSVData("csv", "C:\\NewX\\newX\\ML\\docs\\testData\\base3.csv").withColumnRenamed(label, "label").withColumn("label", $"label".cast("int"))
  testDf.show()


  /**
    * +-----+----+----+----+----+----+----+----+----+---+---------+
    * |label|day7|  m1|  m3|  m6| m12| m18| m24| m60|age|       ad|
    * +-----+----+----+----+----+----+----+----+----+---+---------+
    * |    0|-1.0| 2.0| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/19|
    * |    0|-1.0| 2.0| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/20|
    */

  /**
    * use dt-tree all feature must be number
    */
  //dt统计特征数组
  val features = Array("m1","m3", "m6")

  val row2ColsDf = StaFlow.row2ColDf(testDf, features, label)
    row2ColsDf.show(200)

  val rescc = row2ColsDf.groupBy("key_field_name").agg(countDistinct("value").as("f_num"))
  rescc.show()
  val rrr = rescc.filter($"f_num" >1)
  rrr.show()
  import org.apache.spark.sql.functions._
  import spark.implicits._
  val finalF = rescc.filter($"f_num" >1).groupBy("f_num").pivot("key_field_name").sum("f_num").schema.fieldNames
  println(finalF.filter(x => !x.equals("f_num")).mkString(","))

  val ab = ArrayBuffer[String]()
  val uu = rescc.filter($"f_num" >1).select("key_field_name").collect().map(x => x.getAs[String](0)).toArray
  println("----------:", uu.getClass.getSimpleName)
//  rescc.filter($"f_num" >1).map( x => ab += x.getAs[String](0))

  println(ab.mkString(","))


  println(rrr.schema.fields.mkString(","))
  //  val features = Array("m1", "m60")
  /**
    * +-----+----+---+----+----+----+----+----+----+---+---------+--------+
    * |label|day7| m1|  m3|  m6| m12| m18| m24| m60|age|       ad|features|
    * +-----+----+---+----+----+----+----+----+----+---+---------+--------+
    * |    0|-1.0|  2| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/19|   [0.0]|
    * |    0|-1.0|  2| 6.0|13.0|42.0|48.0|54.0|  大学| 10|2018/6/20|   [0.0]|
    */
  // 训练决策树模型
  val dt = new DecisionTreeClassifier().setLabelCol("label")
    .setFeaturesCol("features")
        .setImpurity("entropy") //
//    .setImpurity("gini") //
//        .setMaxBins(10) //离散化"连续特征"的最大划分数
    .setMaxDepth(4) //树的最大深度

    //如果增益小于该值将不会分裂
    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
//        .setMinInstancesPerNode(10) //每个节点包含的最小样本数
    .setSeed(7)

  val vectorAssembler = new VectorAssembler()
  // Index labels, adding metadata to the label column.
  // Fit on whole dataset to include all labels in index.


  def staDT(staCols: Array[String],df: DataFrame) = {
//    var binsMap: mutable.Map[String, Array[Double]] = mutable.Map()
    var binsMap: mutable.Map[String,String] = mutable.Map()
    for (f <- staCols) {
      //特征包装为向量,过滤空值
      val staDF = df.withColumn(f, $"$f".cast("double")).where($"$f".notEqual(Double.NaN))
      println(s"$f------------staDF")
      staDF.show()
      //vectorAssembler 输入的空特征需要处理
      val singleDf =vectorAssembler.setInputCols(Array(f)).setOutputCol("features").transform(staDF)
      println(" --------------singleDF")
      singleDf.show()
      val model = dt.fit(singleDf.repartition(100))
//      println("--------------------bins:", DTUtils.extractConBins(model).mkString(","))
      binsMap += (f -> model.toDebugString)
//      binsMap += (f -> DTUtils.extractConBins(model))
  }
    binsMap
  }

//  println("---------------------------res----------------------")
  val res = staDT(features, testDf)
  println(res.keySet)
  println(res.mkString(","))
//
//  val bisMap = Array(Double.NegativeInfinity,12.0,25.0,Double.PositiveInfinity)
// val inx =  searchIndex22(2.0, bisMap)
//  println(inx)
//  println( "(" + bisMap(inx - 1) + "," + bisMap(inx) + ")")
//
//  def searchIndex22(v2: Double, array: Array[Double]): Int = {
//    var temp = 0
//    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
//    temp
//  }
}
