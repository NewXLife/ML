package com.niuniuzcd.demo.ml.transformer

import org.apache.spark.SparkException
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer

/**
  * 将连续型变量转化为离散型
  * 仅适用于cont， 支持缺失值
  * Parameters
  * ----------
  * diff_thr : int, default: 20
  * 不同取值数高于该值才进行离散化处理，不然原样返回
  * binning_method : str, default: 'dt', {'dt', 'qcut', 'cut'}
  * 分箱方法,:
  * 'dt' which uses decision tree.
  * 'cut' which cuts data by the equal intervals.
  * 'qcut' which cuts data by the equal quantity.
  * default is 'dt'. if y is None, default auto changes to 'qcut'.
  *
  * bins : int, default: 10
  * 分箱数目， 当binning_method='dt'时，该参数失效
  *
  * 决策树分箱方法使用的决策树参数
  */
class ContinueEncoder(diff_thr: Long = 20, bins: Int = 10, var binningMethod: String = "dt", inplace: Boolean = true) extends Serializable {
  var bucket: Array[Double] = _
  var innerDf: DataFrame = _
  var binCols: ArrayBuffer[String] = ArrayBuffer[String]()
  var noBinCols: ArrayBuffer[String] = ArrayBuffer[String]()
  var binsMap: scala.collection.mutable.Map[String, Array[Double]] = scala.collection.mutable.Map[String, Array[Double]]()
  private final val bc = new Bucketizer()

  def fit(df: DataFrame, y: String = ""): this.type = {
    if (y.isEmpty && binningMethod == "dt") binningMethod = "qcut" else innerDf = df.withColumnRenamed("label", y)

    // bigger than diff_thr need scatter,else keep it
    val cols = df.columns
    for (c <- cols) {
      if (df.select(c).distinct().count() > diff_thr) binCols +:= c else noBinCols +:= c
    }

    for (c <- binCols if binCols.nonEmpty) {
      val tmp = if (y.isEmpty) df.select(c).na.drop() else df.select(c, y).na.drop()
      binsMap += (c -> getCutPoints(tmp, c, binningMethod))
    }
    this
  }

  /**
    */
  def transform(df: DataFrame): DataFrame = {
    var tmp = df
    for (c <- binCols) {
      val bucketizer: UserDefinedFunction = udf { feature: Double =>
        binarySearchForBuckets(binsMap(c), feature, keepInvalid = true)
      }
      val newCol = bucketizer(tmp(c))
      tmp = if (inplace) tmp.withColumn(c + "_bin", newCol).drop(c) else tmp.withColumn(c + "_bin", newCol)
    }
    tmp
  }

  def spliterBins(interval: Array[Double]): Array[Double] = {
    val tem = if (interval.map(_ < 0).length >= 2) {
      var t = interval.filter(x => x > 0).toBuffer
      t +:= Double.NegativeInfinity
      t.toArray
    } else interval
    tem
  }

  def getCutPoints(df: DataFrame, colName: String, binningMethod: String = "dt"): Array[Double] = {
    binningMethod match {
      case "cut" => cut(df, colName)
      case "qcut" => qcut(df, colName)
    }

  }

  import java.{util => ju}
  def binarySearchForBuckets(
                              splits: Array[Double],
                              feature: Double,
                              keepInvalid: Boolean): Double = {
    if (feature.isNaN) {
      if (keepInvalid) {
        splits.length - 1
      } else {
        throw new SparkException("Bucketizer encountered NaN value. To handle or skip NaNs," +
          " try setting Bucketizer.handleInvalid.")
      }
    } else if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx - 1 // index -> 为二分找到到的索引号，修改索引之前
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new SparkException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

  /**
    * 等距离分箱,等同pandas 的cut
    * 桶必须是排好序的，并且不包含重复元素，至少有两个元素
    * 入参是一个数/或者是一个数组
    * 先用排好序的数组的边界值来得出两个桶之间的间距
    * 如果是一个数组 表示： [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
    * 返回一个元组， 分箱边界和分箱内的统计数目
    */

  def cut(df: DataFrame, colName: String) = {
    val (tempBucket, _) = df.select(colName).rdd.mapPartitions(par => par.map(_.getDouble(0))).histogram(bins)
    tempBucket
  }

  /**
    * 等频分箱，等同pandas 的pcut
    */
  def qcut(df: DataFrame, colName: String) = {
    val qd = new QuantileDiscretizer().setInputCol(colName).setOutputCol(colName + "_qIndex")
      .setNumBuckets(bins) //设置分箱数
      .setRelativeError(0) //设置precision-控制相对误差,设置为0时，将会计算精确的分位点（计算代价较高）。
      .setHandleInvalid("keep") //对于空值的处理 handleInvalid = ‘keep’时，可以将空值单独分到一箱,  "skip" 不要空值
      .fit(df)

    spliterBins(qd.getSplits)
  }

  /**
    * 决策树分箱, 该决策树和python的sk-learn 出的结果不一致，需要再研究
    * 训练决策树模型，单特征分箱
    * 决策树需要label列
    */
  def dt = {
    val dt = new DecisionTreeClassifier().setLabelCol("d14")
      .setFeaturesCol("feature")
      .setImpurity("gini") // gini系数 or entropy [ˈentrəpi](熵)
      //    .setMaxBins(100) //离散化"连续特征"的最大划分数
      .setMaxDepth(4) //树的最大深度
      //    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(1) //每个节点包含的最小样本数
      .setSeed(7)

    //    df.printSchema()

    val sm = new VectorAssembler()
      .setInputCols(Array("day7"))
      .setOutputCol("feature")

    //    val data = sm.transform(df)
    //    data.show(5, truncate = 0)

    //input fit must be vector
    //    val model = dt.fit(data)

    //  val mp = """\d|\d+\.\d+""".r
    //  val res = mp.findAllMatchIn(model.toDebugString).toArray
    //  res.foreach(println(_))

    //    println(model.toDebugString)
    //    println(model.featureImportances)
    //    println(model.numClasses)
    //    println(model.numFeatures)
    //    println(model.numNodes)
    //    println(model.rootNode.impurity)
  }
}
