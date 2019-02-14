package sta

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tree.TreeUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{callUDF, col, lit, udf}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BinningMethod {
  /**
    * 分箱方法,:
    * 'dt' which uses decision tree.
    * 'cut' which cuts data by the equal intervals.
    * 'qcut' which cuts data by the equal quantity.
    * default is 'dt'. if y is None, default auto changes to 'qcut'.
    */
  def innerCut(min:Double, max:Double, bins:Int): mutable.Buffer[Double] ={
    var cut_points = cutMain(min, max, bins)
    cut_points = Double.NegativeInfinity +: cut_points.drop(1).dropRight(1)
    cut_points = cut_points :+ Double.PositiveInfinity
    cut_points
  }

  def cutMain(min:Double, max:Double, bins:Int): mutable.Buffer[Double] = {
    val bin = bins + 1
    val right = true
    var mn = min
    var mx = max
    if (mn == mx) {
      mn = mn - (if (mn != 0).001 * math.abs(mn) else .001)
      mx = mx + (if (mx != 0).001 * math.abs(mx) else .001)
      //      bins = linspace(mn, mx, cut_num + 1)
      linspace(mn, mx, bin).toBuffer
    } else {
      val bins = linspace(mn, mx, bin).toBuffer
      val adj = (mx - mn) * 0.001
      if (right) bins(0) -= adj else bins(bins.length - 1) += adj
      bins
    }
  }

  def linspace(start: Double, stop: Double, num: Int = 10, endpoint: Boolean = true) = {
    require(num > 0, "Number of samples, %s, must be non-negative.")

    val div = if (endpoint) num - 1 else num

    val delta = stop * 1.0 - start * 1.0
    val d = delta / div

    val ab = ArrayBuffer[Double]()
    var cut_point = start
    for (_ <- 0 to div) {
      ab += cut_point
      cut_point = cut_point + d
    }
    ab
  }

  def qcut(df: DataFrame, binsNum:Int = 10):DataFrame = {
    import df.sparkSession.implicits._
    val tempDf = df.groupBy("feature").agg(
      callUDF("percentile_approx", $"value", lit((0.0 to 1.0 by 1.0/binsNum).toArray)).as("bins")
    )

    val binsArrayDF = tempDf.withColumn("bins", udf{ splits:Seq[Double] =>
      if(splits != null){
        var buffer = splits.toBuffer
        buffer(0) =  Double.NegativeInfinity
        buffer(buffer.length - 1) = Double.PositiveInfinity
        buffer =  Double.NegativeInfinity +: buffer.filter(_ >0)
        buffer.distinct.toArray
      }else{
        Array(Double.NegativeInfinity, Double.PositiveInfinity)
      }
    }.apply(col("bins")))
    binsArrayDF
  }

  def cut(df: DataFrame, binsNum:Int = 10):DataFrame = {
    import df.sparkSession.implicits._
    val tempDf = df.groupBy("feature").agg(
      callUDF("concat_ws", lit(","), callUDF("collect_set", $"value".cast(StringType))).as("bins")
    )

    val binsArrayDF = tempDf.coalesce(200).withColumn("bins", udf{ inputStr:String => {
      val doubleArray = inputStr.split(",").map( x=> x.toDouble)
      innerCut(doubleArray.min,doubleArray.max, binsNum)
    }}.apply(col("bins")))
    binsArrayDF
  }

  def useBinsTemplate(df: DataFrame, binsArray: Map[String, Array[Double]], newCol:String = "bins", applyCol:String = "feature") = {
    df.withColumn(newCol, udf{f:String=>
      binsArray.filter{case(key, _) => key.equals(f)}.map{case(_, v)=> v}.toSeq.flatten.toArray
    }.apply(col(applyCol)))
  }

  /**
    * init DecisionTreeClassifier
    *
    * @param impurityPra
    * @param maxDepth
    * @param minInfoGain
    * @return
    */
  def initDT(impurityPra: String, maxDepth: Int, minInfoGain: Double): DecisionTreeClassifier = {
    val dt = new DecisionTreeClassifier().setLabelCol("label")
      .setFeaturesCol("features")
      .setImpurity(impurityPra)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setSeed(7)
    dt
  }

  def dt(df: DataFrame, continueArray: Array[String], dt: DecisionTreeClassifier): Map[String, Array[Double]] = {
    import df.sparkSession.implicits._
    val vectorAssembler = new VectorAssembler()
    var binsMap: mutable.Map[String, Array[Double]] = mutable.Map()
    //    for (f <- continueArray) {
    //      val singleDf = vectorAssembler.setInputCols(Array(f)).setOutputCol("features").transform(df.withColumn(f, $"$f".cast("double")))
    val singleDf = vectorAssembler.setInputCols(Array("td_7d_euquipment_num_m")).setOutputCol("features").transform(df.withColumn("td_7d_euquipment_num_m", $"td_7d_euquipment_num_m".cast("double")))
    //      singleDf.show()
    val model = dt.fit(singleDf)
    //      println(model.toDebugString)
    val binsArray = TreeUtils.extractConBins(model).toArray
    binsMap += ("td_7d_euquipment_num_m" -> binsArray)
    //    }
    binsMap.toMap
  }

}
