package ml.feature.encoder

import com.niuniuzcd.demo.ml.transformer.{CategoryEncoder, ContinueEncoder, BaseEncoder}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import util.SparkTools

object ContinueEncoderTest extends SparkTools with Serializable {

//  for( c <- tempLaoke.columns){
//    println(c, tempLaoke.select(c).distinct().count())
//  }

//  val test = tempLaoke.limit(10000).persist()
//  println(test.columns.length)
//
//  val tttt = test.select("apply_risk_id").distinct().count()
//
//  val ccc = new BaseEncoder()
//  ccc.fit(test)
//  val res = ccc.transform(test)
//  println(res.columns.length)


  //  val r1 = baseDf.groupBy("ad").count()
  //  println(r1.printSchema())
  //  import spark.implicits._
  // val r2 =  baseDf.join(r1, baseDf("ad").equalTo(r1("ad")) , "left")
  //  r2.drop("ad").show() //delete all ad name
  //
  //  val ml : UserDefinedFunction =  udf{f: Double => math.log(f)}
  //  val r3 = r2.drop("ad").withColumn("ad", r2("count") + 1)
  //  r3.printSchema()
  //  r3.withColumn("log_ad", ml(r3("ad"))).show()
  /**
    * +----------+-----+
    * |2018-06-25|6810 |
    * |2018-06-29|4266 |
    * |2018-06-28|6006 |
    * |2018-06-24|6596 |
    * |2018-06-26|5471 |
    * +----------+-----+
    */
  /**
    * +---+----+----+----+----+----+----+-----+-----+-----+----+-----------------+
    * |d14|day7|  m1|  m3|  m6| m12| m18|  m24|  m60|count|  ad|           log_ad|
    * +---+----+----+----+----+----+----+-----+-----+-----+----+-----------------+
    * |0.0|-1.0| 2.0| 6.0|13.0|42.0|48.0| 54.0| 54.0| 6596|6597|8.794370279222871|
    * |0.0| 4.0| 5.0|12.0|21.0|67.0|73.0| 80.0| 80.0| 6596|6597|8.794370279222871|
    * |0.0| 3.0|10.0|25.0|36.0|66.0|68.0| 68.0| 68.0| 6596|6597|8.794370279222871|
    */


  //    val ce = new ContinueEncoder()
  //  ce.fit(baseDf)
  //
  //  for ((k, v) <- ce.binsMap){
  //    println(k, v.mkString(","))
  //  }
  //
  //  ce.transform(baseDf).show(100, truncate = 0)

  //  val splitsM1 = Array[Double](Double.NegativeInfinity, 2.0, 3.0, 5.0, 6.0, 8.0, 10.0, 12.0, 16.0, 21.0, Double.PositiveInfinity)
  //  val splitsDay7 = Array[Double](Double.NegativeInfinity, 2.0, 3.0, 4.0, 5.0, 8.0, Double.PositiveInfinity)
  //
  //  val bucketizer: UserDefinedFunction = udf { feature: Double =>
  //    binarySearchForBuckets(splitsDay7, feature, keepInvalid = true)
  //  }
  //  //
  //  val newCol = bucketizer(baseDf("day7"))
  //  //  val newField = prepOutputField(filteredDataset.schema)
  //  baseDf.withColumn("day7_bin", newCol).show(100, truncate = false)
  //
  //  import java.{util => ju}
  //
  //  def binarySearchForBuckets(
  //                              splits: Array[Double],
  //                              feature: Double,
  //                              keepInvalid: Boolean): Double = {
  //    if (feature.isNaN) {
  //      if (keepInvalid) {
  //        splits.length - 1
  //      } else {
  //        throw new SparkException("Bucketizer encountered NaN value. To handle or skip NaNs," +
  //          " try setting Bucketizer.handleInvalid.")
  //      }
  //    } else if (feature == splits.last) {
  //      splits.length - 2
  //    } else {
  //      val idx = ju.Arrays.binarySearch(splits, feature)
  //      if (idx >= 0) {
  //        idx -1 // index -> 为二分找到到的索引号，修改索引之前
  //      } else {
  //        val insertPos = -idx - 1
  //        if (insertPos == 0 || insertPos == splits.length) {
  //          throw new SparkException(s"Feature value $feature out of Bucketizer bounds" +
  //            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
  //            s"the lower/upper bound constraints.")
  //        } else {
  //          insertPos - 1
  //        }
  //      }
  //    }
  //  }

  //  baseDf.show(10, truncate = 0)

  /**
    * +----------+---+----+----+----+----+----+----+----+----+
    * |ad        |d14|day7|m1  |m3  |m6  |m12 |m18 |m24 |m60 |
    * +----------+---+----+----+----+----+----+----+----+----+
    */
  //  ce.fit(baseDf)

  //  for((k, v)<- ce.binsMap){
  //    println(k,spliterBins(v).mkString(","))
  //  }
  /** 和python 对比分箱一样，结果对比不上
    * (m3,-Infinity,7.0,11.0,15.0,18.0,22.0,26.0,31.0,37.0,45.0,Infinity)
    * (m12,-Infinity,20.0,28.0,34.0,40.0,46.0,53.0,60.0,69.0,82.0,Infinity)
    * (m60,-Infinity,23.0,32.0,39.0,45.0,52.0,60.0,68.0,79.0,95.0,Infinity)
    * (m18,-Infinity,22.0,30.0,37.0,44.0,50.0,58.0,66.0,76.0,92.0,Infinity)
    * (m24,-Infinity,23.0,31.0,38.0,45.0,52.0,59.0,68.0,78.0,94.0,Infinity)
    * (m1,-Infinity,2.0,3.0,5.0,6.0,8.0,10.0,12.0,16.0,21.0,Infinity)
    * (day7,-Infinity,-1.0,2.0,3.0,4.0,5.0,8.0,Infinity)     -1 需要去除
    * (m6,-Infinity,12.0,17.0,22.0,27.0,31.0,36.0,42.0,49.0,58.0,Infinity)
    */

  def spliterBins(interval: Array[Double]) = {
    val tem = if (interval.map(_ < 0).length >= 2) {
      var t = interval.filter(x => x > 0).toBuffer
      t +:= Double.NegativeInfinity
      t.toArray
    } else interval
    tem
  }

  //  val res = ce.transform(baseDf)
  //  res.show(10000, truncate = false)
}
