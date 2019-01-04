package sta

import com.niuniuzcd.demo.util.DSHandler

object StabilityBinsTest extends App {
  val spark = StaFlow.spark

  import org.apache.spark.sql.functions._
  import spark.implicits._

    val test = StaFlow.loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\td_statistic_feature_xk.csv")
//  val test = StaFlow.loadCSVData("csv", "file:\\D:\\NewX\\ML\\docs\\testData\\base3.csv").orderBy("ad")
  println(s"total:${test.count()}")
  /**
    * +---+----+----+----+----+----+----+----+---+---+---------+
    * |d14|day7|  m1|  m3|  m6| m12| m18| m24|m60|age|       ad|
    * +---+----+----+----+----+----+----+----+---+---+---------+
    * |  0|-1.0|16.0|33.0|33.0|33.0|33.0|null| 博士| 40|2018/6/19|
    * |  1|-1.0|16.0|33.0|33.0|33.0|33.0|null| 博士| 40|2018/6/20|
    * |  0|-1.0|16.0|33.0|33.0|33.0|33.0|null| 博士| 40|2018/6/21|
    */
  test.show(10)

  val staDf = test.withColumn("apply_risk_created_at", udf{x:Any => {
    StaFlow.timeFormat(x)
  }}.apply(col("apply_risk_created_at")))

  //  d14,day7,m1,m3,m6,m12,m18,m24,m60

  val labelCol = "overdue_days"

//  val colMap = Map("td_7day_platform_count_for_model" -> "(1,2],(2,3]",
//    "td_3month_platform_count_model" -> "(2,3],(3,4]"
//  )
//
//  val colMap2 = Map("td_7day_platform_count_for_model" -> Array("(1,2]", "(2,3]"),
//    "td_3month_platform_count_model" -> Array("(2,3]", "(3,4]")
//  )

  import scala.collection.JavaConversions._
  val finalMap = Map2Json.getJavaMap.mapValues(strV => strV.map(x => x).toArray).toMap
  val featureCols = finalMap.keySet.toArray

  //binsObj.getBinsTemplate.mapValues(strV => strV.split(",").map(x=> x.toDouble)).toMap

  val startTime = "2018/6/19"
  val endTime = "2018/6/26"
  val timeInterval = 2

  val timeArray = StaFlow.timeInteral2Array(startTime, endTime, timeInterval)
  println(timeArray.mkString(","))



  val resbinsArrar =  StaFlow.getBinsArray(timeArray)
  println(resbinsArrar.mkString(","))

//  val timeMap = colMap.keySet.toArray.map(x => (x, timeArray))
//  println(timeMap.mkString(","))


  val row2ColsDf = StaFlow.row2ColDfContainsTimeCol(staDf, featureCols, labelCol, timeCol = "apply_risk_created_at")
   val res1 = row2ColsDf.withColumn("sta_time_range", lit(StaFlow.filterSpecialChar(resbinsArrar.mkString(";"))))
  res1.show(10, truncate = 0)
  res1.printSchema()

  /**
    * +-----+----------+--------------+-----+---------------------------------------------------------------------------------------+
    * |label|ad        |key_field_name|value|timeArray                                                                              |
    * +-----+----------+--------------+-----+---------------------------------------------------------------------------------------+
    * |0    |2018-06-19|day7          |-1.0 |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-19|m1            |2.0  |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-19|m3            |6.0  |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-19|m6            |13.0 |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-19|m12           |42.0 |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-19|m18           |48.0 |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-19|m24           |54.0 |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-19|m60           |大学   |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-20|day7          |-1.0 |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * |0    |2018-06-20|m1            |2.0  |2018-06-19,2018-06-21;2018-06-21,2018-06-23;2018-06-23,2018-06-25;2018-06-25,2018-06-26|
    * +-----+----------+--------------+-----+---------------------------------------------------------------------------------------+
    */
  //  StaFlow.splitBinningString

  val res2= res1.withColumn("sta_time_range", StaFlow.splitBinningString($"apply_risk_created_at", $"sta_time_range"))

   val res3 = StaFlow.useBinsDefinedBinsTemplate(res2, finalMap)
  res3.show(10, truncate = 0)
  res3.printSchema()
  /**
    * +-----+----------+--------------+-----+-----------------------+----------------+
    * |label|ad        |key_field_name|value|timeRange              |bins            |
    * +-----+----------+--------------+-----+-----------------------+----------------+
    * |0    |2018-06-19|day7          |-1.0 |(2018-06-19,2018-06-21)|[(1, 2], (2, 3]]|
    * |0    |2018-06-19|m1            |2.0  |(2018-06-19,2018-06-21)|[(2, 3], (3, 4]]|
    * |0    |2018-06-19|m3            |6.0  |(2018-06-19,2018-06-21)|[(2, 3]]        |
    * |0    |2018-06-20|day7          |-1.0 |(2018-06-19,2018-06-21)|[(1, 2], (2, 3]]|
    */

    val res4 = res3.withColumn("bin", StaFlow.splitStabilitySubBinBinning($"value", $"bins")) //null
  /**
    * +-----+----------+--------------+-----+-----------------------+--------------+---------------+
    * |label|ad        |key_field_name|value|timeRange              |bins          |bin            |
    * +-----+----------+--------------+-----+-----------------------+--------------+---------------+
    * |0    |2018-06-19|day7          |-1.0 |(2018-06-19,2018-06-21)|[(1,2], (2,3]]|(missing-value)|
    * |0    |2018-06-19|m1            |2.0  |(2018-06-19,2018-06-21)|[(2,3], (3,4]]|(missing-value)|
    * |0    |2018-06-19|m3            |6.0  |(2018-06-19,2018-06-21)|[(2,3]]       |(missing-value)|
    */
    val binsDF = StaFlow.binsIndexTimeRange(res4)
  println("bins dataframe")
  binsDF.show(100, truncate = 0)

  val masterDF = StaFlow.totalIndexTimeRangeDF(res4)
  println("master dataframe")
  masterDF.show(100, truncate = 0)

    val binsIndex = StaFlow.binsExcludeMinMaxTimeRangeIndex(binsDF, masterDF)
    binsIndex.show(100, truncate = 0)
//  DSHandler.save2MysqlDb(binsIndex.withColumn("statistic_id", lit(100)).withColumn("statistic_uuid", lit("abc")).withColumnRenamed("bins","bin"), "dataset_statistic_bins_stability")
}
