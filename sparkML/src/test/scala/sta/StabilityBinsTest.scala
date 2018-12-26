package sta

object StabilityBinsTest extends App{
  val spark = StaFlow.spark

  import org.apache.spark.sql.functions._
  import spark.implicits._
//  val test = StaFlow.loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\base3.csv")
  val test = StaFlow.loadCSVData("csv", "file:\\D:\\NewX\\ML\\docs\\testData\\base3.csv").orderBy("ad")
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

//  d14,day7,m1,m3,m6,m12,m18,m24,m60
  val featureCols = "day7,m1,m3,m6,m12,m18,m24,m60".split(",")
  val labelCol = "d14"

  val startTime = "2018/6/19"
  val endTime = "2018/6/26"
    val timeInterval = 2
  val timeArray = StaFlow.timeInteral2Array(startTime, endTime, timeInterval)

  val row2ColsDf = StaFlow.row2ColDfContainsTimeCol(test, featureCols, labelCol, timeCol = "ad")
  row2ColsDf.show()
  /**
    * +-----+---------+--------------+-----+
    * |label|       ad|key_field_name|value|
    * +-----+---------+--------------+-----+
    * |    0|2018/6/19|          day7| -1.0|
    * |    0|2018/6/19|            m1| 1
    */

//
//  val disCount = 3
//  val aggDf = StaFlow.row2ColDFAggValue(row2ColsDf, method ="collect_set")
//  aggDf.show()
//  val binsDf = aggDf.withColumn("binCount",udf{tv:String =>{
//    val disCount = tv.split(",").length
//    disCount
//  }}.apply(col("tValue")))
//  binsDf.show()
//  /**
//    * +-------+-------------------+---------+
//    * |feature|             tValue|binCount|
//    * +-------+-------------------+---------+
//    * |    m18|33.0,68.0,48.0,73.0|        4|
//    * |    m12|67.0,66.0,42.0,33.0|        4|
//    * |     m3| 25.0,12.0,6.0,33.0|        4|
//    * |    m60|小学,博士,大学,初中|        4|
//    * |     m6|36.0,33.0,13.0,21.0|        4|
//    * |     m1|  5.0,10.0,16.0,2.0|        4|
//    * |   day7|       3.0,-1.0,4.0|        3|
//    * |    m24|     68.0,54.0,80.0|        3|
//    * +-------+-------------------+---------+
//    */
//
//  val staBinsDf = binsDf.withColumnRenamed("tValue", "bins")
//
//  val joinDF = row2ColsDf.join(staBinsDf, Seq("key_field_name"), "left")
//  joinDF.show()
//
//  val staDf = joinDF.withColumn("bin", StaFlow.categoriesBin($"value", $"bins", $"binCount"))
//
//  val binsDF = StaFlow.binsIndexExcludeMinMaxDF(staDf)
//  println("binsDF#####")
//  /**
//    * +-------+------+----------+------------+---------------+-------------------+
//    * |key_field_name|   bin|binSamples|overdueCount|notOverdueCount|overdueCountPercent|
//    * +-------+------+----------+------------+---------------+-------------------+
//    * |    m60|    大学|         5|           3|              2|                0.6|
//    * |   day7|   4.0|         3|           0|              3|                0.0|
//    * |    m24|  80.0|         3|           0|              3|                0.0|
//    */
//  binsDF.show()
//  val masterDF  = StaFlow.totalIndexDF(row2ColsDf)
//  println("masterdf#######")
//  masterDF.show()
//  /**
//    * +-------+------------+------------+---------------+-------------------+
//    * |key_field_name|totalSamples|totalOverdue|totalNotOverdue|totalOverduePercent|
//    * +-------+------------+------------+---------------+-------------------+
//    * |    m18|          13|           3|             10|                0.3|
//    * |     m3|          13|           3|             10|                0.3|
//    */
//
//  val binsIndex = StaFlow.binsExcludeMinMaxIndex(binsDF, masterDF)
//  binsIndex.show()
//  /**
//    * +--------------+------+-----------------+-------------------+-------------+-------------------+------------------+------------------+------------------+
//    * |key_field_name|   bin|bins_sample_count|  bins_sample_ratio|overdue_count|overdue_count_ratio|              lift|               woe|                iv|
//    * +--------------+------+-----------------+-------------------+-------------+-------------------+------------------+------------------+------------------+
//    * |           m60|    大学|                5|0.38461538461538464|            3|                0.6|               2.0|1.6094379124341003|1.2875503299472804|
//    * |          day7|   4.0|                3|0.23076923076923078|            0|                0.0|               0.0|              null|              null|
//    * |           m24|  80.0|                3| 0.3333333333333333|            0|                0.0|               0.0|              null|              null|
//    */
//
//  val totalIndex = StaFlow.totalCategoriesIndex(binsIndex)
//  totalIndex.join(staBinsDf.select("key_field_name","bins","binCount"), Seq("key_field_name"), "left").show()

}
