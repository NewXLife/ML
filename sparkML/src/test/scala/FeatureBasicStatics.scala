import com.niuniuzcd.demo.util.DataFrameUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * create by colin on 2018/7/12
  */
object FeatureBasicStatics extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()
  //          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.createDataFrame(Seq((0, "1", "2", 3, 0.3), (1, "", "2", 13, 0.2), (2, "1", "2", 13, 0.2), (3, "1", "2", 23, 0.2), (4, "1", "2", 31, 0.5), (5, "1", "2", 35, 0.7), (0, "1", "2", 36, 0.5), (0, "1", "2", 39, 0.9)))
    .toDF("id", "age", "name", "score", "mu")

  //  val t_record = df.agg("id" -> "count").first().get(0).asInstanceOf[Long]

  //中位数
  //  val mediumDF = df.select("id").sort(df("id"))

  //  mediumDF.show()


  //  val newDf = DataFrameUtil.addIndexDf(mediumDF, "id")
  //  newDf.show()
  //
  //  val max_index = t_record - 1L
  //  val medium_index = if (max_index % 2 == 0) max_index / 2 else max_index / 2 + 1
  //  val index25 = if (medium_index % 2 == 0) medium_index / 2 else medium_index / 2 + 1
  //  val index75 =  (medium_index + max_index) / 2 + 1


  //  val medium_index = genIndex(0)_


  //  val medium = oddEvenProcess(newDf, "id", medium_index(max_index))
  //  val v25  = oddEvenProcess(newDf, "id", medium_index(max_index)/2 - 1)
  //  val v75_index =  genIndex(medium_index(max_index))_
  //
  //  val v75 = oddEvenProcess(newDf, "id", v75_index(max_index))
  //
  //  println(medium, v25, v75)


  //  def oddEvenProcess(df: DataFrame, col: String, maxRecordIndex: Long): Double = {
  //    maxRecordIndex % 2 match {
  //      case 0 => df.select("index", s"$col").where(df("index") === maxRecordIndex / 2).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
  //      case 1 => df.select("index", s"$col").where(df("index") === maxRecordIndex / 2 or df("index") === (maxRecordIndex / 2) + 1).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
  //    }
  //  }

  //  def oddEvenProcess2(df: DataFrame, col: String, index: Long): Double = {
  //    if (index == 0)
  //      df.select("index", s"$col").where(df("index") === index or df("index") === index + 1).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
  //    else index % 2 match {
  //      case 0 => df.select("index", s"$col").where(df("index") === index).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
  //      case _ => df.select("index", s"$col").where(df("index") === index or df("index") === index + 1).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
  //    }
  //  }


  //   df.agg(Map("id" -> "min", "id" -> "max")).show()
  //  +---+----+----+----+
  //  | id| age|name|score|
  //  +---+----+----+----+
  //  |  0|null|   2|   3|
  //  |  1|    |null|  13|
  //  |  2|   1|   2|  13|
  //  |  3|   1|   2|  23|
  //  |  4|   1|   2|  31|
  //  |  5|   1|   2|  35|
  //  |  0|   1|   2|  36|
  //  |  0|   1|   2|  39|
  //  +---+----+----+----+

  //  val cateFeatures = df.dtypes.filter { case (_, t) => t != "IntegerType" }.map { case (f, _) => f }

  //  df.dtypes.foreach(println(_))

  /*
 (id,IntegerType)
(age,StringType)
(name,StringType)
(score,IntegerType)
(mu,DoubleType)
   */



  case class ScatterIndex(id: String, records: Long,
                          diff_val_count: String,
                          mode: String,
                          null_percent: Double,
                          each_val_percent: String
                         )

  val num_val = 5


  //包括 计数count, 平均值mean, 标准差stddev, 最小值min, 最大值max。如果cols给定，那么这个函数计算统计所有数值型的列
  //  df.describe("id","name").show()
  //  +-------+---+----+
  //  |summary| id|name|
  //  +-------+---+----+
  //  |  count|  3|   3|
  //  |   mean|1.0|null|
  //  | stddev|1.0|null|
  //  |    min|  0|   a|
  //  |    max|  2|   c|
  //  +-------+---+----+


  def oddEvenProcess(df: DataFrame, col: String, maxRecordIndex: Long): Double = {
    maxRecordIndex % 2 match {
      case 0 => df.select("index", s"$col").where(df("index") === maxRecordIndex / 2).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
      case 1 => df.select("index", s"$col").where(df("index") === maxRecordIndex / 2 or df("index") === (maxRecordIndex / 2) + 1).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
    }
  }


  def genIndex(startIndex: Long)(endIndex: Long): Long = {
    startIndex match {
      case 0 => endIndex - startIndex
      case _ => endIndex + startIndex + 1
    }
  }



  case class ContinueIndex(id: String, records: Long,
                           mean: Double, medium: Double, mode: Double,
                           null_percent: Double, zero_percent: Double,
                           min_value_exclude_null: Double,
                           max_value_exclude_null: Double,
                           quartiles_exclude_null: Double,
                           three_quartiles_exclude_null: Double,
                           min_value: Double,
                           max_value: Double,
                           quartiles: Double,
                           three_quartiles: Double
                          )


}

