import com.kuainiu.beidou.util.DataFrameUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * create by colin on 2018/7/12
  */
object SparkDataFrame6 extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()
  //          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.createDataFrame(Seq((0, "1", "2", 3, 0.3), (1, "", "2", 13, 0.2), (2, "1", "2", 13, 0.2), (3, "1", "2", 23, 0.2), (4, "1", "2", 31, 0.5), (5, "1", "2", 35, 0.7), (0, "1", "2", 36, 0.5), (0, "1", "2", 39, 0.9)))
    .toDF("id", "age", "name", "score", "mu")

  import spark.implicits._

  val t_record = df.agg("id" -> "count").first().get(0).asInstanceOf[Long]

  //中位数
  val mediumDF = df.select("id").sort(df("id"))

  mediumDF.show()


  val newDf = DataFrameUtil.addIndexDf(mediumDF, "id")
  newDf.show()

  val max_index = t_record - 1L
//  val medium_index = if (max_index % 2 == 0) max_index / 2 else max_index / 2 + 1
//  val index25 = if (medium_index % 2 == 0) medium_index / 2 else medium_index / 2 + 1
//  val index75 =  (medium_index + max_index) / 2 + 1


  val medium_index = genIndex(0)_


  def genIndex(startIndex:Long)(endIndex: Long):Long= {

    startIndex match {
      case 0 =>  endIndex - startIndex
      case _ =>  endIndex + startIndex + 1
    }

}

  val medium = oddEvenProcess(newDf, "id", medium_index(max_index))



  val v25  = oddEvenProcess(newDf, "id", medium_index(max_index)/2 - 1)

  val v75_index =  genIndex(medium_index(max_index))_

  val v75 = oddEvenProcess(newDf, "id", v75_index(max_index))

  println(medium, v25, v75)


  def oddEvenProcess(df: DataFrame, col: String, maxRecordIndex: Long): Double = {
    maxRecordIndex % 2 match {
      case 0 => df.select("index", s"$col").where(df("index") === maxRecordIndex / 2).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
      case 1 => df.select("index", s"$col").where(df("index") === maxRecordIndex / 2 or df("index") === (maxRecordIndex / 2) + 1).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
    }
  }

  def oddEvenProcess2(df: DataFrame, col: String, index: Long): Double = {
    if (index == 0)
      df.select("index", s"$col").where(df("index") === index or df("index") === index + 1).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
    else index % 2 match {
      case 0 => df.select("index", s"$col").where(df("index") === index).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
      case _ => df.select("index", s"$col").where(df("index") === index or df("index") === index + 1).agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / 2.asInstanceOf[Double]
    }
  }


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

  import org.apache.spark.sql.functions._
  import spark.implicits._

  //  val cateFeatures = df.dtypes.filter { case (_, t) => t != "IntegerType" }.map { case (f, _) => f }

  //  df.dtypes.foreach(println(_))

  /*
 (id,IntegerType)
(age,StringType)
(name,StringType)
(score,IntegerType)
(mu,DoubleType)
   */

  case class ContinueIndex(id: String, records: Int,
                           col_avg: Double, medium: Double, mode: Double,
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

  case class ScatterIndex(id: String, records: Long,
                          diff_val_count: String,
                          mode: String,
                          null_percent: Double,
                          each_val_percent: String
                         )

  val num_val = 5
  var ListScatter: ListBuffer[ScatterIndex] = ListBuffer[ScatterIndex]()

  def scatter(df: DataFrame, col: String) = {

    //id号
    val id = "db_tb" + col

    //记录数
    val records = df.select(df(s"$col")).count()

    //取不同值数目（排除空值计算）
    val diff_val_count = df.select(df(s"$col")).where(df(s"$col") === "" or df(s"$col") === null)
      .groupBy(df(s"$col")).count().map(x => (x.get(0).asInstanceOf[Double], x.get(1).asInstanceOf[Long])).toString()

    //取值和占记录数比例，排除空值计算
    val mode = df.select(df(s"$col")).where(df(s"$col") === "" or df(s"$col") === null).groupBy(df(s"$col")).count().map(x => (x.get(0), x.get(1).asInstanceOf[Long] / records)).toString()

    //空值占比
    val null_percent = df.select(df(s"$col")).where(df(s"$col") === "" or df(s"$col") === null).count() / records

    //每个取值的占比（全量计算）
    val each_val_percent = df.select(df(s"$col")).groupBy(df(s"$col")).count().map(x => (x.get(0), x.get(1).asInstanceOf[Long] / records)).toString()

    ListScatter += ScatterIndex(id, records, diff_val_count, mode, null_percent, each_val_percent)
  }


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

  def conStat(df: DataFrame, col: String) = {
    //id号
    val id = "db_tb" + col

    //记录数
    val records = df.select(df(s"$col")).count()

    //均值
    val mean = df.agg(s"$col" -> "sum").first().get(0).asInstanceOf[Long] / records

    //中位数
    val mediumDF = df.select(col).sort(df(s"$col"))
    val newDf = DataFrameUtil.addIndexDf(mediumDF, col)
    val medium = oddEvenProcess(newDf, col, records)


    //min，max
    val min = df.where(df(s"$col") === "" or df(s"$col") === null).agg(s"$col" -> "min").first().get(0)
    val max = df.where(df(s"$col") === "" or df(s"$col") === null).agg(s"$col" -> "max").first().get(0)


    //    //25%分位点，75%分位点（排除空值计算）
    //    val quartiles_exclude_null =
    //    val three_quartiles_exclude_null =
    //

    //25%分位点，75%分位点（全量计算）
    val quartiles = oddEvenProcess(newDf, col, records / 2)
//    val three_quartiles = three_quartiles

  }

  //  df.dtypes.map { case (col, t) => if (t.isInstanceOf[String] || df.select(s"$col").distinct().count() <= num_val) scatter(df, col) else conStat(df, col) }.map { case (f, v) => (f, v) }
}

