package base

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, Row}
import util.{SparkTools, UserDefineFunction}


import DataFrameExtensions._

object BoxLineDiagramTest extends SparkTools {
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val base = loadCSVData("csv", "C:\\NewX\\newX\\MachineLearning\\docs\\testData\\base.csv")
  if(base.nonEmpty()){

  }
//  base.take(1).isEmpty
//  base.head(1).isEmpty
//  base.head().length
//  base.first().length
  import spark.implicits._

  val staDate = "ad"
  val col1 = "day7"

  val cols = "day7"
  val colsArr = Array(cols.split(","): _*)

  val qq = quantile(base, col1, staDate)

  type QAlis = Column => Column
  def qalis(startIndex: Int, aliesName: String): QAlis =
    ss => ss.getItem(startIndex).as(aliesName)

  spark.sqlContext.udf.register("len", (x: String) => x.length)
  qq.select($"sta_date", $"q".getItem(0).as("min"), $"q".getItem(1).as("q1")).show(100, truncate = false)

  //-1是一种控制编码，没有实际意义，只用来标记，不参与统计计算
  //空值0处理
  def quantile(df: DataFrame, colName: String, staDate: String): DataFrame = {
    import df.sparkSession.sql
    df.createOrReplaceTempView("index")
    sql(s"select $staDate as sta_date, percentile_approx($colName,array(0,0.25,0.5,0.75,1)) as q from index where $colName != -1 and $colName !=0 group by $staDate")
  }

  //use splitQuantile function when  the column types is inputs string
  def splitQuantile(df: DataFrame): DataFrame = {
    df.select($"sta_date", $"q").select(
      $"sta_date",
      col("q").getItem(0).as("min"),
      col("q").getItem(1).as("q1"),
      col("q").getItem(2).as("q2"),
      col("q").getItem(3).as("q3"),
      col("q").getItem(4).as("max")
    ).drop("q")
  }

  def sp: DataFrame => DataFrame = {
    df =>
      df.select($"sta_date", $"q").select(
        $"sta_date",
        col("q").getItem(0).as("min"),
        col("q").getItem(1).as("q1"),
        col("q").getItem(2).as("q2"),
        col("q").getItem(3).as("q3"),
        col("q").getItem(4).as("max")
      ).drop("q")
  }

  //IQR = Q3 - Q1
  //lower = Q1  - 1.5*IQR
  //upper = Q3 + 1.5*IQR

  def combineSq(df: DataFrame, staDate: String, colName: String)(splitDf: DataFrame) = df.select($"$staDate".as("sta_date"), $"$colName".cast(DoubleType)).join(sp(splitDf).
    select($"sta_date", $"q1".cast(DoubleType), $"q3".cast(DoubleType), $"min", $"q2", $"max"), Seq("sta_date"), "left").dropDuplicates(Array(s"$colName", "sta_date"))

  val commonDf = combineSq(base, "ad", "day7")(qq)
  println("common.............")
  commonDf.show(10, truncate = 0)

  type RowFilter = Row => Boolean
  type IQRFiler[A] = (A, A) => A
  val IQR: IQRFiler[Double] = (q3, q1) => q3 - q1

  type SizeChecker = (Double, Double, Double) => Boolean

  val predicate: SizeChecker = {
    case (a, b, c) => a < b - 1.5 * IQR(c, b) || a > c + 1.5 * IQR(c, b)
  }

  val sizeTest: SizeChecker => RowFilter =
    f =>
      r => f(r.getDouble(1), r.getDouble(2), r.getDouble(3))

  def reverse(predicate: SizeChecker): SizeChecker =
    (a, b, c) => !predicate(a, b, c)

  def reverse2: SizeChecker => SizeChecker =
    f =>
      (a,b,c) => !f(a,b,c)


  val OutLinerFilter: RowFilter = sizeTest(predicate)
  val LowerUpperFilter: RowFilter = sizeTest(reverse2(predicate))
  //      x => !(x.getDouble(1) < (x.getDouble(2) - 1.5*(x.getDouble(3) - x.getDouble(2))) || x.getDouble(1) > (x.getDouble(3) + 1.5*(x.getDouble(3) - x.getDouble(2))))

  println("use new function..............")
  val outlier = commonDf.filter(row => OutLinerFilter(row))


  outlier.createOrReplaceTempView("q")
  spark.udf.register("contactRows", UserDefineFunction.ContactRowsUDF)
  println("udf------------------------------------")
  val outlierDF = spark.sql(s"select sta_date, contactRows($col1) as outlier from q group by sta_date")


  println("use new function..............2")
  val lowerUpperDf = commonDf.filter(row => LowerUpperFilter(row)).select($"sta_date", $"$col1").groupBy($"sta_date").agg(max(col(s"$col1")).as("upper"), min(col(s"$col1")).as("lower"))

  outlierDF.join(lowerUpperDf, Seq("sta_date"), "left").show(10, truncate = 0)

}
