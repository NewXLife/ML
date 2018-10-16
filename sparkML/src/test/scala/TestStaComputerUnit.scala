import com.kuainiu.beidou.statistic.SampleStatistic
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import util.SparkTools

import scala.collection.mutable.ArrayBuffer

object TestStaComputerUnit extends SparkTools {

  val base1 = loadCSVData("csv", "E:\\NewX\\newX\\bd-engine\\docs\\20180814_old_tdbase.csv").cache()
  base1.show(10)
  println("base count number:", base1.count())

  //  val pre = loadCSVData("csv", "E:\\NewX\\newX\\bd-engine\\docs\\20180907_old_td.csv")
  //    pre.show(10)

  val baseFeatureList = ArrayBuffer("d14", "apply_date", "7day", "1month", "3month", "6month", "12month", "18month", "24month", "60month")
  //remove label
  baseFeatureList.remove(baseFeatureList.indexOf("d14"))
  //remove time column
  baseFeatureList.remove(baseFeatureList.indexOf("apply_date"))

  val modeVal = SampleStatistic.recordsByDay(base1, "7day", "apply_date")
  modeVal.show()

  //avg,min,max,
  println(modeVal.count())

  val avgVal = SampleStatistic.meanGroupByDay(base1, "7day", "apply_date")
  avgVal.show()

  val quartiles = SampleStatistic.quantile(base1, "7day", Array(0, 0.25, 0.5, 0.75, 1))
  quartiles.foreach(println(_))
  /** result as follow:
    * -1.0  min
    * -1.0  1/4
    * 2.0   1/2
    * 4.0   3/4
    * 9.0   max
    */

  // df.selectExpr(s"CAST($col as Double)")
  val col = "7day"
//  base1.selectExpr(s"CAST($col as Double)").groupBy("apply_date").agg("7day"-> "max")
  base1.groupBy("apply_date").agg("7day"-> "max", "1month" -> "max", "7day"-> "count", "1month"->"count").show()
  val quartiles2 = base1.selectExpr(s"CAST($col as Double)").stat.approxQuantile("7day", Array(0, 0.25, 0.5, 0.75, 1), 0)

  base1.createOrReplaceTempView("index")
  println("hive quantile------------------------------------")
  val quantile1 = spark.sql("select apply_date, percentile_approx(7day,array(0,0.25,0.75,1)) as approxQuantile from index group by apply_date")
  val quantile2 = spark.sql("select apply_date, percentile_approx(7day,array(0,0.25,0.75,1)) as approxQuantile from index group by apply_date")

  println("hive quantile---------------join---------------------")
  quantile1.join(quantile2, Seq("apply_date"), "inner").show()


  quartiles2.foreach(println(_))

  object AvgUDF extends UserDefinedAggregateFunction{
    def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

    def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)

    def dataType: DataType = DoubleType

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      //sum = 0.0 double
      buffer(0) = 0.0
      //count = 0L
      buffer(1) = 0L
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)){
        //sum = sum + (input value)
        buffer(0) = buffer.getDouble(0) + input.getDouble(0)

        //count = count + 1
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //sum  buffer1 mege buffer2
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)

      //count
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    def evaluate(buffer: Row): Double =
      buffer.getDouble(0) / buffer.getLong(1).toDouble
  }


  val zeroWhere = s"case when $col=0 then 1 else 0 end"
  val nullWhere = s"case when $col is null then 1 else 0 end"

  spark.udf.register("myAverage", AvgUDF)
  println("udf------------------------------------")
  spark.sql("select myAverage(7day) as 7dayAvg from index").show


  val exeSql =
    s"""select
       |'$col' as index_name,
       |cast(apply_date as date) as sta_date,
       |count($col) as records,
       |count(distinct($col)) as unique_records,
       |mean($col) as avg_value,
       |sum($nullWhere)/count($col) as null_percent,
       |sum($zeroWhere)/count($col) as zero_percent
       |from index group by apply_date
       |""".stripMargin

  spark.sql(exeSql).show()

  // column-operator
  import org.apache.spark.sql.functions._
  val toDouble = udf[Double, String](_.toDouble)
  val modifyDf = base1.withColumn("7dayNew", toDouble(base1("7day"))).drop("7day").show()
  base1.withColumnRenamed("7day", "7dayRename").show()

  //  s"""select
  //     |max($indexId) as index_id,
  //     |'$indexType' as index_type,
  //     |'$col' as index_name,
  //     |count($col) as records,
  //     |count(distinct($col)) as unique_records,
  //     |mean($col) as avg_value,
  //     |'$modeValue' as mode_value,
  //     |'$quartiles' as quartiles,
  //     |'$quartilesExcludeNull' as quartiles_exclude_null,
  //     |sum($nullWhere)/count($col) as null_percent,
  //     |sum($zeroWhere)/count($col) as zero_percent,
  //     |max($dsId) as ds_id,
  //     |max($stId) as st_id
  //     |from  index
  //     |""".stripMargin


  spark.stop()
}
