package sta

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{callUDF, col, lit, udf}

object BinningQcut extends App{
val testarray = Array(1.2,30.0, 4.5)
  println(testarray.toString)

  def qcut(df: DataFrame, binsNum:Int = 10):DataFrame = {
    import df.sparkSession.implicits._
    val tempDf = df.groupBy("feature").agg(
      callUDF("percentile_approx", $"value", lit((0.0 to 1.0 by 1.0/binsNum).toArray)).as("bin")
    )

    tempDf.show(20, truncate = 0)

    val binsArrayDF = tempDf.withColumn("bin", udf{ splits:Seq[Double] =>
      if(splits != null){
        var buffer = splits.toBuffer
        buffer(0) =  Double.NegativeInfinity
        buffer(buffer.length - 1) = Double.PositiveInfinity
        buffer =  Double.NegativeInfinity +: buffer.filter(_ >0)
        buffer.distinct.toArray
      }else{
        Array(Double.NegativeInfinity, Double.PositiveInfinity)
      }

    }.apply(col("bin")))

    binsArrayDF
  }
}
