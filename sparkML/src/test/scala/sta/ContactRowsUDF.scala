package sta

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object ContactRowsUDF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("inputColumn", StringType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("c1", StringType)::Nil)

  override def dataType: DataType = DataTypes.createArrayType(DoubleType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0)= buffer.getString(0) +"," + input.getString(0)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
  }

  override def evaluate(buffer: Row): Array[Double] = {
    val str = buffer.getString(0).tail
    val res = for (t <- buffer.getString(0).tail.split(",") if str.nonEmpty) yield Tuple1(t)
      val temspark = SparkSession.builder().master("local[*]").getOrCreate()
    val tempDF = temspark.createDataFrame(res.toSeq).toDF("f").coalesce(5)
    val qd = new QuantileDiscretizer().setInputCol("f").setNumBuckets(10).setHandleInvalid("skip").fit(tempDF.select(tempDF("f").cast(DoubleType)))
    var interval = qd.getSplits
    if (interval.map(_ < 0).length >= 2) {
      var t = interval.filter(x => x > 0).toBuffer
      t +:= Double.NegativeInfinity
      interval = t.toArray
    }
    interval
  }
}
