package util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UserDefineFunction {
  object ContactRowsUDF extends UserDefinedAggregateFunction{
    def inputSchema: StructType = StructType(StructField("inputColumn", StringType) :: Nil)

    def bufferSchema: StructType = StructType(StructField("c1", StringType) :: Nil)

    def dataType: StringType = StringType

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = ""
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)){
        //sum = sum + (input value)
        buffer(0) = buffer.getString(0) + "," + input.getString(0)
      }
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
    }

    def evaluate(buffer: Row): String = {
      buffer.getString(0)
    }
  }


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
}
