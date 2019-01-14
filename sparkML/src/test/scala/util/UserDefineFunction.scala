package util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

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


  object AggKs extends UserDefinedAggregateFunction{
    def inputSchema: StructType =  StructType(StructField("label",StringType) :: StructField("tValue",StringType):: Nil)

    def bufferSchema: StructType =  StructType(StructField("buffer1", ArrayType(DoubleType, containsNull = false)) :: StructField("buffer2", ArrayType(DoubleType, containsNull = false)) :: Nil)

    def dataType: DataType = DoubleType

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Array[Double]())
      buffer.update(1, Array[Double]())
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val label = input.getAs[String](0)
      val value = input.getAs[String](1)
      val r1 = value.split(",").map(x => x.toDouble).toBuffer
      if(label.toDouble == 0d){
        buffer(0) = buffer.getAs[Seq[Double]](0) ++ r1
      }else{
        buffer(1) = buffer.getAs[Seq[Double]](1) ++ r1
      }
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Seq[Double]](0) ++  buffer2.getAs[Seq[Double]](0)
      buffer1(1) = buffer1.getAs[Seq[Double]](1) ++  buffer2.getAs[Seq[Double]](1)
    }

    def evaluate(buffer: Row): Double = {
      val p =  buffer.getAs[Seq[Double]](0)
      val n =  buffer.getAs[Seq[Double]](1)
      ks2Samp(p.toArray, n.toArray)
    }
  }

  def ks2Samp(data1: Array[Double], data2: Array[Double]): Double = {
    val s1 = data1.sorted
    val s2 = data2.sorted
    val n1 = s1.length
    val n2 = s2.length

    val dataAll = s1 ++ s2

    val cdf1 = searchSorted(s1, dataAll).map(x => x / (1.0 * n1))
    val cdf2 = searchSorted(s2, dataAll).map(x => x / (1.0 * n2))

    val statistic = cdf1.zip(cdf2).map(x => x._2 - x._1).map(x => math.abs(x)).max
    statistic
  }

  private def searchSorted(data1: Array[Double], data2: Array[Double], side: String = "right"): ArrayBuffer[Int] = {
    require(data1.length > 0 && data2.length > 0, "Array size must be bigger than zero")
    val len1 = data1.length
    val len2 = data2.length

    var resIndexArray: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val r1 = (for (i <- data1.indices) yield i.toInt).toArray

    def searchIndex(v2: Double, array: Array[Double]): Int = {
      var temp = 0
      for (i <- array.indices) if (v2 >= array(i)) temp += 1 else return temp
      temp
    }

    for (j <- 0 until len2) {
      val v2 = data2(j)
      resIndexArray += searchIndex(v2, data1)
    }
    resIndexArray
  }
}
