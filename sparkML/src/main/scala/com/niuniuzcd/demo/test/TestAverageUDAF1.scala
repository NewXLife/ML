package com.niuniuzcd.demo.test

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
  * @author Cock-a-doodle-doo!
  *         sum/count
  */
object TestAverageUDAF1 extends UserDefinedAggregateFunction{
  //定义输入类型
  override def inputSchema: StructType = StructType(StructField("input", LongType) :: Nil)

  //定义中间数据类型
  override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType):: Nil)

  //定义返回类型
  override def dataType: DataType = DoubleType

  //定义是否幂等
  override def deterministic: Boolean = true

  //初始化buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //如何更新数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //如何合并数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //合并后，如何计算结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
