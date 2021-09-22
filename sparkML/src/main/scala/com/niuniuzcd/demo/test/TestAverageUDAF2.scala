package com.niuniuzcd.demo.test

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
  * @author Cock-a-doodle-doo!
  *         带类型处理
  *         [in buffer out]
  */
case class AvgTemplate(var sum: Double, var count: Long)

case class Data(id: Long, weight: Double, height: Double, age: Long)

case class Record(id: Long, weight: Double, height: Double, age: Long)

object TestAverageUDAF2 extends Aggregator[Record, AvgTemplate, Double] {
  //初始化buffer对象
  override def zero: AvgTemplate = {
    AvgTemplate(0L, 0L)
  }

  //区数据规约
  override def reduce(b: AvgTemplate, a: Record): AvgTemplate = {
    b.sum += a.age
    b.count += 1L
    b
  }

  //分区合并
  override def merge(b1: AvgTemplate, b2: AvgTemplate): AvgTemplate = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //完成处理逻辑
  override def finish(reduction: AvgTemplate): Double = {
    reduction.sum / reduction.count
  }

  //buffer 编码类型
  override def bufferEncoder: Encoder[AvgTemplate] = Encoders.product

  //输出类型编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}
