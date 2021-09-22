package com.niuniuzcd.demo.test

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

/**
  * @author Cock-a-doodle-doo!
  *         带类型处理
  *         [in buffer out]
  */
case class TestAverageUDAF3(col: String) extends Aggregator[Row, AvgTemplate, Double] {
  //初始化buffer对象
  override def zero: AvgTemplate = {
    AvgTemplate(0d, 0L)
  }

  override def reduce(b: AvgTemplate, a: Row): AvgTemplate = {
    b.sum += a.getAs[Number](col).doubleValue()
    b.count += 1
    b
  }

  override def merge(b1: AvgTemplate, b2: AvgTemplate): AvgTemplate = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: AvgTemplate): Double = {
    reduction.sum / reduction.count
  }

  override def bufferEncoder: Encoder[AvgTemplate] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
