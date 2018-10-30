package ml

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Assembler 装配工
从源数据中提取特征指标数据，这是一个比较典型且通用的步骤，因为我们的原始数据集里，经常会包含一些非指标数据，
如 ID，Description 等。为方便后续模型进行特征输入，需要部分列的数据转换为特征向量，并统一命名，
VectorAssembler类完成这一任务。VectorAssembler是一个transformer，将多列数据转化为单列的向量列
  */
object SparkMLlibVectorAssembler extends App{
  val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()

  val dataset = spark.createDataFrame(
    Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
  ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

  val assembler = new VectorAssembler().setInputCols(Array("hour","mobile")).setOutputCol("features")

 val res =  assembler.transform(dataset)
  res.show()
}
