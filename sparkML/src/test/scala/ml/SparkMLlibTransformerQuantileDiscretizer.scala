package ml

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession

/**
  * 分位数离散化，和Bucketizer（分箱处理）一样也是：将连续数值特征转换为离散类别特征。
  * 实际上Class QuantileDiscretizer extends （继承自） Class（Bucketizer）。
  *
  * 使用算法 approximate algorithm
  * 参数1：不同的是这里不再自己定义splits（分类标准），而是定义分几箱(段）就可以了。QuantileDiscretizer自己调用函数计算分位数，并完成离散化。
  * -参数2： 另外一个参数是精度，如果设置为0，则计算最精确的分位数，这是一个高时间代价的操作。另外上下边界将设置为正负无穷，覆盖所有实数范围。
  */
object SparkMLlibTransformerQuantileDiscretizer extends App{
  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(Seq(
    (0.0, 30.0),
    (1.0, 40.0),
    (2.0, 10.0),
    (3.0, 50.0),
    (4.0, 60.0)
  )).toDF("id","age").cache()


  val continue = df.dtypes.filter(_._2 == "DoubleType").map(_._1)

  continue.foreach(println(_))

 //input must double data type
  val qd = new QuantileDiscretizer().setInputCol("age").setOutputCol("gage")
    .setNumBuckets(3)      //设置分箱数
    .setRelativeError(0) //设置precision-控制相对误差,设置为0时，将会计算精确的分位点（计算代价较高）。
    .fit(df)

  qd.getSplits.foreach(println(_))//分箱区别

  qd.transform(df).show() //左闭，右开区间，离散化数据

//val res = discretizerFun("temp", 4).fit(discretizerFun("hour", 2).fit(df).transform(df)).transform(discretizerFun("hour", 2).fit(df).transform(df))
  val res = discretizerFun("id", 3).fit(df).getSplits.foreach(println(_))

  def discretizerFun (col: String, bucketNo: Int):
  org.apache.spark.ml.feature.QuantileDiscretizer = {
    val discretizer = new QuantileDiscretizer()

    discretizer
      .setInputCol(col)
      .setOutputCol(s"${col}_result")
      .setNumBuckets(bucketNo)
  }

  //splits: (-Infinity,1.0, 2.0, 4.0, 5.0,Infinity)
/*
  | id| age|id_new|
  +---+----+------+
  |0.0|30.0|   0.0|
  |1.0|40.0|   1.0|
  |2.0|10.0|   2.0|
  |3.0|50.0|   2.0|
  |4.0|60.0|   3.0|
  |4.0|60.0|   3.0|
  |4.0|60.0|   3.0|
  |4.0|60.0|   3.0|
  |4.0|60.0|   3.0|
  |5.0|70.0|   4.0|
  */



}
