import util.SparkTools

/**
  * create by colin on 2018/9/7
  */
object TestkolmogorovSmirnovTest extends SparkTools {
//  import spark.implicits._
//
//  case class UT(test:Double)
//  val test = spark.sparkContext.parallelize(Seq(0.1,0.03,0.3,0.25, 0.5).sorted,2)
//  val test_test = spark.createDataFrame(Seq(UT(0.1),UT(0.03),UT(0.3),UT(0.25),UT(0.5))).toDF("test")
// test_test.agg("test"->"mean", "test"->"variance").toDF("avg", "variance").map(x => (x.getDouble(0), x.getDouble(1))).show()

 val t =  Tools.toHash("7day") + 1.hashCode() * 17 + 1.hashCode() * 13
println("month18".hashCode)
println( Tools.toHash("month18").hashCode)
  //+-------------------+--------------+
  //|          avg(test)|variance(test)|
  //+-------------------+--------------+
  //|0.22599999999999998|       0.03263|
  //+-------------------+--------------+

//  val test1 = spark.sparkContext.parallelize(Seq(0.3, 0.1, 0.12, 0.04, 0.32).sorted,2)
//
//
//
//  val base1 = loadCSVData("csv", "/Users/colin/Desktop/bd_ML/docs/policy-test-ks-psi.csv").limit(10)
//  base1.show(10)
//  val pre = loadCSVData("csv", "/Users/colin/Desktop/bd_ML/docs/policy-test-ks-psi2.csv").limit(10)
//  pre.show(10)
//
//  base1.agg("7day"->"mean", "7day"->"variance").show()
//
//  val testResult = Statistics.kolmogorovSmirnovTest(test, "norm", 0.236, 0.03373)
//  println(testResult)
//  val testResult1 = Statistics.kolmogorovSmirnovTest(test1, "norm", 0.17600000000000002, 0.01588)
//  println(testResult1)
//
//  println(testResult.statistic-testResult1.statistic)
//
//
//  def mosMean(data:RDD[(String,Double)]): RDD[(String,Double)] ={
//    val dataStats = data.aggregateByKey(new StatCounter()
//    )(_ merge _,_ merge _)
//    val result = dataStats.map(f=>(f._1,f._2.mean))
//    result
//  }
//
//
//
//  //输入数据
//  val data = Array(
//    Vectors.dense(0.1,0.3),
//    Vectors.dense(0.03,0.1),
//    Vectors.dense(0.3,0.12),
//    Vectors.dense(0.25,0.04),
//    Vectors.dense(0.5,0.32)
//  )
//
//  // Array[Vector]转换成DataFrame
//  val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
//  df.show()
//
//  // DataFrame转换成RDD
////  val rddData=df.select("features").map{case Row(v: Vector) => v}
////  val rddData=df.select("features").rdd.map(line => Vectors.dense(
////  line(0).toString.toDouble,
////  line(1).toString.toDouble,
////  line(2).toString.toDouble,
////  line(3).toString.toDouble,
////  line(4).toString.toDouble
////))
//  val rddData1=df.select("features").rdd.map{case Row(v:Vector) => v}
//
//  // RDD转换成RowMatrix
//  val mat =  new RowMatrix(rddData1)
//
//  // 统计
//  val stasticSummary: MultivariateStatisticalSummary =mat.computeColumnSummaryStatistics()
//
//  // 均值
//  println(stasticSummary.mean)
//
//  // 方差
//  println(stasticSummary.variance)
}
