package sta

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
case class Feature(feature:String, value:String)
object StaTestDFGroupyByKey extends App{
  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")
  val test = Seq((1,3,4),(0, 2,1),(1,0,3))
  val testdd = spark.createDataFrame(test).toDF("label", "c1", "c2")

  val testdd2 = spark.createDataFrame(test).toDF("label", "c11", "c22")
  testdd2.join(testdd, Seq("label"), "left").show()
  testdd.show()
  val testbins = Array(Double.NegativeInfinity, 2.0, 3.0, Double.PositiveInfinity)
  val testbins2 = Array(Double.NegativeInfinity, 3.0, 4.0, Double.PositiveInfinity)
  /**
    * +-----+---+---+
    * |label| c1| c2|
    * +-----+---+---+
    * |    1|  3|  4|
    * |    0|  2|  1|
    * |    1|  0|  3|
    * +-----+---+---+
    */

    val binsMap = Map("c1" -> testbins, "c2"-> testbins2)

  val cola = Array("c1", "c2")
  val staDf = testdd.selectExpr("label", s"${Tools.getStackParams(cola:_*)} as (feature, value)")
  staDf.show()

  /**
    * +-----+-------+-----+
    * |label|feature|value|
    * +-----+-------+-----+
    * |    1|     c1|    3|
    * |    1|     c2|    4|
    * |    0|     c1|    2|
    * |    0|     c2|    1|
    * |    1|     c1|    0|
    * |    1|     c2|    3|
    * +-----+-------+-----+
    */
  import spark.implicits._
  import org.apache.spark.sql.functions._
  val rettt = staDf.withColumn("bins", udf{f:String=>
    binsMap.filter{case(key, _) => key.equals(f)}.map{case(_, v)=> v}.toSeq.flatten.toArray
  }.apply(col("feature")))

  rettt.show(10, truncate = 0)
  import spark.implicits._
  import org.apache.spark.sql.functions._
//  staDf.select("feature", "value").groupByKey(x=> {
//    println(x.getAs[String](0),x.getAs[String](1))
//    x.getAs[String](0)
//  })

  spark.udf.register("contactRowsUDF", ContactRowsUDF)

  val ds = staDf.select("feature","value").as[Feature]
  val df1 = staDf.selectExpr("feature","cast(value as double)")
  //df.selectExpr("CAST(key AS Double)", "CAST(value AS Double)")
  ds.show()
  /**
    * +-------+-----+
    * |feature|value|
    * +-------+-----+
    * |     c1|    3|
    * |     c2|    4|
    * |     c1|    2|
    * |     c2|    1|
    * |     c1|    0|
    * |     c2|    3|
    * +-------+-----+
    */

  def ags(t: Feature, res:String):String ={
    ""
  }

  df1.show()
  /**
    * +-------+-----+
    * |feature|value|
    * +-------+-----+
    * |     c1|    3|
    * |     c2|    4|
    * |     c1|    2|
    * |     c2|    1|
    * |     c1|    0|
    * |     c2|    3|
    * +-------+-----+
    */

  df1.printSchema()

  val res = df1.rdd.map(x => (x.getAs[String](0), x.getDouble(1))).groupByKey().cache().map(row =>{
      var datas = Seq[Tuple1[Double]]()
      for (v <- row._2) {
        if (v != null) {
          datas = datas :+ Tuple1(v)
        }
      }
      val sk = SparkSession.builder().master("local[2]").getOrCreate().createDataFrame(datas).toDF("valueField")
      val bucketizer = new QuantileDiscretizer().setInputCol("valueField").setNumBuckets(10).setRelativeError(0d).setHandleInvalid("skip").fit(sk.select($"valueField".cast(DoubleType)))
     Row(row._1, bucketizer.getSplits)
    })

//  res.collect().foreach(println(_))
  val schema = StructType(Array(StructField("feature",StringType,true),StructField("bin",DataTypes.createArrayType(DoubleType),true)))
  spark.createDataFrame(res, schema).show(10,truncate = 0)

//  ds.groupByKey(x => x.feature).mapValues(v => )
//  ds.map(line => (line.feature, line.value)).show()

//  val df1 = staDf.select("feature","value")
//  df1.groupByKey(x => x.getAs(0)).agg()
//  df1.groupBy("feature").agg(
//    callUDF("contactRowsUDF", $"value").as("newValue")
//  ).show()

//  val PARTITION_SIZE = 10
//  val keyedRDD = df1.map(row => List(Feature(
//    row.getAs(0).toString,
//    row.getAs(1).toString
//  )))
//keyedRDD.show()
//  val reduced = keyedRDD.reduce(_:::_)
//  println(reduced.mkString(","))

}
