package sta

import com.niuniuzcd.demo.util.Tools
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.callUDF
import util.SparkTools

import scala.collection.mutable.ArrayBuffer
case class Feature(feature:String, value:String)
object StaTestDFGroupyByKey extends App{
  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")
  val test = Seq((1,3,4),(0, 2,1),(1,0,3))
  val testdd = spark.createDataFrame(test).toDF("label", "c1", "c2")
  testdd.show()

  val cola = Array("c1", "c2")
  val staDf = testdd.selectExpr("label", s"${Tools.getStackParams(cola:_*)} as (feature, value)")
  staDf.show()

  import spark.implicits._
  import org.apache.spark.sql.functions._
//  staDf.select("feature", "value").groupByKey(x=> {
//    println(x.getAs[String](0),x.getAs[String](1))
//    x.getAs[String](0)
//  })

  spark.udf.register("contactRowsUDF", ContactRowsUDF)

  val ds = staDf.select("feature","value").as[Feature]
  ds.show()
  ds.map(line => (line.feature, line.value)).show()

  val df1 = staDf.select("feature","value")
  df1.groupBy("feature").agg(
    callUDF("contactRowsUDF", $"value").as("newValue")
  ).show()

}
