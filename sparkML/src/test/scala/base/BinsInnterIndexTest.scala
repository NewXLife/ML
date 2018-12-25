package base

import com.niuniuzcd.demo.util.Tools
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.util.Success

object BinsInnterIndexTest extends App{
  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")
  import org.apache.spark.sql.functions._
  import spark.implicits._
  spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)
  spark.conf.set("spark.default.parallelism", 100)
  spark.conf.set("spark.sql.shuffle.partitions", 50)
  //    spark.conf.set("spark.driver.maxResultSize","2g")
  spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", value = true)

  ///user/hive/warehouse/base
  val test = loadCSVData("csv", "file:\\C:\\NewX\\newX\\ML\\docs\\testData\\base2.csv")
  println("source dataframe################")
  test.show()


  val labelCol = "d14"
  val testcols = "m24"
//  val testcols = "day7,m1,m3,m6,m12,m18,m24,m60"
  //  val testcol = Array("td_1month_platform_count","td_3month_platform_count")
  val row2ColDf = test.withColumnRenamed(labelCol, "label").selectExpr("label", s"${Tools.getStackParams(testcols.split(","): _*)} as (feature, value)").coalesce(100).cache()

  println("row2ColDf###############")
  row2ColDf.show()

 val  binsArray = Map("m24"-> Array(Double.NegativeInfinity, Double.PositiveInfinity))

  val row2ColBinsArrayDF = useBinsTemplate(row2ColDf, binsArray)
  println("row2ColBinsArrayDF################")
  row2ColBinsArrayDF.show()
  /**
    * +-----+-------+-----+--------------------+
    * |label|feature|value|                bins|
    * +-----+-------+-----+--------------------+
    * |    0|    m24| 54.0|[-Infinity, Infin...|
    * |    0|    m24| 80.0|[-Infinity, Infin...|
    * |    0|    m24| 68.0|[-Infinity, Infin...|
    * |    0|    m24| 35.0|[-Infinity, Infin...|
    * +-----+-------+-----+--------------------+
    */


  val binsDF = row2ColBinsArrayDF.withColumn("bin", splitBinning($"value", $"bins")).withColumn("bin_inner_index", binsInnerIndex($"value", $"bins")).orderBy("feature","bin_inner_index")
  println("new ---------------bins     ")
  binsDF.show(100, 500)


  def splitBinning: UserDefinedFunction = udf { (value: String, binsArray: Seq[Double]) =>

    if(value != null && !value.equals("null") && !value.equals("NULL")){
      val r1=scala.util.Try(value.toDouble)
      val result = r1 match {
        case Success(_) =>
          val index = searchIndex2(value.toDouble, binsArray.toArray)
          "(" +binsArray(index - 1).formatted("%.4f")+ ","+binsArray(index).formatted("%.4f") + "]"
        case _ =>
          "("+ Double.NegativeInfinity + "," + Double.PositiveInfinity + ")"
      }
      result
    }else{
      "("+ "Missing_values" + ")"
    }
  }


  def loadCSVData(csv: String, filePath: String, hasHeader: Boolean = true) = {
    if (hasHeader) spark.read.format(csv).option("header", "true").load(filePath)
    else spark.read.format(csv).load(filePath)
  }


  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
    temp
  }

  def binsInnerIndex: UserDefinedFunction = udf{(value: String, binsArray: Seq[Double]) =>
    if(value != null && !value.equals("null") && !value.equals("NULL")){
      val r1=scala.util.Try(value.toDouble)
      val result = r1 match {
        case Success(_) => searchIndex2(value.toDouble, binsArray.toArray)
        case _ => -1
      }
      result
    }else{
      -1
    }
  }

  def useBinsTemplate(df: DataFrame, binsArray: Map[String, Array[Double]], newCol:String = "bins", applyCol:String = "feature") = {
    df.withColumn(newCol, udf{f:String=>
      binsArray.filter{case(key, _) => key.equals(f)}.map{case(_, v)=> v}.toSeq.flatten.toArray
    }.apply(col(applyCol)))
  }
}
