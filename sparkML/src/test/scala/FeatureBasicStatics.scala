import com.google.gson.Gson
import com.niuniuzcd.demo.util.DataFrameUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * create by colin on 2018/7/12
  */
object FeatureBasicStatics extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()


  case class Bin(index:Int, bin:String)

  import org.apache.spark.sql.functions._
  import spark.implicits._
  //          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.createDataFrame(Seq((0, "1", "2", 3, 0.3), (1, "", "2", 13, 0.2), (2, "1", "2", 13, 0.2), (3, "1", "2", 23, 0.2), (4, "1", "2", 31, 0.5), (5, "1", "2", 35, 0.7), (0, "1", "2", 36, 0.5), (0, "1", "2", 39, 0.9)))
    .toDF("id", "age", "name", "score", "mu")

  /**
    * scala use map2Json
    */
  val map = new java.util.HashMap[String, Object]()
//  map.put("abc", List(Bin(0, "(0,1]"),Bin(0,"(1,2]")).toArray)
  val gson = new Gson()
//  println( gson.toJson(map) )

  df.withColumn("crossBin", udf{(x:String, y:String) =>{
    map.put("f1", Bin(0, x))
    map.put("f2", Bin(0, y))
//    var map = Map("f1" -> Bin(0, x), "f2"->Bin(0,y))
    gson.toJson(map)
  }}.apply($"score", $"mu")).show(10, truncate = 0)
  /**
    * +---+---+----+-----+---+
    * | id|age|name|score| mu|
    * +---+---+----+-----+---+
    * |  0|  1|   2|    3|0.3|
    * |  1|   |   2|   13|0.2|
    * |  2|  1|   2|   13|0.2|
    * |  3|  1|   2|   23|0.2|
    * |  4|  1|   2|   31|0.5|
    * |  5|  1|   2|   35|0.7|
    * |  0|  1|   2|   36|0.5|
    * |  0|  1|   2|   39|0.9|
    * +---+---+----+-----+---+
    */


    val t_record = df.agg("id" -> "count").first().get(0).asInstanceOf[Long]

//  中位数
    val mediumDF = df.select("id").sort(df("id"))

    mediumDF.show()

    val newDf = DataFrameUtil.addIndexDf(mediumDF, "id")
    newDf.show()

    val medium_index = genIndex(0)_

     df.agg(Map("id" -> "min", "id" -> "max")).show()
//    +---+----+----+----+
//    | id| age|name|score|
//    +---+----+----+----+
//    |  0|null|   2|   3|
//    |  1|    |null|  13|
//    |  2|   1|   2|  13|
//    |  3|   1|   2|  23|
//    |  4|   1|   2|  31|
//    |  5|   1|   2|  35|
//    |  0|   1|   2|  36|
//    |  0|   1|   2|  39|
//    +---+----+----+----+

    val cateFeatures = df.dtypes.filter { case (_, t) => t != "IntegerType" }.map { case (f, _) => f }

    df.dtypes.foreach(println(_))
  /*
  (id,IntegerType)
  (age,StringType)
  (name,StringType)
  (score,IntegerType)
  (mu,DoubleType)
   */
  val num_val = 5

//  包括 计数count, 平均值mean, 标准差stddev, 最小值min, 最大值max。如果cols给定，那么这个函数计算统计所有数值型的列

    df.describe("id","name").select($"id",$"name",concat_ws("->", $"id", $"name").as("value")).show()
//    +-------+---+----+
//    |summary| id|name|
//    +-------+---+----+
//    |  count|  3|   3|
//    |   mean|1.0|null|
//    | stddev|1.0|null|
//    |    min|  0|   a|
//    |    max|  2|   c|
//    +-------+---+----+

  def genIndex(startIndex: Long)(endIndex: Long): Long = {
    startIndex match {
      case 0 => endIndex - startIndex
      case _ => endIndex + startIndex + 1
    }
  }

}

