import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * create by colin on 2018/7/12
  */
object SparkDataFrame5 extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()
  //          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.createDataFrame(Seq((0, null, "2", 3), (1, "", null, 13), (2, "1", "2", 13), (3, "1", "2", 23), (4, "1", "2", 31), (5, "1", "2", 35), (0, "1", "2", 36), (0, "1", "2", 39)))
    .toDF("id", "age", "name", "score")

  //  +---+----+----+----+
  //  | id| age|name|score|
  //  +---+----+----+----+
  //  |  0|null|   2|   3|
  //  |  1|    |null|  13|
  //  |  2|   1|   2|  13|
  //  |  3|   1|   2|  23|
  //  |  4|   1|   2|  31|
  //  |  5|   1|   2|  35|
  //  |  0|   1|   2|  36|
  //  |  0|   1|   2|  39|
  //  +---+----+----+----+

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val dsWithLogSales = df.na.fill(99).withColumn("score",
    udf((sales: Int) => math.log(sales)).apply(col("score")))


  val ageNameDf = dsWithLogSales.withColumn("age",
    udf((age:String)=> if (age == null || age == "") "-999" else age).apply(col("age")))


  val ttt = df.withColumn("diff", abs($"id"-$"score"))



  dsWithLogSales.show()

  ageNameDf.show()


  ttt.show()

//  import spark.sql
//  val t = sql("select count(id) from (select distinct id from test)")
//
//
// val woeCols =  df.schema.fields.map(f =>(f, sql(s"select count($f) from (select distinct $f from test)"))).filter{case(_, ds) => ds.first().get(0).asInstanceOf[Long] <= 20}.map{case(f,_)=> f}


//  df.groupBy(df("id")).agg(Map("id" -> "count")).show()

//  val t = df.select("id").distinct().count()
//  println(t)
//
//
//  val t1 = df.schema.fieldNames.map(f => (f, df.select(s"$f").distinct().count())).filter{ case (_, num) => num.asInstanceOf[Long] >= 2}.map{case (f, _) => f}
//
//  for (i <- t1) print(i)
}

