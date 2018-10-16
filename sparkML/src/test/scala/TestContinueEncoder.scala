

/**
  * create by colin on 2018/8/1
  */
object TestContinueEncoder extends App{
  val ce = new ContinueEncoder()

  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()
  //          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")


  val df = spark.createDataFrame(Seq((0.2, null, "2", 3), (1.4, "", null, 13), (2.5, "1", "2", 13), (6.2, "1", "2", 23), (9.7, "1", "2", 31), (2.1, "1", "2", 35)))
    .toDF("id", "age", "name", "year")


  val bins = ce.cut(df,"id", 3).toArray

//a.formatted("%.2f")
  val t = for( i <- 0 until bins.length -1) yield (bins(i).formatted("%.3f"), bins(i+1).formatted("%.3f"))

  bins.foreach(println(_))

  t.foreach(println(_))
}
