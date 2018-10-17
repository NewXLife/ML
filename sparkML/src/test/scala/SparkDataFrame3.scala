import org.apache.spark.sql.SparkSession

/**
  * create by colin on 2018/7/12
  */
object SparkDataFrame3 extends  App{
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()
//          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.createDataFrame(Seq((0,null,"2", 3),(1, "",null, 13),(2, "1","2",13),(3, "1","2",23),(4, "1","2",31),(5, "1","2",35),(0, "1","2",36),(0, "1","2",39)))
    .toDF("id", "age","name","year")




  val df1 = df.na.fill(Map("age"->"0.3", "name" ->"newName"))
  df1.show()


//  def getCount(df:DataFrame) = {
//         val t =    df.count()
//    t
//  }
//  df.show()
//  val total = getCount(df)
//  val dis = df.schema.fieldNames.map(fName => (fName,df.dropDuplicates(s"$fName").count())).filter{case(_, p) => (p.asInstanceOf[Long] / total) > 0.8}.map{case(f,_) => f}
////  val dis = df.schema.fieldNames.map(fName => (fName,df.dropDuplicates(s"$fName").count()))
//
// for(i <- dis) println(i)
//
//
//  for (i <- df.schema.fieldNames.map(x => getCount(df.dropDuplicates(x)))) println(i)




// val df1 = df.na.fill("9999")

//  println(df.where("age is null").count())
//  df.where("id is null or age is null or name is null or year is null").show()
//
//  df1.describe("id", "age","name","year").show()

//  val t = df.schema.fields.map(f => (f.name,df.where(s"${f.name} is null").count()))
//
//  val temp = for (i <- t ; if i._2 > 0.8) yield i._1
//  println(temp.getClass.getSimpleName)
//  temp.foreach(println(_))

//freqItems

//  val ss = df.groupBy("age").count()
//
//  val st = ss.sort(- ss("count")).show()
//  +----+-----+
//  | age|count|
//  +----+-----+
//  |   1|    7|
//  |null|    1|
//  +----+-----+

//  println(ss.sort(- ss("count")).first().get(1))

//  println(df.count())
//  val total = df.count().asInstanceOf[Double]

//  println("total=", total)

  //写法一：
//  val temp = df.schema.fieldNames.map(fName => (fName, df.groupBy(s"$fName").count().sort(-$"count").first().get(1).asInstanceOf[Long] / total))

  
  //写法二：
//  val temp = df.schema.fieldNames.map(fName => (fName, df.groupBy(s"$fName").count())).filter{case(_, cdf) =>cdf.sort(cdf("count").desc).first().get(1).asInstanceOf[Long] / total > 0.8}.map{case(f,_)=> f}
//
//  for(i<- temp) println(i)
                                            
//写法三：
  //  val temp = df.schema.fieldNames.map(fName => (fName, df.groupBy(s"$fName").count())).map(x =>(x._1, x._2.sort(x._2("count").desc).first().get(1).asInstanceOf[Long] / total))
//  for (i <- temp ) println(i._1, i._2)



//  val t = df.schema.fields.map(f => (f.name,df.stat.freqItems(Seq(s"${f.name}"))))
//    for (i <- t)println(i._1, i._2)



}
