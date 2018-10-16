import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.reflect.internal.util.TableDef.Column

/**
  * create by colin on 2018/7/12
  */
object SparkDataFrame4 extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()
  //          Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.createDataFrame(Seq((0, null, "2", 3), (1, "", null, 13), (2, "1", "2", 13), (3, "1", "2", 23), (4, "1", "2", 31), (5, "1", "2", 35), (0, "1", "2", 36), (0, "1", "2", 39)))
    .toDF("id", "age", "name", "year")

  var ts: DataFrame = _
  var ts1: DataFrame = _

  var columns: Array[String] = _
  var dfTemp : DataFrame = _
  var dfs :Seq[DataFrame] = _

  var tempList: ListBuffer[DataFrame] = ListBuffer[DataFrame]()

  import spark.implicits._

  //  +---+----+----+----+
  //  | id| age|name|year|
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

  fitTest(df, spark).show()

  def fitTest(df:DataFrame, spark :SparkSession) ={
    val r1 = df.schema.fieldNames.map(f => (f, df.groupBy(s"$f").count())).map { case (f, tmp_df) => (f, tmp_df.select(tmp_df(s"$f"), (tmp_df("count") + 1).as(s"${f}_count"))) }
    val r2 = for (i <- r1) yield df.select(df(s"${i._1}")).join(i._2, df(s"${i._1}") === i._2(s"${i._1}"), "left").select(s"${i._1}_count")

    for ( i <- r2.indices) tempList +=addIndexDf(r2(i),r2(i).schema.fieldNames(0), spark)

    for (i <- tempList.indices){
      if (dfTemp != null){
        dfTemp = dfTemp.join(tempList(i), dfTemp("index") === tempList(i)("index")).drop(dfTemp("index"))
      }else{
        dfTemp = tempList(i)
      }
    }

    dfTemp = dfTemp.drop(dfTemp("index"))
    dfTemp
  }


  import spark.implicits._
  //  val t = df.schema.fieldNames.map(f =>(f, df.groupBy(s"$f").count())).map{case(_, d) => }


//  val r1 = df.schema.fieldNames.map(f => (f, df.groupBy(s"$f").count())).map { case (f, tmp_df) => (f, tmp_df.select(tmp_df(s"$f"), (tmp_df("count") + 1).as(s"${f}_count"))) }


//  for (i <- r2){
//    i.na.fill(1).map(v => math.log(v.get(0).asInstanceOf[Long])).show()

//  }



//  val bv = spark.sparkContext.broadcast(i)



//  r2(0).withColumn("cc", r2(0)("id_count")*0).map{case (i,v ) => }



//  resDf.join(resDf1, resDf("index") === resDf1("index")).drop(resDf("index")).show()

  //df1("id") === df("id"), "inner"

//  var df_temp : DataFrame = _
//
//  for (i <- dfs.indices){
//    if (df_temp != null){
//      df_temp = df_temp.join(dfs(i), dfs(i)("index") === df_temp("index")).drop(dfs(i)("index"))
//    }else{
//      df_temp = dfs(i)
//    }
//
//  }
//
//  val res = df_temp.drop(df_temp("index"))



  def addIndexDf(df:DataFrame, f:String, spark:SparkSession):DataFrame = {
    val mRdd = df.na.fill(1).rdd

//    val newRdd = mRdd.map(x => x.toString.substring(1, x.toString.length -1)).zipWithIndex()
    val newRdd = mRdd.mapPartitions(par => par.map(x => x.toString.substring(1, x.toString.length -1))).zipWithIndex()

//    val rowRdd = newRdd.map( a => Row(a._1.toInt, a._2.toInt))
    val rowRdd = newRdd.mapPartitions(par => par.map(a => Row(a._1.toInt, a._2.toInt)))

    val schema = StructType(
      Array(
        StructField(f, IntegerType, nullable = true),
        StructField("index", IntegerType, nullable = true)
      )
    )

    val resDf = spark.createDataFrame(rowRdd, schema)
   resDf
  }



//  val t1 = df.groupBy(df("id")).agg(Map("id" -> "count"))
//  df.groupBy(df("age")).agg(Map("age" -> "count")).show()
//  df.groupBy(df("name")).agg(Map("name" -> "count")).show()
//  df.groupBy(df("year")).agg(Map("year" -> "count")).show()





//  val t3 = spark.sql("select age_count from test2 where age_count is not null")
//  t3.show()



//
//  for(i <- r2.indices){
//    r2(i).show()
//  }

//  df.withColumn("id2", df("id")).show()
//
//  println(r2(1)("age_count"))
//

//  +--------+---------+
//  |id_count|age_count|
//  +--------+---------+
//  |       4|     null|
//  |       4|        2|
//  |       4|        7|
//  |       4|        7|
//  |       4|        7|
//  |       4|        7|
//  |       4|        7|
//  |       4|        7|
//  |       2|     null|
//  |       2|        2|
//  |       2|        7|
//  |       2|        7|
//  |       2|        7|
//  |       2|        7|
//  |       2|        7|
//  |       2|        7|
//  |       2|     null|
//  |       2|        2|
//  |       2|        7|
//  |       2|        7|
//  +--------+---------+

//  for (i <- r2.indices){
//
//    if (ts != null){
//      ts = ts.crossJoin(r2(i))
//    }else{
//      ts = r2(i)
//    }
//  }

//  ts.show()

//  def genDf(df:DataFrame):DataFrame = {
//    var dt = df
//    val ds = dt.crossJoin()
//
//  }




  //features

//  val data = spark.createDataFrame(List(
//    (1.0, 2.0, 3.0),
//    (11.0, 21.0, 31.0)
//  )).toDF("tag1", "tag2", "tag3")
//  val assembler = new VectorAssembler()
//    .setInputCols(Array("tag1", "tag2"))
//    .setOutputCol("features")
//  val output = assembler.transform(data)
//  output.show()



  //  df.withColumn("id2", df("id")).show()


  //  val t = df.groupBy("age").count()
  //
  //  count by column, smoothing 1
  //  val temp = t.select(t("age"), (t("count")+1).as("age_count"))
  //  temp.show()
  //  temp.select(temp("age_count")).map(x => math.log(x(0).asInstanceOf[Long])).show()


  //  df.stat.freqItems(Seq("id", "age","name","year")).show()

  //  df.groupBy("id").count().map{case(x, y) => (x,y+1)}.show()


  //  df.stat.freqItems(Seq("id")).show()

//    val dd = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3))).toDF("key", "value")
//    val dd1 = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3))).toDF("key1", "value1")
//
//  dd.crossJoin(dd1).show()

  //  dd.show()
  //  val ct = dd.stat.crosstab("key", "value")
  //  ct.show()




//  people.filter("age > 30")
//    .join(department, people("deptId") === department("id"))
//    .groupBy(department("name"), people("gender"))
//    .agg(avg(people("salary")), max(people("age")))
}
