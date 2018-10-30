import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * create by colin on 2018/7/12
  */

object SparkDataFrame extends App {
  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()

  val dfbbb = spark.createDataFrame(Seq((0.0, "a"), (0.1, "b"), (0.23, "c"), (0.123, "c"), (4.0, "c"), (9.0, "c"))).toDF("id", "name")

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

  df.createOrReplaceTempView("test")

  import spark.sql
  import spark.implicits._

  val zerowhere = "case when id=0 then 1 else 0 end"
  val nullwhere = "case when id is null or trim(id)='' then 1 else 0 end"

  //  +-----+-------------------+
  //  |   _1|                 _2|
  //  +-----+-------------------+
  //  |  0.0|0.16666666666666666|
  //  |  0.1|0.16666666666666666|
  //  |  4.0|0.16666666666666666|
  //  | 0.23|0.16666666666666666|
  //  |0.123|0.16666666666666666|
  //  |  9.0|0.16666666666666666|
  //  +-----+-------------------+

  //类型转换
  val pairs = df.selectExpr("CAST(key AS Double)", "CAST(value AS Double)")

  //concat_ws
  val st = sql("select concat_ws(',',key,value) as eacheP from each_table")
  //  +--------------------+
  //  |              eacheP|
  //  +--------------------+
  //  |0.0,0.16666666666...|
  //  |0.1,0.16666666666...|
  //  |4.0,0.16666666666...|
  //  |0.23,0.1666666666...|
  //  |0.123,0.166666666...|
  //  |9.0,0.16666666666...|
  //  +--------------------+

  st.createOrReplaceTempView("res_test")
  val rrr = sql("select concat_ws(';',collect_set(eachep)) from (select concat_ws(',',key,value) as eacheP from each_table)").first().get(0).toString
  println(rrr)

  //use agg function
  val colName = "id"
  spark.sqlContext.udf.register("fName", (id: String) => id)

  val statis = mutable.ArrayBuffer[(String, String)]()
  statis += "id" -> "sum"
  statis += "id" -> "avg"
  statis += "id" -> "max"
  statis += "id" -> "min"
  val res111 = statis.toArray

  val t1 = "id" -> "sum"

  df.agg( "id" -> "sum",res111: _*).show()


  import spark.implicits._

  /** 原来的id 列都增加1 */
  df.select(expr("id + 1").as[Double]).show()

  df.explode("c3", "c3_") { time: String => time.split(" ") }
  df.select(explode(split($"id", " ")).as("word"))

  /**
    * 获取指定字段统计信息
    * 统计字段出现频率在30%以上的内容
    */
  df.stat.freqItems(Seq("id"), 0.3).show()

  //when
  df.select(when($"gender" === "male", 0)
    .when($"gender" === "female", 1)
    .otherwise(2))

  // Creates a new struct column that composes multiple input columns.
  df.select(struct("id", "name").as("a-b")).show()
  /*
    +-----+
    |  a-b|
    +-----+
    |[0,a]|
    |[1,b]|
    |[2,c]|
    |[3,c]|
    |[4,c]|
    |[5,c]|
    +-----+
   */

  val data = Array(1, 2, 3, 4)
  val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("id")
  //
  //  dataFrame.stat.approxQuantile("id",Array(0.25, 0.5, 0.75), 0).foreach(println(_))
  //  val qs = SampleStatistic.quantile(dataFrame, "id", Array(0.25,0.5, 0.75))

  val dd = spark.sparkContext.parallelize(1.0 to 10.0 by 1.0).map(Tuple1.apply).toDF("id")
  val discretizer = new QuantileDiscretizer().setInputCol("id").setOutputCol("y").setNumBuckets(6)
  discretizer.fit(dd).getSplits.foreach(println(_))

  //　计算一个用数表示的列的DataFrame近似的分位点.
  val t = df.stat.approxQuantile("id", Array(0.5), 0)

  //二进制向量数据结构,它具有很好的空间和时间效率,被用来检测一个元素是不是集合中的一个成员。
  df.stat.bloomFilter($"id",5,10)

  discretizer.fit(df).getSplits.foreach(println(_))
  //  df.show()
  //  +---+----+
  //  | id|name|
  //  +---+----+
  //  |  0|   a|
  //  |  1|   b|
  //  |  2|   c|
  //  +---+----+

  val df1 = spark.range(0, 10)

  val dfd2 = df1.withColumn("add", lit(10))
  dfd2.show()

  //包括 计数count, 平均值mean, 标准差stddev, 最小值min, 最大值max。如果cols给定，那么这个函数计算统计所有数值型的列
  df.describe("id", "name").show()
  //  +-------+---+----+
  //  |summary| id|name|
  //  +-------+---+----+
  //  |  count|  3|   3|
  //  |   mean|1.0|null|
  //  | stddev|1.0|null|
  //  |    min|  0|   a|
  //  |    max|  2|   c|
  //  +-------+---+----+


  df.where(" id=1 or name = 'a'").show()
  df.filter(" id=1 or name = 'a'").show()

  //  | id|name|
  //  +---+----+
  //  |  0|   a|
  //  |  1|   b|
  //  +---+----+


  //true /false 指示显示长字符串和多个column是否都显示
  df.select("id").show(true)

  df.select(df("id")).distinct().count()

  df.select(df("id"), df("name") + "Pre-").show()
  //  +---+-------------+
  //  | id|(name + Pre-)|
  //    +---+-------------+
  //  |  0|         null|
  //  |  1|         null|
  //  |  2|         null|
  //  +---+-------------+

  df.selectExpr("id as newId", "name as newName").show()
  //  +-----+-------+
  //  |newId|newName|
  //  +-----+-------+
  //  |    0|      a|
  //  |    1|      b|
  //  |    2|      c|
  //  +-----+-------+

  //重分区
  df.repartition(10)
  df.repartition(10, $"id")
  df.repartition($"id",$"age")

  val idCol = df.col("id") //id
  val idcol1 = df("id") //id

  val idcol2 = df.apply("id") //id
  println(idcol2)

  df.drop("id").show()

  df.limit(10).show()

  df.orderBy(-df("id")).show()
  df.orderBy(df("id").desc).show()
  df.sort(-df("id")).show()
  df.sort(df("id")).show()

  df.sort($"id").show()

  /**
    * Returns a new Dataset with each partition sorted by the given expressions.
    * This is the same operation as "SORT BY" in SQL (Hive QL).
    */
  df.sortWithinPartitions(-df("id")).show()
  df.distinct()

  df.dropDuplicates()
  df.dropDuplicates(Seq("id", "name"))

  df.groupBy("id")
  df.groupBy(df("id")).max().alias("maxId").show()
  df.groupBy(df("id")).max().as("maxId").show()

  df.agg("id" -> "max", "name" -> "sum")

  df.union(df.limit(10))


  //  join use one or more fields
  df.join(df1, "id")
  df.join(df1, Seq("id", "name"), "inner")

  df.join(df1, df1("id") === df("id"))
  df.join(df1, df1("id") === df("id"), "inner")


  //  　计算一个DataFrame中两列的相关性作为一个double值 ，目前只支持皮尔逊相关系数
  val pre = df.stat.corr("id", "id")

  //  计算给定列的协方差，有他们的names指定，作为一个double值
  df.stat.cov("id", "id")

  //  由给定的列计算一个双向的频率表.也被称为一个列联表,　每一列的不同值的数量应该小于1e4. 最多1e6 非零对频率将被返回.
  def crosstabTest() = {
    val df = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3))).toDF("key", "value")
    val ct = df.stat.crosstab("key", "value")
    ct.show(10, truncate = 0)
  }

  //字段匹配过滤
  //写法一：
  //  val temp = df.schema.fieldNames.map(fName => (fName, df.groupBy(s"$fName").count().sort(-$"count").first().get(1).asInstanceOf[Long] / total))
  //写法二：
  //  val temp = df.schema.fieldNames.map(fName => (fName, df.groupBy(s"$fName").count())).filter{case(_, cdf) =>cdf.sort(cdf("count").desc).first().get(1).asInstanceOf[Long] / total > 0.8}.map{case(f,_)=> f}
  //  for(i<- temp) println(i)
  //写法三：
  //  val temp = df.schema.fieldNames.map(fName => (fName, df.groupBy(s"$fName").count())).map(x =>(x._1, x._2.sort(x._2("count").desc).first().get(1).asInstanceOf[Long] / total))
  //  for (i <- temp ) println(i._1, i._2)

  val fractions1: Map[Int, Double] = List((1, 0.2), (2, 0.8)).toMap //设定抽样格式
  //  sampleByKey(withReplacement = false, fractions, 0)
  //  fractions表示在层1抽0
  //  .2 ， 在层2中抽0
  //  .8
  //  withReplacement false表示不重复抽样
  //    0 表示随机的seed

  val df = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2),
    (3, 3))).toDF("key", "value")
  val fractions = Map(1 -> 1.0, 3 -> 0.5)
  df.stat.sampleBy("key", fractions, 36L).show()
  //+---+-----+
  //|key|value|
  //+---+-----+
  //|  1|    1|
  //|  1|    2|
  //|  3|    2|
  //+---+-----+

//  df.stat.sampleBy()
//  df.stat.countMinSketch()
//  df.stat.bloomFilter()


  //  统计交集
  df.intersect(df)

  //  获取在df1的而没有在df2的
  df.except(df)

  //  字段重命名，如果字段不存在，不进行操作
  df.withColumnRenamed("id", "newId")

  df.withColumn("id2", df("id")).show()

  //  行转列
  df.explode("c3", "c3_") { time: String => time.split(" ") }

  //  na, randomSplit, repartition, alias, as
  //    +-------+------+---+------------+--------+-------------+---------+----------+------+
  //    |affairs|gender|age|yearsmarried|children|religiousness|education|occupation|rating|
  //    +-------+------+---+------------+--------+-------------+---------+----------+------+
  //    |      0|  male| 37|          10|      no|            3|       18|         7|     4|
  //    |      0|  male| 57|          15|     yes|            2|       14|         4|     4|
  //    |      0|female| 32|          15|     yes|            4|       16|         1|     2|
  //    |      0|  male| 22|         1.5|      no|            4|       14|         4|     5|
  //    |      0|  male| 37|          15|     yes|            2|       20|         7|     2|
  //    |      0|  male| 27|           4|     yes|            4|       18|         6|     4|
  //    |      0|  male| 47|          15|     yes|            5|       17|         6|     4|
  //    |      0|female| 22|         1.5|      no|            2|       17|         5|     4|
  //    |      0|female| 27|           4|      no|            4|       14|         5|     4|
  //    |      0|female| 37|          15|     yes|            1|       17|         5|     5|
  //    +-------+------+---+------------+--------+-------------+---------+----------+------+

  //删除某列的空值和NaN
  val res = df.na.drop(Array("gender", "yearsmarried"))

  // 删除某列的非空且非NaN的低于10的
  df.na.drop(10, Array("gender", "yearsmarried"))


  //填充所有空值的列
  val res123 = df.na.fill("wangxiao123")

  //  对指定的列空值填充
  val res2 = df.na.fill(value = "wangxiao111", cols = Array("gender", "yearsmarried"))


  df.alias("al").show()
  df.as("as").show()

  //dataframe join
  df1.join(df, $"df1Key" === $"df2Key")
  df1.join(df).where($"df1Key" === $"df2Key")

  import org.apache.spark.sql.functions._
  //joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
  df1.join(df, $"df1Key" === $"df2Key", "outer")

  //withColumn
  import spark.implicits._
  import org.apache.spark.sql.functions._

  val dsWithLogSales = df.na.fill(99).withColumn("score",
    udf((sales: Int) => math.log(sales)).apply(col("score")))

  val ageNameDf = dsWithLogSales.withColumn("age",
    udf((age:String)=> if (age == null || age == "") "-999" else age).apply(col("age")))

  val ttt = df.withColumn("diff", abs($"id"-$"score"))
}
