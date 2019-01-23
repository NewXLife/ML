package ml

import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * create by colin on 2018/7/24
  * StringIndexer对数据集的label进行重新编号
  */
object SparkMLlibTransformerStringIndexer2String extends App{
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val df = spark.createDataFrame(
    Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
  ).toDF("id", "category")


  df.show()
  /*
  +---+--------+
| id|category|
+---+--------+
|  0|       a|
|  1|       b|
|  2|       c|
|  3|       a|
|  4|       a|
|  5|       c|
+---+--------+
   */

  val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")
    .setHandleInvalid("skip") //忽略这些label所在行的数据,
    .fit(df)

  val ts = indexer.transform(df)
  ts.show()

  /**
    * 按照出现的频次高低来编号
    */
  /* a-> 3次， b：一次， c：二次 ，所以按照频次排序为  a：0 ， b：2， c：1
  +---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          2.0|
|  2|       c|          1.0|
|  3|       a|          0.0|
|  4|       a|          0.0|
|  5|       c|          1.0|
+---+--------+-------------+
   */

//  val conver = new IndexToString().setInputCol("categoryIndex").setOutputCol("origionStr")
//  conver.transform(ts).show()

  /**
    * StringIndexer本质上是对String类型–>index( number);
    * 如果是：数值(numeric)–>index(number),实际上是对把数值先进行了类型转换（ cast numeric to string and then index the string values.），
    * 也就是说无论是String，还是数值，都可以重新编号（Index);
    */


  val df2 = spark.createDataFrame(
    Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e"), (5, "c"))
  ).toDF("id", "category")

  /* 如果有新的数据d， e 添加进来，那么如何索引呢；需要设置参数 .setHandleInvalid("skip")，否则会抛出异常
 id | category | categoryIndex
----|----------|---------------
 0  | a        | 0.0
 1  | b        | 2.0
 2  | d        | ？
 3  | e        | ？
 4  | a        | 0.0
 5  | c        | 1.0
   */
  indexer.transform(df2).show()
}
