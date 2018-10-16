package ml

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

/**独热编码将类别特征（离散的，已经转换为数字编号形式），映射成独热编码。
  * 这样在诸如Logistic回归这样需要连续数值值作为特征输入的分类器中也可以使用类别（离散）特征
  *
  * 独热编码即 One-Hot 编码，又称一位有效编码，其方法是使用N位 状态寄存
  * 器来对N个状态进行编码，每个状态都由他独立的寄存器 位，并且在任意
  * 时候，其 中只有一位有效。
  * 例如： 自然状态码为：000,001,010,011,100,101 即（0， 1， 2， 3， 4， 5）
  * 独热编码为：000001,000010,000100,001000,010000,100000（用6位数表示，每一位 0/1）
  * 可以这样理解，对于每一个特征，如果它有m个可能值，那么经过独 热编码
  * 后，就变成了m个二元特征。并且，这些特征互斥，每次只有 一个激活。因
  * 此，数据会变成稀疏的。
  * 这样做的好处主要有：
  * 解决了分类器不好处理属性数据的问题
  * 在一定程度上也起到了扩充特征的作用
  */


/**
  * 离散 <-> 连续   或者 label相互转换
  */
object SparkMLlibTransformerOneHotEncoder extends App{
  val data = Array((0, "1"), (1,"3"), (2, "2"), (3, "4"),(4, "5"))

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.createDataFrame(data).toDF("id","features")

  df.show()
  /*
+---+--------+
| id|features|
+---+--------+
|  0|       1|
|  1|       3|
|  2|       2|
|  3|       4|
|  4|       5|
+---+--------+
   */

  /*
   在使用 OneHotEncoder 前需要将string->number
   */
  val indexer = new StringIndexer().setInputCol("features").setOutputCol("index_features").fit(df).transform(df)


  //对随机分布的类别进行OneHotEncoder，转换后可以当成连续数值输入
  val res = new OneHotEncoder().setInputCol("index_features").setOutputCol("hot_index_features")

  res.transform(indexer).show()

  /*
  +---+--------+--------------+------------------+
| id|features|index_features|hot_index_features|
+---+--------+--------------+------------------+
|  0|       1|           2.0|     (4,[2],[1.0])|
|  1|       3|           4.0|         (4,[],[])|
|  2|       2|           3.0|     (4,[3],[1.0])|
|  3|       4|           0.0|     (4,[0],[1.0])|
|  4|       5|           1.0|     (4,[1],[1.0])|
+---+--------+--------------+------------------+
   */
}
